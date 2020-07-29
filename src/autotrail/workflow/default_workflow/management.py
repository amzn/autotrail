"""Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

"""
import os

from itertools import chain
from multiprocessing import Process

from autotrail.core.state_machine import run_state_machine_evaluator
from autotrail.core.api.management import MethodAPIHandlerWrapper, SocketServer
from autotrail.core.api.callbacks import ManagedCallback
from autotrail.workflow.default_workflow.state_machine import (make_state_machine_definitions, APIHandlers, State,
                                                               ACTION_EVALUATIONS)
from autotrail.workflow.default_workflow.api import WorkflowAPIHandler


class WorkflowManager:
    """Manager for the default workflow.

    The manager does the following:
    1. Creates the workflow based on the ordered pairs provided. This includes the success and failure branches.
    2. Sets up automated state transitions.
    3. Sets up automatic serialization of machine and context objects (optional).
    4. Runs a multiprocessing.Pipe based server to facilitate API calls.
    5. Sets up the final callback function that will execute when the final states are reached.
    """
    def __init__(self, success_pairs, failure_pairs, context, context_serializer, socket_file, workflow_delay=1,
                 api_delay=1, transition_rules=None, initial_state=State.READY, action_evaluations=None,
                 final_callback_function=None, machine_serializer=None, api_handlers=None):
        """Initialize the workflow manager.

        :param success_pairs:           A list of tuples. Each tuple is an ordered pair associating a step with its
                                        successor when it successfully runs. E.g., [(a, b), (b, c)] results in the
                                        following:
                                            a --[if successful]--> b --[if successful]--> c
        :param failure_pairs:           Similar to success_pairs, but links failures. E.g., [(a, b)] results in:
                                            a --[if failed]--> b
        :param context:                 The context dictionary.
        :param context_serializer:      A core.api.serializers.Serializer like callable that can be used to serialize
                                        the context object.
        :param socket_file:             The socket file name to be used for communicating with the workflow API server.
        :param workflow_delay:          Introduce a delay in the loop of the state machine evaluations. This delay
                                        affects how frequently callbacks are called.
        :param api_delay:               The delay used by the API server. This decides how frequently the server listens
                                        for API messages.
        :param transition_rules:        The rules for state transitions as a mapping of the following form:
                                        {
                                            <State 1 (str)>: {
                                                <Action 1 (str)>: <State 2 (str)>,
                                                ...
                                            },
                                            ...
                                        }
                                        The above snippet defines the rule that if a machine is in "State 1", "Action 1"
                                        can be performed on it. If "Action 1" is performed, then the machine transitions
                                        to "State 2".
                                        Defaults to using workflow.default_workflow.state_machine.TRANSITION_RULES.
        :param initial_state:           The inital state of the machines. This is a single state that will be applied to
                                        all the machines.
        :param action_evaluations:      A mapping expressing the rules and functions for automatic state transitions.
                                        This is of the following form:
                                        {
                                            <Action 1 (str)>: (<Callable 1>, <Successful return value (any type)>),
                                            ...
                                        }
                                        Where, for a step (machine), if an action is available (as a possible action),
                                        its corresponding callable will be called with the step object and context.
                                        If this callable returns the specified 'Successful return value', then the
                                        action will be considered to have taken place on the machine and its state will
                                        be transitioned according to the transition_rules.
        :param final_callback_function: Callable to execute when the final states are reached, i.e., no further actions
                                        are available or possible.
                                        This callable should accept the machine states of the form:
                                           {
                                             <Machine 1 name (str)>: <Machine 1 state (str),
                                             ...
                                           }
                                           The return value of this function is ignored.
        :param machine_serializer:      An object like core.api.serializers.Serializer to serialize the machines.
        :param api_handlers:            An object to handle API calls. Each public method represents an API
                                                call and must:
                                                1. Accept (states, transitions) as the first 2 mandatory parameters.
                                                   Any additional parameters (args, kwargs) will need to be received
                                                   as part of the API request. Where:
                                                    states is a dictionary of the form:
                                                         {
                                                             <machine 1 name (str)>: <state of machine 1 (str)>,
                                                             ...
                                                         }
                                                    transitions is a dictionary of the form:
                                                         {
                                                             <machine 1 name (str)>: [<possible action 1 (str)>,
                                                                                      <possible action 2 (str)>,
                                                                                      ...],
                                                             ...
                                                         }
                                                2. Return a tuple of the form: (<API result>, <Actions>), where;
                                                   <API result> is the result returned to the client making the API
                                                                call.
                                                   <Actions> is a mapping of the form:
                                                             {<Machine name>: <Action to take>}
        """
        steps = list(chain.from_iterable(success_pairs))
        steps.extend(list(chain.from_iterable(failure_pairs)))
        self._steps = list(set(steps))

        self._step_id_to_object_mapping = {step.id: step for step in steps}
        action_evaluations = action_evaluations or ACTION_EVALUATIONS
        self._action_definition = {step.id: action_evaluations for step in steps}
        self._state_machine_definitions = make_state_machine_definitions(success_pairs, failure_pairs,
                                                                         transition_rules=transition_rules,
                                                                         initial_state=initial_state)
        api_handlers = api_handlers or APIHandlers(self._step_id_to_object_mapping, context)

        self._callback_manager = ManagedCallback(api_handlers, self._action_definition,
                                                 self._step_id_to_object_mapping,
                                                 context=context,
                                                 context_serializer=context_serializer,
                                                 machine_serializer=machine_serializer,
                                                 delay=workflow_delay,
                                                 final_callback_function=final_callback_function)
        self._workflow_process = run_state_machine_evaluator(self._state_machine_definitions, self._callback_manager)
        self._api_process = None
        self._api_delay = api_delay

        self._socket_file = socket_file

    def cleanup(self):
        """Remove the socket file."""
        try:
            os.remove(self._socket_file)
        except OSError:
            pass

    def start(self):
        """Start the workflow and API server processes."""
        self._workflow_process.start()
        workflow_api_handler = MethodAPIHandlerWrapper(
            WorkflowAPIHandler(self._steps, self._callback_manager, self._workflow_process))
        workflow_api_server = SocketServer(self._socket_file, workflow_api_handler, delay=self._api_delay, timeout=1)
        self._api_process = Process(target=workflow_api_server)
        self._api_process.start()

    def terminate(self):
        """Terminate the workflow and API server processes."""
        self.terminate_workflow()
        self.terminate_api_server()

    def terminate_workflow(self):
        """Terminate the workflow process."""
        try:
            self._workflow_process.terminate()
        except AttributeError:
            pass

    def terminate_api_server(self):
        """Termiante the API server process."""
        try:
            self._api_process.terminate()
        except AttributeError:
            pass

    def is_workflow_alive(self):
        """Check if the workflow process is alive.

        :return: Boolean. True if the process is alive, False otherwise.
        """
        return self._workflow_process.is_alive()

    def is_api_server_alive(self):
        """Check if the API server process is alive.

        :return: Boolean. True if the process is alive, False otherwise.
        """
        return self._api_process.is_alive()

    def join(self, timeout=None):
        """Wait for the workflow and API server processes to finish.

        :param timeout: Seconds to wait for the processes to finish.
        :return:        None.
        """
        self._workflow_process.join(timeout)
        self._api_process.join(timeout)
