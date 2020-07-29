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
import logging

from multiprocessing import Manager, Pipe
from time import sleep

from autotrail.core.api.management import ConnectionServer, MethodAPIHandlerWrapper


logger = logging.getLogger(__name__)


class ActionCallback:
    """Base type for an Action callback. It is a callable that accepts states and transitions and returns actions."""
    def __call__(self, states, transitions):
        """A callable that accepts states and transitions and returns actions.

        :param states:      A dictionary of the form:
                            {
                                <machine 1 name (str)>: <state of machine 1 (str)>,
                                ...
                            }
        :param transitions: A dictionary of the form:
                            {
                                <machine 1 name (str)>: [<possible action for machine 1 (str)>,
                                                         <possible action for machine 2 (str)>,
                                                         ...],
                                ...
                            }
        :return:            The returned actions should either be None or a dictionary of the form:
                            {
                                <machine 1 name (str)>: <action for machine 1 (str),
                                ...
                            }
        """
        raise NotImplementedError()


class ChainActionCallbacks(ActionCallback):
    """A combiner for multiple ActionCallback callables."""
    def __init__(self, callbacks):
        """Define the list of callbacks that will be called in the passed order.

        Ordering of the callbacks is important. If more than one callback returns actions for the same state machine,
        the latest one takes precedence.

        :param callbacks: An iterable of ActionCallback like callable objects.
        """
        self._callbacks = callbacks

    def __call__(self, states, transitions):
        """Calls each of the given callbacks in order and collates the returned actions together.

        :param states:      As per the ActionCallback class specification.
        :param transitions: As per the ActionCallback class specification.
        :return:            Collated actions returned by each of the callbacks.
        :raises:            If any callable raises an exception, this callable logs and re-raises it.
        """
        next_actions = {}
        for callback in self._callbacks:
            try:
                actions = callback(states, transitions) or {}
            except Exception as e:
                logger.exception('Action callback {} failed with exception {}.'.format(callback, e))
                raise

            if actions:
                logger.debug('The action callback {} returned the following actions: {}'.format(callback, actions))

            next_actions.update(actions)

        if next_actions:
            logger.info('Actions collected from callbacks: {} are: {}'.format(self._callbacks, next_actions))

        return next_actions


class SimpleCallback(ActionCallback):
    """Wraps an action callback function that doesn't accept any parameters."""
    def __init__(self, callback):
        """Define the callback that will be wrapped.

        :param callback: A callable that accepts no parameters and is compliant with the return specifications of the
                         ActionCallback class.
        """
        self._callback = callback

    def __call__(self, states, transitions):
        """Call the wrapped callback function with no parameters.

        :param states:      As per the ActionCallback class specification.
        :param transitions: As per the ActionCallback class specification.
        :return:            As per the ActionCallback class specification.
        """
        return self._callback()


class FinalCallback(ActionCallback):
    """A wrapper that will call the given callback function only when there are no transitions available for any of the
    machines i.e., this will be the final callback before the state machine stops evaluating.
    """
    def __init__(self, callback):
        """Define the callable that will be called when there are no transitions available for any of the machines

        :param callback: A callable similar to the ActionCallback class specification, but doesn't accept transitions.
        """
        self._callback = callback

    def __call__(self, states, transitions):
        """Call the defined callable with the passed states.

        :param states:      As per the ActionCallback class specification.
        :param transitions: As per the ActionCallback class specification.
        :return:            None.
        """
        if not any(transitions.values()):
            self._callback(states)


class DelayCallback(ActionCallback):
    """An action callback that will return after sleeping for the given number of seconds."""
    def __init__(self, delay=1):
        """Define the delay in seconds.

        :param delay: Float representing seconds of delay to introduce.
        """
        self._delay = delay

    def __call__(self, states, transitions):
        """Sleep for the defined time.

        :param states:      Ignored. Accepted to comply with the ActionCallback class specification.
        :param transitions: Ignored. Accepted to comply with the ActionCallback class specification.
        :return:            None
        """
        sleep(self._delay)


class StatesCallback(ActionCallback):
    """An action callback that stores the passed machine states in a multiprocessing.Manager shared dictionary.

    The shared dictionary can be accessed with the 'states' instance attribute.
    """
    def __init__(self):
        """Initialize the shared dictionary."""
        self.states = Manager().dict()

    def __call__(self, states, transitions):
        """Update the 'states' shared dictionary with the passed states.

        :param states:      As per the ActionCallback class specification.
        :param transitions: Ignored. Accepted to comply with the ActionCallback class specification.
        :return:            None
        """
        self.states.update(states)


class TransitionsCallback(ActionCallback):
    """An action callback that stores the passed machine transitions in a multiprocessing.Manager shared dictionary.

    The shared dictionary can be accessed with the 'transitions' instance attribute.
    """
    def __init__(self):
        """Initialize the shared dictionary."""
        self.transitions = Manager().dict()

    def __call__(self, states, transitions):
        """Update the 'transitions' shared dictionary with the passed states.

        :param states:      Ignored. Accepted to comply with the ActionCallback class specification.
        :param transitions: As per the ActionCallback class specification.
        :return:            None
        """
        self.transitions.update(transitions)


class AutomatedActionCallback(ActionCallback):
    """An action callback that automatically determines if the available actions can be performed on the corresponding
    machines using the rules (explained under 'machine_action_definitions').
    """
    def __init__(self, machine_name_to_object_mapping, context, machine_action_definitions):
        """Define the rules for automatic actions.

        :param machine_name_to_object_mapping:  A mapping of the form:
                                                {
                                                  <Machine 1 name (str)>: <Machine 1 object>,
                                                  ...
                                                }
        :param context:                         The context to be passed.
        :param machine_action_definitions:      A mapping of the form:
                                                {
                                                  <Machine 1 name (str)>:
                                                      {
                                                          <Action 1 for Machine 1(str)>: (<function>,
                                                                                          <successful return value>),
                                                          ...
                                                      },
                                                  ...
                                                }
                                                Where, for each machine, if an action is available (as a possible
                                                action), its corresponding function will be called with the given
                                                context.
                                                If this function returns the specified 'successful return value', then
                                                the action will be considered to have taken place on the machine and
                                                the state machines will be transitioned accordingly.
        """
        self._machine_name_to_object_mapping = machine_name_to_object_mapping
        self._context = context
        self._machine_action_definitions = machine_action_definitions

    def __call__(self, states, transitions):
        """Call the function associated with the available actions for each machine in the given transitions and
        return the successful actions taken (per machine).

        :param states:      As per the ActionCallback class specification.
        :param transitions: As per the ActionCallback class specification.
        :return:            As per the ActionCallback class specification. If the function associated with the available
                            actions for a machine returns the specified 'successful return value', then the action will
                            be considered to have taken place on the machine and the returned dictionary will contain
                            the machine (as key) and the action taken (as the associated value).
        """
        machine_action_mapping = {machine_name: available_transitions.keys()
                                  for machine_name, available_transitions in transitions.items()}
        return attempt_actions(self._machine_action_definitions, self._machine_name_to_object_mapping, self._context,
                               machine_action_mapping)


class InjectedActionCallback(ActionCallback):
    """An action callback that opens a multiprocessing.Pipe to inject actions into the state machines.

    The endpoint to receive actions can be accessed with the 'actions_writer' instance attribute.
    This is a write-only multiprocessing.Connection object that expects messages representing actions in the following
    form:
    {
        <Machine 1 name (str)>: <Action for Machine 1 (str)>,
        ...
    }

    The actions can be injected as follows:
        InjectedActionCallback.actions_writer.send({
            <Machine 1 name (str)>: <Action for Machine 1 (str)>,
            ...
        })

    If an action that is not possible/available for a machine is injected, it will have no
    effect and will be ignored.
    """
    def __init__(self):
        """Initialize the multiprocessing.Pipe endpoints to receive actions."""
        self._actions_reader, self.actions_writer = Pipe(duplex=False)

    def __call__(self, states, transitions):
        """Read actions from the multiprocessing.Pipe endpoint and return them per the ActionCallback class
        specification.

        :param states:      As per the ActionCallback class specification.
        :param transitions: As per the ActionCallback class specification.
        :return:            As per the ActionCallback class specification.
        """
        next_actions = {}
        if self._actions_reader.poll():
            try:
                next_actions = self._actions_reader.recv()
            except EOFError:
                pass

        return next_actions


class ManagedCallback(ActionCallback):
    """An action callback wrapper that sets up a standard set of action callbacks.

    The following instance attributes are available:
    states:                            Store the machine states in a multiprocessing.Manager shared dictionary.
    transitions:                       Store the machine transitions available/possible in a
                                        multiprocessing.Manager shared dictionary.
    api_client_connection:             The connection object used to send and receive API requests.
    actions_writer:                    Write-only multiprocessing.Connection object that expects messages
                                        representing actions in the following form:
                                        {
                                            <Machine 1 name (str)>: <Action for Machine 1 (str)>,
                                            ...
                                        }

                                        The actions can be injected as follows:
                                            InjectedActionCallback.actions_writer.send({
                                                <Machine 1 name (str)>: <Action for Machine 1 (str)>,
                                                ...
                                            })

                                        If an action that is not possible/available for a machine is injected,
                                        it will have no effect and will be ignored.
    """
    def __init__(self, api_handler, machine_action_definitions, machine_name_to_object_mapping, context=None,
                 machine_serializer=None, context_serializer=None, delay=1, final_callback_function=None,
                 api_server_timeout=1):
        """Define the resources required to set up a standard set of action callbacks.

        If the parameters required to setup an action callback is not passed, then it is not included.

        :param api_handler:                     An object to handle API calls. Each public method represents an API
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

        :param machine_action_definitions:      A mapping of the form:
                                                {
                                                  <Machine 1 name (str)>:
                                                      {
                                                          <Action 1 for Machine 1(str)>: (<function>,
                                                                                          <successful return value>),
                                                          ...
                                                      },
                                                  ...
                                                }
                                                Where, for each machine, if an action is available (as a possible
                                                action), its corresponding function will be called with the given
                                                context.
                                                If this function returns the specified 'successful return value', then
                                                the action will be considered to have taken place on the machine and
                                                the state machines will be transitioned accordingly.
        :param machine_name_to_object_mapping:  A mapping of the form:
                                                 {
                                                   <Machine 1 name (str)>: <Machine 1 object>,
                                                   ...
                                                 }
        :param context:                         The context to be passed.
        :param machine_serializer:              An object like autotrail.core.api.serializers.Serializer.
        :param context_serializer:              An object like autotrail.core.api.serializers.Serializer.
        :param delay:                           Introduce a delay in the loop of the state machine evaluations.
                                                This delay affects how frequently callbacks are called.
                                                An float. Defaults to 1 second.
        :param final_callback_function:         Function to execute when the final states are reached, i.e., no further
                                                actions are available or possible. This needs to be a callable that
                                                accepts the machine states of the form:
                                                {
                                                  <Machine 1 name (str)>: <Machine 1 state (str),
                                                  ...
                                                }
                                                The return value of this function is ignored.
        :param api_server_timeout:              The timeout in seconds (float) the API server will wait to receive and
                                                serve requests.
        """
        callbacks = [AutomatedActionCallback(machine_name_to_object_mapping, context, machine_action_definitions)]

        self._states_callback = StatesCallback()
        callbacks.append(self._states_callback)
        self.states = self._states_callback.states

        self._transitions_callback = TransitionsCallback()
        callbacks.append(self._transitions_callback)
        self.transitions = self._transitions_callback.transitions

        self.machines_serialized = None
        if machine_serializer is not None:
            self._machines_serializer_callback = SimpleCallback(machine_serializer)
            callbacks.append(self._machines_serializer_callback)
            self.machines_serialized = machine_serializer.serialized

        self.context_serialized = None
        if context_serializer is not None:
            self._context_serializer_callback = SimpleCallback(context_serializer)
            callbacks.append(self._context_serializer_callback)
            self.context_serialized = context_serializer.serialized

        self._injected_action_callback = InjectedActionCallback()
        self.actions_writer = self._injected_action_callback.actions_writer
        callbacks.append(self._injected_action_callback)

        self.api_client_connection, self._server_connection = Pipe(duplex=True)
        self._api_callback = ConnectionServer(MethodAPIHandlerWrapper(api_handler),
                                              self._server_connection,
                                              timeout=api_server_timeout)
        callbacks.append(self._api_callback)

        if final_callback_function is not None:
            callbacks.append(FinalCallback(final_callback_function))

        if delay is not None:
            callbacks.append(DelayCallback(delay))

        self._action_callback = ChainActionCallbacks(callbacks)

    def __call__(self, states, transitions):
        """Call the defined callbacks and return the collated actions from all of them.

        The action callbacks will be executed in the following order:
        1. Automated actions:           To automatically execute functions and perform state transitions.
        2. Record machine states:       Store the machine states in a multiprocessing.Manager shared dictionary.
        3. Record machine transitions:  Store the machine transitions available/possible in a multiprocessing.Manager
        4. Serialize machine objects:   To serialize the machine objects to some medium. (Optional)
        5. Serialize context object:    To serialize the context object to some medium. (Optional)
        6. Injected actions:            Ability to inject actions into the state machines.
        7. API server:                  Run a multiprocessing.Pipe based server to facilitate API calls.
        8. Final callback:              Function to execute when the final states are reached, i.e., no further actions
                                        are available or possible. (Optional)
        9. Delay:                       Introduce a delay in the loop of the state machine evaluations. This delay
                                        affects how frequently callbacks are called. (Optional)

        :param states:      As per the ActionCallback class specification.
        :param transitions: As per the ActionCallback class specification.
        :return:            As per the ActionCallback class specification, the actions returned by all the callbacks
                            are collated and returned.
        """
        return self._action_callback(states, transitions)


def attempt_actions_for_machine(machine_action_definition, machine, context, actions):
    """Check the given actions for a machine and determine which action was successfully taken.

    For the given list of actions, the corresponding function from the given machine_action_definition will be called
    with the machine and context objects. If there is no context object (None), then machine will be passed as the only
    parameter to the function.
    If the function returns a value equal to the associated "successful return value", the action would be considered
    successfully taken and will be returned. Symbolically:
        <successful return value> == <function>(<machine>, <context>)
        or
        <successful return value> == <function>(<machine>)

    If a function appears multiple times for different actions, it will be called exactly once and the value returned
    will be matched against the other cases to determine the successful action.

    If a function raises an exception, the action will not be considered successful and subsequent actions will be
    attempted.

    If an action doesn't exist in the machine_action_definition, it will be ignored.

    :param machine_action_definition: A mapping of the form:
                                      {
                                          <Action 1 for Machine 1(str)>: (<function>, <successful return value>),
                                          ...
                                      }
    :param machine:                   Any object representative of the machine.
    :param context:                   The context object.
    :param actions:                   List of strings representing the actions to check.
    :return:                          The action performed (string).
    """
    action_cache = {}
    for action in actions:
        if action not in machine_action_definition:
            continue
        action_performer, successful_action = machine_action_definition[action]
        args = (machine, context) if context is not None else (machine,)
        try:
            if action_cache.setdefault(action_performer, action_performer(*args)) == successful_action:
                return action
        except Exception:
            continue


def attempt_actions(machine_action_definitions, machine_name_to_object_mapping, context, machine_action_mapping):
    """Attempt running actions on the corresponding machines and return the actions successfully taken.

    For each machine in the given machine_action_mapping (described below), all the corresponding actions are attempted
    using the machine_action_definitions and a successful action is determined (there may not be any successful
    actions).

    For each action associated with a machine, the corresponding function from the machine_action_definitions will be
    called with the machine and context objects. If there is no context object (None), then machine will be passed as
    the only parameter to the function.
    If the function returns a value equal to the associated "successful return value", the action would be considered
    successfully taken. Symbolically, a successful action is an action where:
        <successful return value> == <function>(<machine>, <context>)
        or
        <successful return value> == <function>(<machine>)

    For a machine, if a function appears multiple times for different actions, it will be called exactly once and the
    value returned will be matched against the other cases to determine the successful action.

    If a function raises an exception, the action will not be considered successful on the machine and subsequent
    actions will be attempted on it.

    If an action doesn't exist in the machine_action_definitions for a machine, it will be ignored.

    :param machine_action_definitions:      A mapping of the form:
                                            {
                                              <Machine 1 name (str)>:
                                                  {
                                                      <Action 1 for Machine 1(str)>: (<function>,
                                                                                      <successful return value>),
                                                      ...
                                                  },
                                              ...
                                            }
    :param machine_name_to_object_mapping:  A mapping of the form:
                                            {
                                              <Machine 1 name (str)>: <Machine 1 object>,
                                              ...
                                            }
    :param context:                         The context to be passed.
    :param machine_action_mapping:          A mapping of the form:
                                            {
                                              <Machine 1 name (str)>: [<Action 1 for machine 1 (str)>, ...],
                                              ...
                                            }
    :return:                                The successful actions for all the machines will be collated and returned
                                            as a dictionary of the form:
                                            {
                                                <Machine 1 name (str)>: <Action taken for machine 1 (str)>,
                                                ...
                                            }
    """
    actions_performed = {}
    for machine_name, available_actions in machine_action_mapping.items():
        action_performed = attempt_actions_for_machine(machine_action_definitions[machine_name],
                                                       machine_name_to_object_mapping[machine_name],
                                                       context,
                                                       available_actions)
        if action_performed:
            actions_performed[machine_name] = action_performed

    return actions_performed
