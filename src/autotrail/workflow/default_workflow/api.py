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

from autotrail.core.api.management import (MethodAPIClientWrapper, SocketServer, SocketClient, ConnectionClient,
                                           APIHandlerResponse)
from autotrail.workflow.default_workflow.state_machine import Action, State


logger = logging.getLogger(__name__)


def filter_steps_by_states(steps, step_states, states):
    """Filter step objects that are in the given states.

    :param steps:       An iterable of step objects.
    :param step_states: A dictionary of the form:
                        {
                            <machine 1 name (str)>: <state of machine 1 (str)>,
                            ...
                        }
    :param states:      The collection of states to filter by.
    :return:            A generator of step objects that are in the given states.
    """
    return (step for step in steps if step_states[step.id] in states)


def filter_steps_by_tags(steps, tags):
    """Filter step objects by the given tags.

    :param steps:   An iterable of step objects.
    :param tags:    Tags are arbitrary key-value pairs (dictionary) that can be associated with a step.
    :return:        A generator of step objects that have the given tags associated with them.
    """
    return (step for step in steps if tags in step)


def filter_steps_by_action(steps, transitions, action):
    """Filter step objects on which, the given action can be performed.

    :param steps:       An iterable of step objects.
    :param transitions: A dictionary of the form:
                        {
                            <machine 1 name (str)>: [<possible action for machine 1 (str)>,
                                                     <possible action for machine 2 (str)>,
                                                     ...],
                            ...
                        }
    :param action:      The action to filter by (str).
    :return:
    """
    return (step for step in steps if action in transitions[step.id])


def extract_step_ids(steps):
    """Extract the 'id' attribute of each step.

    :param steps: An iterable of step objects.
    :return:      A generator of step IDs.
    """
    return (step.id for step in steps)


def get_class_globals(klass):
    """Returns a set of the globals defined in the given class.
    Globals are identified to be uppercase and do not start with an underscore.

    :param klass: The class whose globals need to be fetched.
    :return: A set of class globals
    """
    return {getattr(klass, attribute)
            for attribute in dir(klass) if not attribute.startswith('_') and attribute.isupper()}


class StatusField:
    """Namespace for the fields returned as part of the status API call. Use this class instead of plain strings."""
    NAME = 'Name'
    TAGS = 'Tags'
    STATE = 'State'
    ACTIONS = 'Actions'
    IO = 'I/O'
    OUTPUT = 'Output'
    RETURN_VALUE = 'Return value'
    EXCEPTION = 'Exception'


class WorkflowAPIHandler:
    """API definition of the default workflow. Compliant with the requirements of MethodAPIHandlerWrapper.

    This class handles the API calls that may be used to manage and interact with the default workflow.
    Each public method is an API call definition that must return a APIHandlerResponse or similar object.
    If a method raises an exception, the following APIHandlerResponse object will be assumed:
        APIHandlerResponse(None, <exception raised>, None)
    If a method wants to shutdown the API server, it must set relay_value=SocketServer.SHUTDOWN in the returned
        APIHandlerResponse object.

    For API Requesters:
    The API requester will receive APIResponse objects that are similar to the APIHandlerResponse objects but without
    the relay_value. Therefore, treat the APIHandlerResponse return description as though the were APIResponse objects
    and ignore the relay_value.
    """
    def __init__(self, steps, callback_manager, process):
        """Initialise the API handler.

        :param steps:               An iterable of step objects (similar to default_workflow.step.Step).
        :param callback_manager:    A managed callback callable similar to core.api.callbacks.ManagedCallback.
        :param process:             The process running the state machine evaluator.
        """
        self._callback_manager = callback_manager
        self._steps = list(steps)
        self._machine_api_client = MethodAPIClientWrapper(
            ConnectionClient(self._callback_manager.api_client_connection))
        self._process = process

    @property
    def _states(self):
        """Creates a dictionary copy of the multiprocessing.Manager shared dictionary of states."""
        return dict(self._callback_manager.states)

    @property
    def _transitions(self):
        """Creates a dictionary copy of the multiprocessing.Manager shared dictionary of transitions."""
        return dict(self._callback_manager.transitions)

    @property
    def _context_serialized(self):
        """Creates a dictionary copy of the serialized context that is shared in a multiprocessing.Manager."""
        return dict(self._callback_manager.context_serialized)

    def get_serialized_context(self):
        """Get a serialized copy of the current state of the context.

        :return: An APIHandlerResponse object whose 'return_value' is the current state of the context as a dictionary
                 or other serialized structure.
        """
        return APIHandlerResponse(self._context_serialized)

    def start(self, dry_run=True):
        """Start a workflow. This will change the state of all steps in 'Ready' state to 'Waiting' state.

        :param dry_run: Boolean. API call doesn't have any effect when True.
        :return: An APIHandlerResponse object whose 'return_value' is the list of step IDs that were started.
        """
        step_ids = list(extract_step_ids(
            filter_steps_by_action(self._steps, self._transitions, Action.START)))
        if not dry_run:
            self._callback_manager.actions_writer.send({step_id: Action.START for step_id in step_ids})
        return APIHandlerResponse(step_ids)

    def shutdown(self, dry_run=True):
        """Shutdown the workflow server. After this call, no other API calls can be served.

        :param dry_run: Boolean. API call doesn't have any effect when True.
        :return: An APIHandlerResponse object whose 'return_value' is the list of step IDs that were interrupted and
                 'relay_value' is set to SocketServer.SHUTDOWN to signal the server to stop serving requests.
        """
        if dry_run:
            return self.interrupt(dry_run=True)
        else:
            self.pause(dry_run=False)
            response = self.interrupt(dry_run=False)
            if not list(filter_steps_by_states(self._steps, self._states, State.RUNNING)):
                try:
                    self._process.terminate()
                except OSError:
                    pass
            response.relay_value = SocketServer.SHUTDOWN  # This relay value signals the server to stop.
            return response

    def send_message_to_steps(self, message, dry_run=True, **tags):
        """Send a message to matching steps.

        :param message: Any JSON encodable Python object.
        :param dry_run: Boolean. API call doesn't have any effect when True.
        :param tags:    Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.
        :return:        An APIHandlerResponse object whose 'return_value' is the list of step IDs to which the message
                        was sent.
        """
        step_ids = list(extract_step_ids(
            filter_steps_by_states(filter_steps_by_tags(self._steps, tags), self._states, [State.RUNNING])))

        if not dry_run:
            step_message_mapping = {step_id: message for step_id in step_ids}
            try:
                self._machine_api_client.send_messages(step_message_mapping)
            except Exception as e:
                logger.exception('Unable to send messages: {} due to error: ({}) {}'.format(
                    step_message_mapping, type(e), e))
                raise
        return APIHandlerResponse(step_ids)

    def list(self, **tags):
        """List all the steps' tags in a workflow (in topological order).

        :param tags:    Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.
                        If no keyword arguments are provided, this will list the tags of all the steps in the workflow.
        :return:        An APIHandlerResponse object whose 'return_value' is the list of dictionaries (tags) of the
                        matching steps.
        """
        return APIHandlerResponse([step.tags for step in self._steps if tags in step])

    def status(self, fields=None, states=None, **tags):
        """Get workflow status.

        :param fields:  List of field names to return in the results. A list of strings from StatusField class.
                        This will limit the status to the fields requested.
                        Defaults to all the fields in the StatusField namespace.
                        No matter what fields are passed, the following 2 fields will always be present in the result:
                            StatusField.N: <Sequence number of the step>
                            StatusField.NAME: <Name of the step>
                        This is because without these fields, it will be impossible to uniquely identify the steps.
        :param states:  List of strings that represent the states of a Step.
                        This will limit the status to only the steps that are in the given list of states.
        :param tags:    Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.
        :return:        An APIHandlerResponse object whose 'return_value' is the list of dictionaries, each containing
                        the status of the matching steps along with:
                           1. The two keys as explained above (StatusField.N, StatusField.NAME)
                           2. Other keys as specified with the fields keyword argument.
        """
        states = states or get_class_globals(State)
        fields = fields or get_class_globals(StatusField)

        statuses = {}
        for step in filter_steps_by_states(filter_steps_by_tags(self._steps, tags), self._states, states):
            state = self._states[step.id]
            step_data = self._context_serialized['step_data'].get(step.id, {})
            step_status = {StatusField.NAME: step.tags['name']}
            if StatusField.TAGS in fields:
                step_status[StatusField.TAGS] = step.tags
            if StatusField.STATE in fields:
                step_status[StatusField.STATE] = state
            if StatusField.ACTIONS in fields:
                step_status[StatusField.ACTIONS] = list(self._transitions[step.id])
            if StatusField.IO in fields:
                step_status[StatusField.IO] = step_data.get('io', None)
            if StatusField.OUTPUT in fields:
                step_status[StatusField.OUTPUT] = step_data.get('output', None)
            if StatusField.RETURN_VALUE in fields:
                step_status[StatusField.RETURN_VALUE] = step_data.get('return_value', None)
            if StatusField.EXCEPTION in fields:
                step_status[StatusField.EXCEPTION] = step_data.get('exception', None)
            statuses[step.id] = step_status
        return APIHandlerResponse(statuses)

    def steps_waiting_for_user_input(self, **tags):
        """Get status of steps that are waiting for user input.
        Limits the fields to only include the UNREPLIED_PROMPT_MESSAGE.
        Excludes steps that do not have any UNREPLIED_PROMPT_MESSAGE.

        :param tags:    Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.
        :return:        An APIHandlerResponse object whose 'return_value' is the list of step IDs that are waiting for
                        user input.
        """
        return self.status(states=[State.RUNNING], fields=[StatusField.IO],  **tags)

    def pause(self, dry_run=True, **tags):
        """Send message to pause steps.
        Mark the specified steps so that they are paused.
        A step can only be paused if it is ready or waiting.

        :param dry_run: Boolean. API call doesn't have any effect when True.
        :param tags:    Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.
                        If tags are provided, then only the steps matching the tags will be paused.
                        If no tags are provided, all possible steps will be marked to be paused.
        :return:        An APIHandlerResponse object whose 'return_value' is the list of step IDs that were paused.
        """
        step_ids = list(extract_step_ids(filter_steps_by_action(filter_steps_by_tags(
            self._steps, tags), self._transitions, Action.PAUSE)))
        if not dry_run:
            self._callback_manager.actions_writer.send({step_id: Action.PAUSE for step_id in step_ids})
        return APIHandlerResponse(step_ids)

    def interrupt(self, dry_run=True, **tags):
        """Send message to interrupt running steps.
        This will interrupt the running steps by sending them SIGINT. A step can be interrupted only if it is running.

        :param dry_run: Boolean. API call doesn't have any effect when True.
        :param tags:    Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.
                        If tags are provided, then only the steps matching the tags will be interrupted.
                        If no tags are provided, all possible steps will be marked to be interrupted.
        :return:        An APIHandlerResponse object whose 'return_value' is the list of step IDs that were interrupted.
        """
        step_ids = list(extract_step_ids(filter_steps_by_action(filter_steps_by_tags(
            self._steps, tags), self._transitions, Action.INTERRUPT)))
        if not dry_run:
            self._machine_api_client.interrupt(step_ids)
        return APIHandlerResponse(step_ids)

    def resume(self, dry_run=True, **tags):
        """Send message to resume steps.
        Mark the specified steps so that they are resumed.
        A step can only be resumed if it is either paused, marked to pause, paused due to failure or interrupted.

        :param dry_run: Boolean. API call doesn't have any effect when True.
        :param tags:    Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.
                        If tags are provided, then only the steps matching the tags will be resumed.
                        If no tags are provided, all possible steps will be marked to be resumed.
        :return:        An APIHandlerResponse object whose 'return_value' is the list of step IDs that were resumed.
        """
        step_ids = list(extract_step_ids(filter_steps_by_action(filter_steps_by_tags(
            self._steps, tags), self._transitions, Action.RESUME)))
        if not dry_run:
            self._callback_manager.actions_writer.send({step_id: Action.RESUME for step_id in step_ids})
        return APIHandlerResponse(step_ids)

    def rerun(self, dry_run=True, **tags):
        """Send message to resume steps.
        Mark the specified steps so that they are resumed.
        A step can only be resumed if it is either paused, marked to pause, paused due to failure or interrupted.

        :param dry_run: Boolean. API call doesn't have any effect when True.
        :param tags:    Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.
                        If tags are provided, then only the steps matching the tags will be re-run.
                        If no tags are provided, all possible steps will be marked to be re-run.
        :return:        An APIHandlerResponse object whose 'return_value' is the list of step IDs that were re-run.
        """
        step_ids = list(extract_step_ids(filter_steps_by_action(filter_steps_by_tags(
            self._steps, tags), self._transitions, Action.RERUN)))
        if not dry_run:
            self._callback_manager.actions_writer.send({step_id: Action.RERUN for step_id in step_ids})
        return APIHandlerResponse(step_ids)

    def skip(self, dry_run=True, **tags):
        """Send message to mark steps to be skipped.
        Mark the specified steps so that they are skipped when execution reaches them.
        A step can only be skipped if it is either paused, marked to pause, paused due to failure, waiting or ready.

        :param dry_run: Boolean. API call doesn't have any effect when True.
        :param tags:    Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.
                        If tags are provided, then only the steps matching the tags will be skipped.
                        If no tags are provided, all possible steps will be marked to be skipped.
        :return:        An APIHandlerResponse object whose 'return_value' is the list of step IDs that were marked to
                        be skipped.
        """
        step_ids = list(extract_step_ids(filter_steps_by_action(filter_steps_by_tags(
            self._steps, tags), self._transitions, Action.MARKSKIP)))
        if not dry_run:
            self._callback_manager.actions_writer.send({step_id: Action.MARKSKIP for step_id in step_ids})
        return APIHandlerResponse(step_ids)

    def unskip(self, dry_run=True, **tags):
        """Send message to un-skip steps that have been marked to be skipped.
        A step can be un-skipped only if it is marked to be skipped but has not been skipped yet.
        Once a step has already been skipped, this API call will not affect them (they cannot be unskipped).

        :param dry_run: Boolean. API call doesn't have any effect when True.
        :param tags:    Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.
                        If tags are provided, then only the steps matching the tags will be un-skipped.
                        If no tags are provided, all possible steps will be marked to be skipped.
        :return:        An APIHandlerResponse object whose 'return_value' is the list of step IDs that were unmarked
                        from being skipped.
        """
        step_ids = list(extract_step_ids(filter_steps_by_action(filter_steps_by_tags(
            self._steps, tags), self._transitions, Action.UNSKIP)))
        if not dry_run:
            self._callback_manager.actions_writer.send({step_id: Action.UNSKIP for step_id in step_ids})
        return APIHandlerResponse(step_ids)


def make_api_client(socket_file, timeout=5):
    """Factory to create a MethodAPIClientWrapper that communicates using a SocketClient.

    :param socket_file: A Unix socket file that will be used to communicate.
    :param timeout:     The timeout (in seconds) while waiting for a response.
    :return:            A MethodAPIClientWrapper that serves requests at the given Unix socket file.
    """
    return MethodAPIClientWrapper(SocketClient(socket_file), timeout=timeout)
