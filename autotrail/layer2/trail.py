"""Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.

Layer 2 Trail server and client classes.

This module consists of the more user friendly Trail server and client classes which take a lot of the burden of
running the trail_manager, sending and receiving JSON API calls.

TrailServer is the server class.
TrailClient is the client class.
"""


import json
import signal
import socket

from multiprocessing import Process
from tempfile import mktemp
from time import sleep

from autotrail.core.dag import Step, topological_traverse
from autotrail.core.socket_communication import send_request, socket_server_setup
from autotrail.layer1.trail import AutoTrailException, make_dag, trail_manager
from autotrail.layer1.api import APICallField, APICallName, StatusField


class MatchTrailsException(AutoTrailException):
    """Exception raised when two trails do not match each other."""
    pass


def sigint_resistant_function(function, *args, **kwargs):
    """Makes the function SIGINT resistant by capturing the signal and doing nothing about it.
    Useful when we want the function to be run on a separate thread and not be interrupted by SIGINT.
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    return function(*args, **kwargs)


class TrailServer(object):
    """Class used to manage a Trail.

    This class does the following:
    1. Setup of the trail resources like backup files, state restoration, socket files etc.
    2. Run the trail server either directly or in threaded mode, allowing the trail to be both run and managed by the
       same program/script.

    Usage of this client:
    trail_definition -- Let this be a list of ordered pairs of Step objects representing the trail.
    socket_file      -- The socket file to use for communicating API calls and responses. If not provided, a temporary
                        socket file is created in /tmp with the prefix 'autotrail.socket.'.
                        This filename can be found at: TrailServer.socket_file
    dag_file         -- This is the filename that is used to store the state of the DAG during the trail run.
                        This file can be used to retrieve the last known state of the trail.
                        If not provided, a temporary file is created in /tmp with the prefix 'autotrail.'.
                        This filename can be found at: TrailServer.dag_file
    context          -- This is a user-defined object that needs to be passed to the action functions as an argument
                        when they are run.
    delay            -- This is the delay with which each iteration of the trail manager runs.
                        A longer delay means the trail will respond to any API calls slower, check status of Steps
                        slower, pass messages slower etc.
                        Defaults to 1 second.
    timeout          -- This is the timeout used when receiving messages to and from the socket.
                        A longer timeout means an API server will wait longer to receive connections. Keeping this high
                        will make the trail become slow just waiting for API requests.
    """
    def __init__(self, trail_definition, socket_file=None, dag_file=None, context=None, delay=1, timeout=0.0001,
                 backlog_count=1):
        self.context = context
        self.root_step = make_dag(trail_definition)
        self.socket_file = socket_file
        self.started = False
        self.timeout = timeout
        self.backlog_count = backlog_count
        self.trail_process = None # Will point to the trail process only in threaded mode.
        self.delay = delay

        if not dag_file:
            self.dag_file = mktemp(prefix='autotrail.')
        else:
            self.dag_file = dag_file

        if not socket_file:
            self.socket_file = mktemp(prefix='autotrail.socket.')
        else:
            self.socket_file = socket_file

        self.api_socket = socket_server_setup(self.socket_file, backlog_count=backlog_count, timeout=timeout)

    def restore_trail(self, dag_file, state_transformation_mapping=None):
        """Restore the state of the trail from the DAG file supplied when instantiating this object.

        Arguments:
        dag_file                     -- The JSON file containing the state of the entire trail, which will be used to
                                        restore the state. The JSON should be of the following form:
                                        {
                                            '<action function name>': {
                                                StatusField.STATE: <State of the step as an attribute of the Step class
                                                                    eg., Step.WAIT, Step.SUCCESS>,
                                                StatusField.RETURN_VALUE: <The string representation of the return value of
                                                                           the step.>,
                                                StatusField.PROMPT_MESSAGES: <Prompt messages from the step>,
                                                StatusField.OUTPUT_MESSAGES:<Output messages from the step>,
                                                StatusField.INPUT_MESSAGES: <Input messages sent to the step>,
                                                'parents': [
                                                    <Name of parent1>,
                                                    <Name of parent2>,
                                                ],
                                            }
                                        }
        state_transformation_mapping -- A dictionary that maps one state to another. When a step with a state that is a
                                        key in this dictionary is restored, its state will be set to the value.
                                        E.g., with the mapping:
                                        {
                                            Step.BLOCK: Step.PAUSE,
                                        }
                                        When a step is restored, that is in the state Step.BLOCK in trail_data, it will
                                        be put in Step.PAUSE.
                                        By default, they following mapping is used:
                                        {
                                            Step.BLOCKED: Step.PAUSED,
                                            Step.RUN: Step.PAUSED
                                        }
        """
        state_transformation_mapping = state_transformation_mapping or {Step.BLOCKED: Step.PAUSED,
                                                                        Step.RUN: Step.PAUSED}
        restore_trail(self.root_step, dag_file, state_transformation_mapping)

    def serve(self, threaded=False):
        """Starts a trail as a subprocess.

        Keyword Arguments:
        threaded -- Start the trail server as a separate process (using multiprocessing.Process). Useful when running
                    the server and client from the same script.

        Returns:
        None
        """
        backup_function = lambda root_step: backup_trail(root_step, self.dag_file)
        if threaded:
            self.trail_process = Process(
                target=sigint_resistant_function,
                args=(trail_manager, self.root_step, self.api_socket, backup_function),
                kwargs=dict(context=self.context, delay=self.delay))
            self.trail_process.start()
        else:
            trail_manager(self.root_step, self.api_socket, backup_function, context=self.context, delay=self.delay)


class TrailClientError(Exception):
    """Exception raised when the TrailClient encounters errors when making an API call."""
    pass


class TrailClient(object):
    """Client to interact with the Trail.

    This client makes API calls using JSON requests to the socket and decoding the JSON responses.
    Returns results normally.
    Raises TrailClientError if any error is returned by the trail server.
    """
    def __init__(self, socket_file):
        """Initialize the client.

        Arguments:
        socket_file -- The socket file to use for communicating API calls and responses.
        """
        self.socket_file = socket_file

    def _make_api_call(self, api_call):
        """Make the given API call by encoding it as JSON and decode the JSON response.

        Arguments:
        api_call -- A dictionary containing keys from the APICallField namespace and values specific to the API call
                    being made.

        Returns:
        The decoded JSON response (Boolean, string, list, dictionary etc).
        If an error was received, then TrailClientError is raised with details of the error.
        """
        try:
            response = send_request(self.socket_file, api_call)
        except socket.error:
            response = None

        if not response:
            return None

        if response['error']:
            raise TrailClientError(response['error'])
        return response['result']

    def start(self):
        """Send message to start a trail. This will change the state of all steps in 'Ready' state to 'Waiting' state.

        Returns:
        List of dictionaries (tags) of steps that were affected by the API call.
        This list will contain only the 'n' and 'name' tags of the steps affected.
        """
        return self._make_api_call({
            APICallField.NAME: APICallName.START,
            APICallField.DRY_RUN: False})

    def stop(self, interrupt=False):
        """Send mesage to stop the run of a trail.
        This is achieved by calling 'block' on all the steps.
        If interrupt is True, then the 'interrupt' method is called as well.

        Keyword Arguments:
        interrupt -- If True, then this will interrupt any running steps.

        Returns:
        List of dictionaries (tags) of steps that were affected by the block API call.
        This list will contain only the 'n' and 'name' tags of the steps affected.
        """
        # If the user wants it, kill all the running steps
        if interrupt:
            self.interrupt(dry_run=False)

        return self.block(dry_run=False)

    def shutdown(self, dry_run=True):
        """Send message to shutdown the trail server. After this call, no other API calls can be served.

        Returns:
        True  -- If the trail was shutdown.
        False -- False otherwise (in dry_run mode).
        """
        return self._make_api_call({
            APICallField.NAME: APICallName.SHUTDOWN,
            APICallField.DRY_RUN: dry_run})

    def send_message_to_steps(self, message, dry_run=True, **tags):
        """Send a message to matching steps.

        Keyword arguments:
        message      -- Any JSON encodable Python object.
        dry_run=True -- Results in a list of steps that the message will be sent to.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Returns:
        List of dictionaries (tags) of steps that were affected by the API call.
        This list will contain only the 'n' and 'name' tags of the steps affected.
        """
        return self._make_api_call({
            APICallField.NAME: APICallName.SEND_MESSAGE_TO_STEPS,
            APICallField.DRY_RUN: dry_run,
            APICallField.MESSAGE: message,
            APICallField.TAGS: tags})

    def list(self, **tags):
        """Send message to list all the steps' tags in a trail (in topological order).

        If no keyword arguments are provided, this will list the tags of all the steps in the trail.

        Keyword arguments:
        **tags       -- Any key=value pair provided in the arguments is treated as a tag.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Returns:
        List of dictionaries (tags) of the matching steps.
        """
        return self._make_api_call({
            APICallField.NAME: APICallName.LIST,
            APICallField.TAGS: tags})

    def status(self, fields=None, states=None, **tags):
        """Send message to get trail status.

        Keyword Arguments:
        fields -- List of field names to return in the results. A list of strings from StatusField class.
                  This will limit the status to the fields requested.
                  Defaults to:
                  [
                     StatusField.STATE,
                     StatusField.UNREPLIED_PROMPT_MESSAGE,
                     StatusField.OUTPUT_MESSAGES,
                     StatusField.RETURN_VALUE,
                  ]
                  If the trail has finished, the the fields used are:
                  [
                     StatusField.STATE,
                     StatusField.RETURN_VALUE,
                  ]

                  No matter what fields are passed, the following 2 fields will always be present in the result:
                      StatusField.N: <Sequence number of the step>
                      StatusField.NAME: <Name of the step>
                  This is because without these fields, it will be impossible to uniquely identify the steps.
        states -- List of strings that represent the states of a Step.
                  This will limit the status to only the steps that are in the given list of states.
        tags   -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                  Each step by default gets a tag viz., name=<action_function_name>.

        Returns:
        List of dictionaries, each containing the status of the matcing steps as follows:
        1. 2 keys as explained above (StatusField.N, StatusField.NAME)
        2. Other keys as specified with the fields keyword argument.
        """
        # Default to the following fields
        if fields is None:
            fields = [StatusField.STATE, StatusField.UNREPLIED_PROMPT_MESSAGE, StatusField.OUTPUT_MESSAGES,
                      StatusField.RETURN_VALUE]

        if states is None:
            states = []

        return self._make_api_call({
            APICallField.NAME: APICallName.STATUS,
            APICallField.STATUS_FIELDS: fields,
            APICallField.STATES: states,
            APICallField.TAGS: tags})

    def steps_waiting_for_user_input(self, **tags):
        """Get status of steps that are waiting for user input.
        Limits the fields to only include the UNREPLIED_PROMPT_MESSAGE.
        Excludes steps that do not have any UNREPLIED_PROMPT_MESSAGE.

        Keyword Arguments:
        tags   -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                  Each step by default gets a tag viz., name=<action_function_name>.

        Returns:
        List of dictionaries, each containing the status of the matcing steps as follows:
        1. 2 keys viz., StatusField.N, StatusField.NAME, to be able to uniquely identify the steps.
        2. The unreplied prompt message for the step.
        """
        statuses = self.status(fields=[StatusField.UNREPLIED_PROMPT_MESSAGE], states=[Step.RUN], **tags)
        if statuses:
            statuses = [status for status in statuses if status[StatusField.UNREPLIED_PROMPT_MESSAGE]]
        return statuses

    def pause(self, dry_run=True, **tags):
        """Send message to pause steps.
        Mark the specified steps so that they are paused.
        A step can only be paused if it is ready or waiting.

        If tags are provided, then only the steps matching the tags will be paused.
        If no tags are provided, all possible steps will be marked to be paused.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be paused.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Returns:
        List of dictionaries (tags) of steps that were affected by the API call.
        This list will contain only the 'n' and 'name' tags of the steps affected.
        """
        return self._make_api_call({
            APICallField.NAME: APICallName.PAUSE,
            APICallField.DRY_RUN: dry_run,
            APICallField.TAGS: tags})

    def pause_branch(self, dry_run=True, **tags):
        """Send message to pause all steps in matching branches.
        Look for all the steps that are present in the branches originating from each of the matching steps and mark
        them so that they are paused.
        A step can only be paused if it is ready or waiting.

        If tags are provided, then only the steps matching the tags will be paused.
        If no tags are provided, all possible steps will be marked to be paused.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be paused.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Returns:
        List of dictionaries (tags) of steps that were affected by the API call.
        This list will contain only the 'n' and 'name' tags of the steps affected.
        """
        return self._make_api_call({
            APICallField.NAME: APICallName.PAUSE_BRANCH,
            APICallField.DRY_RUN: dry_run,
            APICallField.TAGS: tags})

    def interrupt(self, dry_run=True, **tags):
        """Send message to interrupt running steps.
        This will interrupt the running steps by sending them SIGINT. A step can be interrupted only if it is running.

        If tags are provided, then only the steps matching the tags will be interrupted.
        If no tags are provided, all possible steps will be marked to be interrupted.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be interrupted.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Returns:
        List of dictionaries (tags) of steps that were affected by the API call.
        This list will contain only the 'n' and 'name' tags of the steps affected.
        """
        return self._make_api_call({
            APICallField.NAME: APICallName.INTERRUPT,
            APICallField.DRY_RUN: dry_run,
            APICallField.TAGS: tags})

    def resume(self, dry_run=True, **tags):
        """Send message to resume steps.
        Mark the specified steps so that they are resumed.
        A step can only be resumed if it is either paused, marked to pause, paused due to failure or interrupted.

        If tags are provided, then only the steps matching the tags will be resumed.
        If no tags are provided, all possible steps will be marked to be resumed.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be resumed.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Returns:
        List of dictionaries (tags) of steps that were affected by the API call.
        This list will contain only the 'n' and 'name' tags of the steps affected.
        """
        return self._make_api_call({
            APICallField.NAME: APICallName.RESUME,
            APICallField.DRY_RUN: dry_run,
            APICallField.TAGS: tags})

    def next_step(self, step_count=1, dry_run=True):
        """Send message to resume the next step.

        When multiple steps have been paused, or if an branch has been paused, they can be resumed one step at a time
        in the correct order using this method.

        Keyword arguments:
        step_count -- A number greater than 0. This is the number of steps to run before pausing again.
                      By default this will resume only one step.

        Returns:
        List of dictionaries (tags) of steps that were affected by the API call.
        This list will contain only the 'n' and 'name' tags of the steps affected.
        """
        return self._make_api_call({
            APICallField.NAME: APICallName.NEXT_STEPS,
            APICallField.STEP_COUNT: step_count,
            APICallField.DRY_RUN: dry_run})

    def resume_branch(self, dry_run=True, **tags):
        """Send message to resume all steps in matching branches.
        Look for all the steps that are present in the branches originating from each of the matching steps and mark
        them so that they are resumed.
        A step can only be resumed if it is either paused, marked to pause, paused due to failure or interrupted.

        If tags are provided, then only the steps matching the tags will be resumed.
        If no tags are provided, all possible steps will be marked to be resumed.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be resumed.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Returns:
        List of dictionaries (tags) of steps that were affected by the API call.
        This list will contain only the 'n' and 'name' tags of the steps affected.
        """
        return self._make_api_call({
            APICallField.NAME: APICallName.RESUME_BRANCH,
            APICallField.DRY_RUN: dry_run,
            APICallField.TAGS: tags})

    def skip(self, dry_run=True, **tags):
        """Send message to mark steps to be skipped.
        Mark the specified steps so that they are skipped when execution reaches them.
        A step can only be skipped if it is either paused, marked to pause, paused due to failure, waiting or ready.

        If tags are provided, then only the steps matching the tags will be skipped.
        If no tags are provided, all possible steps will be marked to be skipped.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be skipped.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Returns:
        List of dictionaries (tags) of steps that were affected by the API call.
        This list will contain only the 'n' and 'name' tags of the steps affected.
        """
        return self._make_api_call({
            APICallField.NAME: APICallName.SKIP,
            APICallField.DRY_RUN: dry_run,
            APICallField.TAGS: tags})

    def unskip(self, dry_run=True, **tags):
        """Send message to un-skip steps that have been marked to be skipped.
        A step can be un-skipped only if it is marked to be skipped but has not been skipped yet.
        Once a step has already been skipped, this API call will not affect them (they cannot be unskipped).

        If tags are provided, then only the steps matching the tags will be un-skipped.
        If no tags are provided, all possible steps will be marked to be skipped.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be unskipped.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Returns:
        List of dictionaries (tags) of steps that were affected by the API call.
        This list will contain only the 'n' and 'name' tags of the steps affected.
        """
        return self._make_api_call({
            APICallField.NAME: APICallName.UNSKIP,
            APICallField.DRY_RUN: dry_run,
            APICallField.TAGS: tags})

    def block(self, dry_run=True, **tags):
        """Send message to block steps.
        Mark the specified steps so that they are blocked.
        A step can only be blocked if it is either paused, marked to pause, paused due to failure, waiting or ready.

        If tags are provided, then only the steps matching the tags will be blocked.
        If no tags are provided, all possible steps will be marked to be skipped.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be blocked.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Returns:
        List of dictionaries (tags) of steps that were affected by the API call.
        This list will contain only the 'n' and 'name' tags of the steps affected.
        """
        return self._make_api_call({
            APICallField.NAME: APICallName.BLOCK,
            APICallField.DRY_RUN: dry_run,
            APICallField.TAGS: tags})

    def unblock(self, dry_run=True, **tags):
        """Send message to un-block steps that have been marked to be blocked.
        A step can be un-blocked only if it is marked to be blocked but has not been blocked yet.
        Once a step has already been blocked, this API call will not affect them (they cannot be unblocked).

        If tags are provided, then only the steps matching the tags will be unblocked.
        If no tags are provided, all possible steps will be marked to be skipped.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be unblocked.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Returns:
        List of dictionaries (tags) of steps that were affected by the API call.
        This list will contain only the 'n' and 'name' tags of the steps affected.
        """
        return self._make_api_call({
            APICallField.NAME: APICallName.UNBLOCK,
            APICallField.DRY_RUN: dry_run,
            APICallField.TAGS: tags})

    def set_pause_on_fail(self, dry_run=True, **tags):
        """Send message to set the pause_on_fail flag of a step.
        By default, all steps are marked with pause_on_fail so that they are paused if they fail. If a step's
        pause_on_fail attribute was explicitly set to False, it can be set back to True using this method.
        This API call sets this flag so that the Step will be paused if it fails.

        If tags are provided, then only the steps matching the tags will be paused.
        If no tags are provided, all possible steps will be marked to be paused.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be paused.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Returns:
        List of dictionaries (tags) of steps that were affected by the API call.
        This list will contain only the 'n' and 'name' tags of the steps affected.
        """
        return self._make_api_call({
            APICallField.NAME: APICallName.SET_PAUSE_ON_FAIL,
            APICallField.DRY_RUN: dry_run,
            APICallField.TAGS: tags})

    def unset_pause_on_fail(self, dry_run=True, **tags):
        """Send message to clear the pause_on_fail flag of a step.
        By default, all steps are marked with pause_on_fail so that they are paused if they fail.
        This allows the user to either re-run the step, skip it or block it.
        This API call unsets this flag. This will cause the Step to fully fail (irrecoverably) if the action function
        fails (raises an exception).

        If tags are provided, then only the steps matching the tags will be paused.
        If no tags are provided, all possible steps will be marked to be paused.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be paused.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Returns:
        List of dictionaries (tags) of steps that were affected by the API call.
        This list will contain only the 'n' and 'name' tags of the steps affected.
        """
        return self._make_api_call({
            APICallField.NAME: APICallName.UNSET_PAUSE_ON_FAIL,
            APICallField.DRY_RUN: dry_run,
            APICallField.TAGS: tags})


def serialize_trail(root_step):
    """Serialize a trail by serializing each Step starting at root Step.

    A Step is serialized by storing the attributes of each step in the following form:
    {
        '<action function name>': {
            StatusField.STATE: <State of the step as an attribute of the Step class eg., Step.WAIT, Step.SUCCESS>,
            StatusField.RETURN_VALUE: <The string representation of the return value of the step.>,
            StatusField.PROMPT_MESSAGES: <Prompt messages from the step>,
            StatusField.OUTPUT_MESSAGES:<Output messages from the step>,
            StatusField.INPUT_MESSAGES: <Input messages sent to the step>,
            'parents': [
                <Name of parent1>,
                <Name of parent2>,
            ],
        }
    }

    Note: The names of the children need not be stored since they are redundant. We can do this because we are not
    dealing with a single Step here but the entire trail. If two vertices in 2 trails have same parents but different
    children, then there will be atleast 1 child in each trail that will not match thus making the trails different.

    Returns:
    dictionary -- The serialized trail data in the above form.
    """
    trail_data = {}
    for step in topological_traverse(root_step):
        parents_names = [str(parent) for parent in step.parents]
        trail_data[str(step)] = {
            StatusField.STATE: str(step.state),
            StatusField.RETURN_VALUE: str(step.return_value),
            StatusField.PROMPT_MESSAGES: step.prompt_messages,
            StatusField.OUTPUT_MESSAGES: step.output_messages,
            StatusField.INPUT_MESSAGES: step.input_messages,
            'parents': parents_names,
        }
    return trail_data


def deserialize_trail(root_step, trail_data, state_transformation_mapping):
    """Restore the state of a trail from trail_data.

    Updates the matching step with the step's attribute data obtained from the trail_data.
    This is useful when 'restoring' the state of a trail from a backed-up source.

    Arguments:
    root_step                    -- The root step of the trail DAG.
    trail_data                   -- A dictionary containg the trail data in the following form:
                                    {
                                        '<action function name>': {
                                            StatusField.STATE: <State of the step as an attribute of the Step class
                                                                eg., Step.WAIT, Step.SUCCESS>,
                                            StatusField.RETURN_VALUE: <The string representation of the return value of
                                                                       the step.>,
                                            StatusField.PROMPT_MESSAGES: <Prompt messages from the step>,
                                            StatusField.OUTPUT_MESSAGES:<Output messages from the step>,
                                            StatusField.INPUT_MESSAGES: <Input messages sent to the step>,
                                            'parents': [
                                                <Name of parent1>,
                                                <Name of parent2>,
                                            ],
                                        }
                                    }
    state_transformation_mapping -- A dictionary that maps one state to another. When a step with a state that is a
                                    key in this dictionary is restored, its state will be set to the value.
                                    E.g., with the mapping:
                                    {
                                        Step.BLOCK: Step.PAUSE,
                                    }
                                    When a step is restored, that is in the state Step.BLOCK in trail_data, it will
                                    be put in Step.PAUSE.

    Post-condition:
    Topologically traverses the DAG from the given root_step and modifies the state of the Steps based on the data
    in trail_data.

    Raises:
    MatchTrailsException -- When the trails (from root_step and trail_data) do not match.
    """
    for step in topological_traverse(root_step):
        if str(step) not in trail_data:
            raise MatchTrailsException('Step: {} does not exist in trail data.'.format(str(step)))
        step_data = trail_data[str(step)]
        parents = step_data['parents']
        step_parents = map(str, step.parents)
        if sorted(parents) != sorted(step_parents):
            raise MatchTrailsException('The parents of step: {} do not match the ones in trail data.'.format(str(step)))
        step.state = state_transformation_mapping.get(step_data[StatusField.STATE], step_data[StatusField.STATE])
        step.return_value = step_data[StatusField.RETURN_VALUE]
        step.prompt_messages = step_data[StatusField.PROMPT_MESSAGES]
        step.output_messages = step_data[StatusField.OUTPUT_MESSAGES]
        step.input_messages = step_data[StatusField.INPUT_MESSAGES]


def backup_trail(root_step, filename):
    """Save the JSON encoded serialized state of the trail (using serialize_trail) to the given file."""
    with open(filename, 'w') as trail_file:
        json.dump(serialize_trail(root_step), trail_file)


def restore_trail(root_step, filename, state_transformation_mapping):
    """Restore the state of a trail from the given file.

    Arguments:
    root_step                    -- The root step of the trail DAG.
    filename                     -- The file containting the state of the steps in the JSON format compatible with
                                    deserialize_trail.
    state_transformation_mapping -- A dictionary that maps one state to another. When a step with a state that is a
                                    key in this dictionary is restored, its state will be set to the value.
                                    E.g., with the mapping:
                                    {
                                        Step.BLOCK: Step.PAUSE,
                                    }
                                    When a step is restored, that is in the state Step.BLOCK in trail_data, it will
                                    be put in Step.PAUSE.

    Post-condition:
    All the steps will have their attributes updated as obtained from the file and their state will be set based on
    the given state_transformation_mapping.
    """
    with open(filename, 'r') as trail_file:
        trail_data = json.load(trail_file)
    deserialize_trail(root_step, trail_data, state_transformation_mapping)


def wait_till_change(func, predicate, validate, delay=5, max_tries=100):
    """Waits till the given function returns a different value.
    Repeatedly calls 'func' with a gap of 'delay' seconds.
    Gives up if the value does not change after 'max_tries' attempts.
    Since there is no seed value being accepted for func(), it is called a maximum of max_tries + 1 times.

    Arguments:
    func      -- The function that needs to be called. This function should have a comparable return value, i.e., its
                 return value should:
                 1. Support equality check (a == b).
                 2. Should change over time. A function that always returns None will not be helpful in this case.
                 func can raise StopIteration if it wants the iteration over wait_till_change to stop at any point.
    predicate -- A predicate function that acts as a guard for func.
                 A True value will result in a call to func().
                 A False value will return from this function and the value returned will be the last value obtained
                 from func().
    validate  -- A predicate function that validates the return values of func. While the predicate parameter provides
                 a function that guards the function call, validate informs whether the value returned by func() is
                 valid (e.g. comparable).
                 The signature of this predicate function is:
                 Arguments:
                 value -- The return value of func()
                 Returns:
                 True  -- If the value is valid.
                 False -- If the value is not valid and needs to be ignored.
                 The call to func() is counted whether validate returns True or False.

    Keyword Arguments:
    delay     -- The number of seconds to wait between calls to 'func'.
                 Defaults to 5 seconds.
    max_tries -- If the value returned by func() does not change, this is the maximum number of times func() will be
                 tried before giving up.
                 Defaults to 100 tries.

    Returns:
    A generator that yields the latest return value of func().
    The generator raises StopIteration under 3 circumstances:
    1. If the max_tries has been reached.
    2. If the predicate returns a False value.
    3. If func() raises StopIteration.
    """
    old_value = func()
    new_value = old_value

    try_count = 0

    while try_count < max_tries:
        if new_value == old_value:
            # Sleep first because we promised a delay between consecutive calls
            # to func.
            sleep(delay)
            if predicate():
                value = func()
                try_count += 1
                if validate(value):
                    new_value = value
            else:
                break
        else:
            old_value = new_value
            try_count = 0
            yield new_value
