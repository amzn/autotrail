"""Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.

Trail management data structures and algorithms.

This module provides the Layer 1 of AutoTrail viz., the trail_manager. All layer 2 clients use the trail_manager to run
and manage a trail.
Everything below are helpers for the layer2 clients. They can be used directly, but need understanding of the internals
of AutoTrail.
"""


import logging

from collections import namedtuple
from functools import partial
from multiprocessing import Process
from multiprocessing import Queue as ProcessQueue
from socket import SHUT_RDWR
from time import sleep

from six.moves.queue import Empty as QueueEmpty

from .api import get_progeny, handle_api_call, log_step, search_steps_with_states
from autotrail.core.dag import is_dag_acyclic, Step, topological_traverse, topological_while
from autotrail.core.socket_communication import serve_socket


class AutoTrailException(Exception):
    """Base exception class used for the exceptions raised by AutoTrail.

    This is typically extended by other exceptions with a more specific name.
    """
    pass


class CyclicException(AutoTrailException):
    """Exception raised when a given trail definition contains cycles."""
    pass


StepResult = namedtuple('StepResult', ['result', 'return_value'])


def step_manager(step, args, kwargs):
    """Manage the running of a step.

    This function is meant to be run in either a separate thread or process, hence return value is found in the
    result_queue.

    This will:
    1) Run the action function in the given Step.
    2) Catch any exception it raises and return the str(Exception)

    Return the result in the form of a StepResult object. The result is put in the result_queue.

    The result_queue object is a multiprocess.Queue like object and the only attribute used is put(<message>).
    """
    try:
        log_step(logging.debug, step, 'Starting run.')
        return_value = step.action_function(*args, **kwargs)
        step_result = StepResult(result=step.SUCCESS, return_value=return_value)
        log_step(logging.info, step, 'Run successful. Return value: {}.'.format(str(return_value)))
    except Exception as e:
        log_step(logging.exception, step,
                 'Exception encountered when running the step. Error message: {}.'.format(str(e)))
        step_result = StepResult(result=step.FAILURE, return_value=e)

    step.result_queue.put(step_result)


def run_step(step):
    """Run a given step.

    1. Starts a process to run the next step.
    2. Creates a queue to communicate with the process.
    3. Changes the state of the Step to Step.RUN.
    """
    log_step(logging.debug, step, 'Preparing objects to run.')

    step.prompt_queue = ProcessQueue()
    step.input_queue = ProcessQueue()
    step.output_queue = ProcessQueue()
    step.result_queue = ProcessQueue()

    # Reset some attributes in-case the Step is being re-run.
    # Older values present can be confusing to the user, so remove them.
    step.prompt_messages = []
    step.input_messages = []
    step.return_value = None
    step.environment = TrailEnvironment(step.prompt_queue, step.input_queue, step.output_queue)

    try:
        args, kwargs = step.pre_processor(step.environment, step.context)
        step.process = Process(target=step_manager, args=(step, args, kwargs))
        log_step(logging.debug, step, 'Starting subprocess to run step.')
        step.process.start()
        step.state = step.RUN
    except Exception as e:
        step.state = step.FAILURE
        step.return_value = e


def check_running_step(step):
    """Check if the step has completed by trying to obtain its result."""
    try:
        step_result = step.result_queue.get_nowait()
        step.state = step_result.result
        step.return_value = step_result.return_value
    except QueueEmpty:
        # Step is still running and hasn't returned any results, hence do nothing.
        pass
    finally:
        collect_output_messages_from_step(step)
        collect_prompt_messages_from_step(step)

    if step.state == step.SUCCESS:
        try:
            step.return_value = step.post_processor(step.environment, step.context, step.return_value)
            log_step(logging.debug, step,
                     'Step has succeeded. Changed state to: {}. Setting return value to: {}'.format(
                         step.state, step.return_value))
        except Exception as e:
            step.state = step.FAILURE
            step.return_value = e

    if step.state in step.FAILURE:
        if step.pause_on_fail:
            step.state = step.PAUSED_ON_FAIL

        if step.skip_progeny_on_failure:
            skip_progeny(step)

        try:
            step.return_value = step.failure_handler(step.environment, step.context, step.return_value)
        except Exception as e:
            step.return_value = e

        log_step(logging.debug, step,
                 'Step has failed. Changed state to: {}. Setting return value to: {}'.format(
                     step.state, step.return_value))


DONE_STATES = [
    # These states are done, therefore their downstream steps can be picked up.
    Step.SUCCESS,
    Step.SKIPPED,
]


IGNORE_STATES = [
    # Ignore the following states as these steps or their downstream cannot be run.
    Step.FAILURE,
    Step.BLOCKED,
]


STATE_TRANSITIONS = {
    # Mapping expressing state to the corresponding transition functions.
    # These functions produce side-effects by changing the state of the steps that are passed to them (if necessary).
    Step.WAIT           : run_step,
    Step.RUN            : check_running_step,
    Step.TOSKIP         : lambda step: setattr(step, 'state', Step.SKIPPED),
    Step.TOPAUSE        : lambda step: setattr(step, 'state', Step.PAUSED),
    Step.TOBLOCK        : lambda step: setattr(step, 'state', Step.BLOCKED),
}


def trail_manager(root_step, api_socket, backup, delay=5, done_states=DONE_STATES,
                  ignore_states=IGNORE_STATES, state_transitions=STATE_TRANSITIONS):
    """Manage a trail.

    This is the lowest layer of execution of a trail, a Layer 1 client that directly manages a trail.

    Using this function directly requires no knowledge of the working of autotrail, but the requirements for using this
    client should be fulfilled. They are detailed in the documentation below.

    Arguments:
    root_step         -- A Step like object fulfilling the same contract of states it can be in. A trail is represented
                         by the a DAG starting at root_step. A DAG can be created from a list of ordered pairs of Step
                         objects using the make_dag function provided in this module.
    api_socket        -- A socket.socket object where the API is served. All API calls are recieved and responded via
                         this socket.
    backup            -- A call-back function to backup the state of the steps. This function should accept only one
                         parameter viz., the root_step. It will be called with every iteration of the main trail loop
                         to store the state of the DAG. Avoid making this a high latency function to keep the trail
                         responsive.
                         The return value of this function is ignored.
                         Ensure this function is exception safe as an exception here would break out of the trail
                         manager loop.

    Keyword arguments:
    delay             -- The delay before each iteration of the loop, this is the delay with which the trail_manager
                         iterates over the steps it is keeping track of.
                         It is also the delay with which it checks for any API calls.
                         Having a long delay will make the trail less responsive to API calls.
    done_states       -- A list of step states that are considered to be "done". If a step is found in this state, it
                         can be considered to have been run.
    ignore_states     -- A list of step states that will be ignored. A step in these states cannot be traversed over,
                         i.e., all downstream steps will be out of traversal and will never be reached (case when a
                         step has failed etc).
    state_transitions -- A mapping of step states to functions that will be called if a step is found in that state.
                         These functions will be called with 1 parameter - the step. Their return value is
                         ignored. These functions can produce side effects by altering the state of the step if needed.

    Responsibilities of the manager include:
    1) Run the API call server.
    2) Iterate over the steps in a topological traversal based on the state_functions and ignorable_state_functions
       data structures.
    3) Invoked the call-back backup function to save the trail state.
    4) Return when the API server shuts down.
    """
    logging.debug('Starting trail manager.')

    # Preparing a list of steps as these are frequently needed for serving the API calls, but topological traversal
    # is expensive whereas, iteration over a list is not.
    steps = list(topological_traverse(root_step))

    # The trail_manager uses the topological_while function, which provides a way to traverse vertices in a topological
    # order (guaranteeing the trail order) while allowing the trail_manager to control the flow.
    # The topological traversal has the effect that a step is not acted upon, unless all its parents are done.
    # The done_check and ignore_check call-backs allow us to tell the topological_while function if a step can be
    # considered done or not.
    done_check = lambda step: True if step.state in done_states else False
    ignore_check = lambda step: True if step.state in ignore_states else False
    step_iterator = topological_while(root_step, done_check, ignore_check)

    while True:
        # It is important to first serve API calls before working on steps because we want a user's request to
        # affect any change before the trail run changes it.
        to_continue = serve_socket(api_socket, partial(handle_api_call, steps=steps))
        if to_continue is False:
            logging.info('API Server says NOT to continue. Shutting down.')
            api_socket.shutdown(SHUT_RDWR)
            break

        step = next(step_iterator, None)
        if step and step.state in state_transitions:
            state_transitions[step.state](step)

        backup(root_step)
        sleep(delay)


def skip_progeny(step):
    """Skip the progeny of the given step."""
    for step in search_steps_with_states(get_progeny([step]), states=[Step.WAIT, Step.TOPAUSE]):
        step.state = Step.SKIPPED
        log_step(logging.info, step, (
            'Skipping step as one of its previous steps failed and it had '
            'skip_progeny_on_failure=True.'))


class TrailEnvironment(object):
    """Class that provides a run-time environment to the action functions to interact with AutoTrail.

    Sometimes, at runtime, an action function may need to use some mechanisms of AutoTrail, all such mechanisms will be
    provided as methods in this class.

    Arguments:
    prompt_queue -- multiprocessing.Queue like object which is used to send prompt messages to the user.
                    An action function can write to this queue and the messages will be conveyed to the user.
                    This is different from the output_queue in that messages in this queue will be expecting a reply.
                    This is like an input prompting resource for the action function, hence the name.
    input_queue  -- multiprocessing.Queue like object which is used to send messages to the action function.
                    This is like an input source for the action function, hence the name.
    output_queue -- multiprocessing.Queue like object which is used to send messages to the user.
                    An action function can write to this queue and the messages will be conveyed to the user.
                    This is like an output resource for the action function, hence the name.
                    Messages in this queue are considered to be FYI and do not need a reply.
    """
    def __init__(self, prompt_queue, input_queue, output_queue):
        self.prompt_queue = prompt_queue
        self.input_queue = input_queue
        self.output_queue = output_queue

    def input(self, prompt, timeout=None):
        """Request input from the user.

        This method sends the 'prompt' as a message to the user and blocks until the user replies to the Step with the
        response.

        Arguments:
        prompt -- String - Message to be shown the user.

        Keyword Arguments:
        timeout -- int - Time in seconds that this method will block for an answer from the user.
                         Defaults to None - block indefinitely.
        """
        self.prompt_queue.put(prompt)
        return self.input_queue.get(timeout=timeout)

    def output(self, message):
        """Send a message to the user.

        This method sends the 'message' to the user. This method does not block.

        Arguments:
        message -- String - Message to be shown the user.
        """
        self.output_queue.put(message)


def collect_output_messages_from_step(step):
    """Collect all output messages sent by the action function in Step.

    When an action function wants to send a message, it will put them in the output_queue provided to it.
    This function reads all messages in the Queue and updates the Step.output_messages attribute of Step with the newly
    collected messages.

    Arguments:
    step -- An object of type Step or similar.

    Post-condition:
    The following attributes of the given Step object will be updated:
    output_queue        -- will be drained of any messages.
    output_messages     -- will be appended with the messages obtained from output_queue.
    """
    messages = []
    while True:
        try:
            message = step.output_queue.get_nowait()
            messages.append(message)
            log_step(logging.info, step, 'Output message sent: {}'.format(str(message)))
        except QueueEmpty:
            break
    step.output_messages.extend(messages)


def collect_prompt_messages_from_step(step):
    """Collect all prompt messages sent by the action function in Step.

    When an action function wants to prompt the user for some input, it sends a message into the prompt_queue provided
    to it.
    This function reads all messages in the prompt Queue and updates the following attribute of Step with the
    collected messages:
    Step.prompt -- This is a list of all messages sent by the step so far.

    Arguments:
    step -- An object of type Step or similar.

    Post-condition:
    The following attributes of the given Step object will be updated:
    prompt_queue        -- will be drained of any messages.
    prompt_messages     -- will be appended with the messages obtained from output_queue.
    """
    messages = []
    while True:
        try:
            message = step.prompt_queue.get_nowait()
            messages.append(message)
            log_step(logging.info, step, 'Prompt message sent: {}'.format(str(message)))
        except QueueEmpty:
            break

    step.prompt_messages.extend(messages)


def make_dag(trail_definition):
    """Convert a given trail definition (ordered pair representation) into a DAG.

    Returns:
    root_step          -- obtained from the Left-hand-side of the first ordered pair.

    Raises:
    CyclicException -- If the trail definition contains cycles.
    """
    # A trail definition using ordered pair approach will consist of a list of ordered pairs of the form:
    # [
    #     (parent_step, child_step),
    #     ...
    # ]
    root_step, child_step = trail_definition.pop(0)
    root_step.add_child(child_step)
    for parent_step, child_step in trail_definition:
        parent_step.add_child(child_step)

    if not is_dag_acyclic(root_step):
        raise CyclicException('The trail contains cycles.')

    assign_sequence_numbers_to_steps(root_step)

    return root_step


def assign_sequence_numbers_to_steps(root_step, tag_name='n'):
    """Assign unique numbers to each step in a trail.

    A Step is a wrapper around an action function. However, for a given trail, Steps are unique while action_functions
    may not be. E.g.,

    def action_function_a(context):
        pass

    step_a1 = Step(action_function_a)
    step_a2 = Step(action_function_a)

    While the same action function is being referred to by the steps, they are different objects.
    We need a way to uniquely refer to a step. While the 'name' tag allows us to refer to a Step using its action
    function name, it is not unique.
    This function iterates over the DAG in topological order and assigns a simple tag to each step
    (calling it 'n' by default).
    The assignment starts at the root step being 0 and the last topological step having the highest number.

    Arguments:
    root_step -- A Step like object which is the starting point of the DAG.
    tag_name  -- The name of the tag.
                 Defaults to 'n'.
    """
    for number, step in enumerate(topological_traverse(root_step)):
        step.tags[tag_name] = number
