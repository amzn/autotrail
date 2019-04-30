"""Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.

Action Function helpers

This module defines some helpers that make writing action functions very easy.

So far, there are 2 kinds of helpers:
1) Decorators       -- These are used to decorate action functions and context classes to genereate some boilerplate
                       code that saves typing.
2) Action Functions -- Action functions can be of two types: Python functions or classes with a __call__ method defined.
                       This helpers module provides some classes which handle some common action functions and allow the
                       user to avoid writing a lot of boilerplate code.
"""


from collections import deque
from time import sleep

from autotrail.core.dag import Step
from autotrail.layer1.api import StatusField


def historian(function, limit):
    """A generator that will call the function and store the result and yield the list of stored results (like a
    historian).

    Arguments:
    function -- The function to be called with each 'next()' call to this generator.
    limit    -- Limits the length of the history list that is yielded.

    Returns:
    A generator that calls the given function and yields all the values collected so far (from all iterations).
    Stops when the function call raises StopIteration or the caller uses some other logic to not proceed with further
    yields from the generator.
    """
    history = deque(maxlen=limit)
    while True:
        history.append(function())
        yield history


def filtered_handlers(value, filter_handler_pairs):
    """
    value                -- The value to which the filter_handler_pairs will be applied.
    filter_handler_pairs -- A list of tuples of the following form:
                            [
                                (<filter_function>, <handler_function>),
                            ]
                            Where:
                            <filter_function> accepts the value returned by the function call and returns the value
                                              on which the handler_function will work. This is useful to normalize
                                              the values to comply with the handler_function's input requirements.
                                              If the filter_function returns a False value, then the handler_function
                                              will not be called.
                            <handler_function> accepts the value  returned by the filter_function. Any value returned
                                              by this function is ignored.

    Post-condition:
    Each filter_function is called with the given value and if the filter_function returns a true value, then the
    corresponding handler_function will be called with the return value of the filter_function.
    """
    for filter_function, handler_function in filter_handler_pairs:
        filtered_value = filter_function(value)
        if filtered_value:
            handler_function(filtered_value)


def monitor(function, result_predicate, pre_processor, filter_handler_pairs, delay=60, history_limit=1):
    """Monitor a function by calling it periodically and using the filter_handler_pairs to handle the return value.

    Arguments:
    function             -- The function to monitor. Will be called every delay seconds.
    result_predicate     -- A predicate function that is used to decide when to stop monitoring (exit this function).
                            Should accept one parameter, the history of values returned by the function.
                            The history is a list. Its max length is defined by the given history_limit.
                            The last element of the history (history[-1]) is the latest value returned by the function.
                            Should return a True or False value.
    pre_processor        -- A function to process the result returned by the function before the filter_handler_pairs
                            work on it. Should accept one parameter, the history of values returned by the function.
                            If this function returns a true value, only then the filter_handler_pairs will be called.
    filter_handler_pairs -- A list of tuples of the following form:
                            [
                                (<filter_function>, <handler_function>),
                            ]
                            Where:
                            <filter_function> accepts the value returned by the function call and returns the value
                                              on which the handler_function will work. This is useful to normalize
                                              the values to comply with the handler_function's input requirements.
                                              If the filter_function returns a False value, then the handler_function
                                              will not be called.
                            <handler_function> accepts the value  returned by the filter_function. Any value returned
                                              by this function is ignored.

    Keyword Arguments:
    delay                -- The delay in seconds between consecutive calls to the given function.
    history_limit        -- The maximum number of return values to be stored in the history.
    """
    for history in historian(function, history_limit):
        if not result_predicate(history):
            break
        result = pre_processor(history)
        if result:
            filtered_handlers(result, filter_handler_pairs)
        sleep(delay)


def diff_lists(lists):
    """Diff the last list with the other lists and return a list of elements that are in the last list but not in the
    any of the previous lists.

    Arguments:
    lists -- The last list (-1) in this list of lists will be diffed with the other lists.

    Returns:
    A list of elements that exist in the last list but do not exist in any of the other lists.
    If the last list is None, returns [].
    If the previous lists don't exist then returns the last list (in this case, the only list).
    If any of the previous lists are a false value (None or []), they are ignored.
    """
    lists_reversed = reversed(lists)
    new_elements = next(lists_reversed, []) or []

    for previous_list in lists_reversed:
        # This can be done faster using set difference but that doesn't work if the elements of the list are
        # dictionaries.
        previous_list = previous_list or []
        new_elements = [element for element in new_elements if element not in previous_list]
    return list(new_elements)


def monitor_trail(trail_client, status_filter_handler_pairs, status_kwargs=None, delay=60, max_tries=3):
    """Monitor a trail periodically and call the status_handlers (functions) with a list of step statuses that have
    changed.

    The handlers will be called *only* with step statuses that have changed and will not contain other step statuses.

    This function blocks until the trail finishes running. It will call the status handler functions in an infinite
    loop until the trail ends. If you provide a very short delay, be sure about it as it can overwhelm the trail queue.

    Arguments:
    trail_client                -- The TrailClient or similar object which will be used to fetch the status.
    status_filter_handler_pairs -- A list of tuples of the following form:
                                   [
                                        (<filter_function>, <handler_function>),
                                   ]
                                   Where:
                                   <filter_function> accepts a list of statuses and returns a list of statuses
                                                     relevant to the handler. If the returned list is empty, then
                                                     the handler won't be called. If the list is non empty, then
                                                     the handler will be called with it.
                                   <handler_function> accepts the list of step statuses returned by filter_function.

                                   Step statuses is a list of dictionaries of the following form:
                                    [
                                        {
                                            StatusField.N: <Sequence number of the step>,
                                            StatusField.NAME: <Name of the step>,
                                            <Fields specified with status_kwargs>: <values of those fields>,
                                        },
                                        ...
                                    ]
                                    Two fields (StatusField.N and StatusField.NAME) will always be present in the
                                    result, because without these it will be impossible to uniquely identify the steps.

    Keyword Arguments:
    status_kwargs   -- A dictionary of keyword arguments that will be passed as-is when invoking the status() method.
                       Defaults to:
                        dict(fields=[StatusField.STATE, StatusField.UNREPLIED_PROMPT_MESSAGE,
                                     StatusField.OUTPUT_MESSAGES, StatusField.RETURN_VALUE],
                             states=[Step.RUN, Step.FAILURE, Step.PAUSED, Step.PAUSED_ON_FAIL])
    delay           -- The delay in seconds between consecutive status calls. Do not set very short delays as that can
                       overwhelm the trail, unless you are sure of what you are doing.
    max_retries     -- The number of times to re-try if no status is received. This is useful to bring the monitoring
                       to an end when the trail has been shutdown.

    Side-effects:
    1. The trail_client.status() is called every delay seconds.
    2. If the status has changed, then the filter and handler pairs will be called, but only with the changed statuses.
    """
    if status_kwargs is None:
        status_kwargs = dict(fields=[StatusField.STATE, StatusField.UNREPLIED_PROMPT_MESSAGE,
                                     StatusField.OUTPUT_MESSAGES, StatusField.RETURN_VALUE],
                             states=[Step.RUN, Step.FAILURE, Step.PAUSED, Step.PAUSED_ON_FAIL])

    return monitor(
        function=lambda: trail_client.status(**status_kwargs),
        # Stop monitoring if the last max_tries statuses including the current one have been None.
        result_predicate=lambda history: any(list(history)[-max_tries:]),
        pre_processor=lambda history: diff_lists(list(history)[-2:]),
        filter_handler_pairs=status_filter_handler_pairs,
        delay=delay,
        history_limit=max_tries + 1)


def search_for_any_element(iterable_a, iterable_b):
    """Searches for any element from iterable_a in iterable_b. Returns the first element it finds.

    Returns:
    The first element from iterable_a that was found in iterable_b. The function early exits once an element is found.
    None if no element in iterable_a is present in iterable_b.
    """
    b = set(iterable_b)
    for a in iterable_a:
        if a in b:
            return a


# This is an ordered list, so do not reorder without considering the consequences.
# If a state in this ordered list is encountered, the groups' state will be reported as that state.
# So, if *any* step in a group has failed, then the summary state of the group will show as FAILED.
# States further down the list will not be reached if a state higher up the list is found.
# Please read the documentation of determine_group_state function to see how this data structure is used.
GROUP_STATE_ORDER = [
    Step.FAILURE,
    Step.PAUSED_ON_FAIL,
    Step.INTERRUPTED,
    Step.BLOCKED,
    Step.PAUSED,
    Step.RUN,
    Step.TOBLOCK,
    Step.TOSKIP,
    Step.TOPAUSE,
    Step.WAIT,
    Step.READY,
    Step.SUCCESS,
    Step.SKIPPED,
]


def determine_group_state(states, group_state_order=GROUP_STATE_ORDER):
    """Determine a single state for the given states, which is representative of the group.

    Arguments:
    states             -- An iterable of step states (strings).
    group_state_order  -- An iterable of step states which, when encountered in the given states, will be considered
                          to be the group state.
                          This list is order sensitive. When being iterated over, if a step state is present in the
                          given states, then that step state will be representative of the group.
                          Because it is order sensitive, once a state is encountered, the function returns and other
                          states are not searched.

    Returns:
    String             -- Representing the status for the given statuses.
    """
    group_status = search_for_any_element(group_state_order, states)
    if not group_status:
        raise ValueError(('Group status could not be determined as none of the states in group_state_order={} exist '
                          'in the given states={}').format(str(group_state_order), str(states)))
    return group_status


def filter_status_by_states(statuses, states):
    """Get the status of steps that are in the given states.

    Arguments:
    statuses -- A list of dictionaries, each dictionary should be the status of a step with the StatusField.STATE key
                present.
    states   -- A list of states.

    Returns:
    A list of dictionaries, containing only the status of the steps that are in the given states.
    """
    return [status for status in statuses if status[StatusField.STATE] in states]
