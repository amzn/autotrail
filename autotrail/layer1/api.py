"""Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.
"""

import logging

from collections import namedtuple

from autotrail.core.dag import Step, topological_traverse
from autotrail.core.socket_communication import HandlerResult


class APICallName:
    """Namepsace for the API call names. Use this class instead of plain strings."""
    # List of valid API call names
    # These values don't carry any specific meaning but they should be unique, i.e., no two API calls should have the
    # same value.
    START = 'start'
    SHUTDOWN = 'shutdown'
    STATUS = 'status'
    LIST = 'list'
    PAUSE = 'pause'
    INTERRUPT = 'interrupt'
    NEXT_STEPS = 'next_steps'
    RESUME = 'resume'
    SKIP = 'skip'
    UNSKIP = 'unskip'
    BLOCK = 'block'
    UNBLOCK = 'unblock'
    PAUSE_BRANCH = 'pause_branch'
    RESUME_BRANCH = 'resume_branch'
    SET_PAUSE_ON_FAIL = 'set_pause_on_fail'
    UNSET_PAUSE_ON_FAIL = 'unset_pause_on_fail'
    SEND_MESSAGE_TO_STEPS = 'send_message_to_steps'


class APICallField:
    """Namespace for the various fields (keys) in an API call. Use this class instead of plain strings."""
    NAME = 'name'
    TAGS = 'tags'
    STEP_COUNT = 'step_count'
    DRY_RUN = 'dry_run'
    MESSAGE = 'message'
    STATES = 'states'
    STATUS_FIELDS = 'status_fields'


class StatusField(object):
    """Namespace for the fields returned as part of the status API call. Use this class instead of plain strings."""
    N = 'n'
    NAME = 'Name'
    STATE = 'State'
    RETURN_VALUE = 'Return value'
    OUTPUT_MESSAGES = 'Output messages'
    INPUT_MESSAGES = 'Input messages'
    PROMPT_MESSAGES = 'Prompt messages'
    UNREPLIED_PROMPT_MESSAGE = 'Waiting for response'


def get_class_globals(klass):
    """Returns a set of the globals defined in the given class.
    Globals are identified to be uppercase and do not start with an underscore.

    Arguments:
    klass -- The class whose globals need to be fetched.

    Returns:
    set -- of the class globals.
    """
    return {getattr(klass, attribute) for attribute in dir(klass) if not attribute.startswith('_') and attribute.isupper()}


def validate_states(states):
    """Validate the given states.

    Arguments:
    states          -- An iterable containing step states.

    Returns:
    None            -- If all the states are valid.

    Raises:
    ValidationError -- If any of the states are invalid. Exception contains details about the state that is invalid.
    """
    try:
        valid_states = get_class_globals(Step)
        for state in states:
            if state not in valid_states:
                raise ValueError('The state "{}" is not valid.'.format(str(state)))
    except TypeError:
        raise TypeError('{} should be a list or iterable.'.format(APICallField.STATES))


def validate_status_fields(status_fields):
    """Validate the given status fields.

    Arguments:
    states          -- An iterable containing status fields (fields to be included in a status API response).

    Returns:
    None            -- If all the status fields are valid.

    Raises:
    ValidationError -- If any of the status fields are invalid. Exception contains details about the status field that
                       is invalid.
    """
    try:
        valid_fields = get_class_globals(StatusField)
        for field in status_fields:
            if field not in valid_fields:
                raise ValueError('The field "{}" is not valid.'.format(str(field)))
    except TypeError:
        raise TypeError('{} should be a list or iterable.'.format(str(status_fields)))


def validate_mapping(obj):
    """Validate if the given obj behaves like a dictionary by allowing look-ups and iteration.

    Arguments:
    obj             -- Any object being validated

    Returns:
    None            -- If the validation is successful.

    Raises:
    ValidationError -- If the object doesn't allow look-ups or iteration like a dictionary.
    """
    try:
        for key in obj:
            obj[key]
    except Exception:
        raise TypeError('{} needs to be a dictionary like mapping.'.format(str(obj)))


def validate_number(obj):
    """Validate the given obj as a number.

    Arguments:
    obj             -- The object to be validated.

    Returns:
    None            -- If the given object behaves like a number as used by the API calls.

    Raises:
    ValidationError -- If the object does not exhibit numerical behaviour required for the API calls.
    """
    try:
        l = [0, 1, 2]
        l[:obj]
    except TypeError:
        raise TypeError('{} needs to be a number.'.format(str(obj)))


def validate_boolean(obj):
    """Validate the obj as a boolean.

    Arguments:
    obj             -- The object to be validated.

    Returns:
    None            -- If the object is a boolean (True or False)

    Raises:
    ValidationError -- If the object is neither True nor False.
    """
    if obj is not True and obj is not False:
        raise ValueError('{} should be a boolean True or False.'.format(str(obj)))


def search_steps_with_tags(steps, tags):
    """Search steps for which the given tags match.

    Returns:
    A generator of Steps -- that have the provided tags.
    """
    if tags == {}:
        return steps
    return (step for step in steps if is_dict_subset_of(tags, step.tags))


def search_steps_with_states(steps, states):
    """Search for steps that are in the given states.

    Arguments:
    steps  -- An iterator over Step like objects.
    states -- A list of states allowed by the Step like objects e.g., Step.WAIT.

    Returns:
    A generator object that will yield step objects whose state is in the given list of states.
    """
    if states == []:
        return steps
    return (step for step in steps if step.state in states)


def get_progeny(steps):
    """Returns the strict progeny of all the vertices provided.

    Strict progeny means that each vertex returned is not a child of any other vertices.

    Arguments:
    steps  -- An iterator over Step like objects.

    Returns:
    A set generator of Steps -- of Each of these steps are found in the branches originating from the given steps.
    """
    return {progeny for step in steps for progeny in topological_traverse(step)}


def step_to_stepstatus(step, status_fields):
    """Create a status dictionary from a Step like object. This dictionary will include keys from the status_fields and
    their corresponding values from the step. Some keys will always be present.

    Arguments:
    step          -- A Step like object.
    status_fields -- A list of fields to be included in the status dictionaries. These are defined in the StatusField
                     namespace.

    Returns:
    dictionary -- Of the form:
    {
        # The following two key-value pairs will always be present in this dictionary, because without these it
        # will be impossible to uniquely identify the steps.
        StatusField.N: <Sequence number of the step>,
        StatusField.NAME: <Name of the step>,

        # The rest of the key-value pairs are obtained based on the given status_fields
        <Key from status_fields>: <Corresponding value from the step>,
    }

    Example:
    If the status_fields specified are:
        [StatusField.STATE, StatusField.RETURN_VALUE]
    Then the returned dictionary will be of the form:
        {
            StatusField.N: <Sequence number of the step>,
            StatusField.NAME: <Name of the step>,
            StatusField.STATE: <State of the step>,
            StatusField.RETURN_VALUE: <Return value from the step>,
        }
    """
    step_status = {
        StatusField.N: step.tags['n'],
        StatusField.NAME: str(step),
    }
    if StatusField.STATE in status_fields:
        step_status[StatusField.STATE] = step.state
    if StatusField.RETURN_VALUE in status_fields:
        step_status[StatusField.RETURN_VALUE] = step.return_value
    if StatusField.OUTPUT_MESSAGES in status_fields:
        step_status[StatusField.OUTPUT_MESSAGES] = step.output_messages
    if StatusField.PROMPT_MESSAGES in status_fields:
        step_status[StatusField.PROMPT_MESSAGES] = step.prompt_messages
    if StatusField.UNREPLIED_PROMPT_MESSAGE in status_fields:
        step_status[StatusField.UNREPLIED_PROMPT_MESSAGE] = get_unreplied_prompt_message(step)

    return step_status


def extract_essential_tags(steps):
    """Extract the 'n' and 'name' tags from the steps. These are the tags that can uniquely identify the step.

    Arguments:
    steps   -- An iterator over Step like objects.

    Returns:
    A generator that yields dictionaries each containing only the 'n' and 'name' tags.
    """
    return ({'n': step.tags['n'], 'name': step.tags['name']} for step in steps)


def get_unreplied_prompt_message(step):
    """Get the unreplied message sent by the action function of a Step.

    This function will return the unreplied prompt message sent by the action function of a Step so far.

    Basically the number of prompt messages sent by an action function is tallied against any messages sent to it by
    the user.

    Arguments:
    step      -- A Step like object.

    Returns:
    The message that hasn't been replied to yet. (The type depends on the action function.)
    """
    return step.prompt_messages[-1] if (len(step.prompt_messages) != len(step.input_messages)) else None


def change_attribute(steps, attribute, value):
    """Change the attribute of the given steps to the value provided.

    Arguments:
    steps      -- A iterable of Step like objects.
    attribute  -- An attribute of the objects that needs to be changed.
    value      -- The value that the attribute needs to be changed to.

    Post-condition:
    The attribute of all the steps will be updated to the given value.
    """
    for step in steps:
        setattr(step, attribute, value)
        log_step(logging.debug, step, 'Changing attribute {} to {}'.format(attribute, str(value)))


def interrupt(steps):
    """Interrupt the action functions of the steps by calling their process' terminate method.

    Arguments:
    steps -- A iterable of Step like objects.

    Post-condition:
    Each step's process.terminate() will be called, interrupting the run of their action functions.
    """
    for step in steps:
        log_step(logging.debug, step, 'Interruping the step.')
        step.process.terminate()


def get_status(steps, status_fields):
    """Get the status of every step by including the fields specified in status_fields.

    Arguments:
    steps         -- An iterator over Step like objects.
    status_fields -- A list of fields to be included in the status dictionaries.

    Returns:
    A list of dictionaries. Each dictionary represents the status of a step. See the documentation of the
    step_to_stepstatus function to know how this dictionary is structured.
    """
    return [step_to_stepstatus(step, status_fields) for step in steps]


def send_message_to_steps(steps, message):
    """Send the message to the input_queue of the steps.

    Arguments:
    steps            -- An iterator over Step like objects.
    message          -- This should be any picklable Python object that can be put into a multiprocessing.Queue.

    Post-condition:
    1. The the message will be put into the input_queue of each step.
    2. The message will also be added to the input_messages list.
    """
    for step in steps:
        log_step(logging.debug, step, 'Sent message to step. Message: {}'.format(str(message)))
        step.input_queue.put(message)
        step.input_messages.append(message)


# This datastructure declaratively defines how an API call is handled.
# The actual execution of the business logic is handled by functions (handle_api_call).
# Each APICallDefinition consists of the following attributes:
# validators    : This is a dictionary that maps the fields that are required for this API call mapped to their
#                 validation function. Each validation function should:
#                 1. Accept the value of the corresponding field from the api_call dictionary.
#                 2. If validation is successful, return (doesn't matter what is returned).
#                 3. If validation fails, then raise an Exception.
#                 For example, consider the validators dictionary below:
#                 {
#                     APICallField.TAGS: validate_mapping,
#                 }
#                 The key represented by APICallField.TAGS will be extracted from the api_call dictionary and
#                 validate_mapping will be called with it as:
#                     validate_mapping(api_call.get(APICallField.TAGS))
# steps         : This is a function that will be used to extract the steps needed for the API call. Typically,
#                 API calls work on steps that match specific tags or that are in specific states, the function here
#                 is used to extract that list of steps.
#                 This function should:
#                 1. Accept 2 parameters - steps (list) and the api_call dictionary.
#                 2. Return either a list or iterator over steps.
#                 For example, consider the steps function below:
#                     lambda steps, api_call: search_steps_with_tags(steps, api_call[APICallField.TAGS])
#                 The above will return all steps that match the tags specified in the api_call dictionary.
# predicate     : This is a guard that is used to decide if the handlers should be called or not.
#                 This relieves the handlers of the responsibility to check various conditions.
#                 This predicate should:
#                 1. Accept 2 parameters - steps (list) and the api_call dictionary.
#                 2. Return either a True or False value (values like [] or '' are valid as they can be intrpreted as
#                    False.
#                 If the predicate is True, then the handlers will be called.
#                 For example, consider the following predicate function:
#                     lambda steps, api_call: not api_call[APICallField.DRY_RUN]
#                 The above predicate will ensure that the handlers won't be called unless dry run is False in the
#                 api_call dictionary.
# handlers      : This is a list of functions that take the action required by the API call. Since there are multiple
#                 handlers, the return value of the first is passed to the second and so on, which means, that the
#                 first handler accepts one less parameter compared to the rest.
#                 The first hander should:
#                 1. Accept 2 parameters - steps (list) and the api_call dictionary.
#                 All the other handers should:
#                 2. Accept 3 parameters - steps (list), the api_call dictionary and the return value of the previous
#                    handler.
#                 The return values between the handlers can be anything and is of no consequence to the business logic
#                 so long as they work seamlessly between the handlers and the post_processor.
#                 For example, consider the following handlers:
#                   [
#                       lambda steps, api_call: interrupt(steps),
#                       lambda steps, api_call, return_value: change_attribute(
#                           steps, attribute='state', value=Step.INTERRUPTED)
#                   ]
#                 The above handlers first invoke interrupt with the steps.
#                 Then, change_attribute is invoked for the same list of steps.
#                 The return_value is not used.
# post_processor: This is a function that is responsible for returning the final result of the API call in the form of
#                 a 3-tuple.
#                 This function should:
#                 1. Accept 3 parameters - steps (list), the api_call dictionary and the return value of the last run
#                    handler.
#                 3. Return a 3-tuple of the form: (<api result>, <error>, <flag for trail continuation>)
#                    <api result> -- the result of the API call.
#                    <error>      -- Any error encountered that needs to be sent back to the user.
#                    <flag>       -- This is a boolean to indicate if the trail should continue running.
#                                    True means that the trail can continue running normally and False indicates
#                                    that the trail should be shutdown.
APICallDefinition = namedtuple('APICallDefinition',
                               ['validators', 'steps', 'predicate', 'handlers', 'post_processor'])


API_CALL_DEFINITIONS = {
    APICallName.START: APICallDefinition(
        validators={APICallField.DRY_RUN: validate_boolean},
        steps=lambda steps, api_call: search_steps_with_states(steps, [Step.READY]),
        predicate=lambda steps, api_call: not api_call[APICallField.DRY_RUN],
        handlers=[lambda steps, api_call: change_attribute(steps, attribute='state', value=Step.WAIT)],
        post_processor=lambda steps, api_call, return_value: (list(extract_essential_tags(steps)), None, True),
    ),
    APICallName.SHUTDOWN: APICallDefinition(
        validators={APICallField.DRY_RUN: validate_boolean},
        steps=None,
        predicate=lambda steps, api_call: False,
        handlers=None,
        post_processor=lambda steps, api_call, return_value: (
            not api_call[APICallField.DRY_RUN], None, api_call[APICallField.DRY_RUN]),
    ),
    APICallName.LIST: APICallDefinition(
        validators={APICallField.TAGS: validate_mapping},
        steps=lambda steps, api_call: search_steps_with_tags(steps, api_call[APICallField.TAGS]),
        predicate=lambda steps, api_call: False,
        handlers=None,
        post_processor=lambda steps, api_call, return_value: ([step.tags for step in steps], None, True),
    ),
    APICallName.STATUS: APICallDefinition(
        validators={
            APICallField.TAGS: validate_mapping,
            APICallField.STATUS_FIELDS: validate_status_fields,
            APICallField.STATES: validate_states},
        steps=lambda steps, api_call: search_steps_with_states(
            search_steps_with_tags(steps, api_call[APICallField.TAGS]),
            api_call[APICallField.STATES]),
        predicate=lambda steps, api_call: True,
        handlers=[lambda steps, api_call: get_status(steps, api_call[APICallField.STATUS_FIELDS])],
        post_processor=lambda steps, api_call, return_value: (return_value, None, True),
    ),
    APICallName.PAUSE: APICallDefinition(
        validators={
            APICallField.TAGS: validate_mapping,
            APICallField.DRY_RUN: validate_boolean},
        steps=lambda steps, api_call:search_steps_with_states(
            search_steps_with_tags(steps, api_call[APICallField.TAGS]),
            [Step.READY, Step.WAIT]),
        predicate=lambda steps, api_call: not api_call[APICallField.DRY_RUN],
        handlers=[lambda steps, api_call: change_attribute(steps, attribute='state', value=Step.TOPAUSE)],
        post_processor=lambda steps, api_call, return_value: (list(extract_essential_tags(steps)), None, True),
    ),
    APICallName.INTERRUPT: APICallDefinition(
        validators={
            APICallField.TAGS: validate_mapping,
            APICallField.DRY_RUN: validate_boolean},
        steps=lambda steps, api_call: search_steps_with_states(
            search_steps_with_tags(steps, api_call[APICallField.TAGS]),
            [Step.RUN]),
        predicate=lambda steps, api_call: not api_call[APICallField.DRY_RUN],
        handlers=[
            lambda steps, api_call: interrupt(steps),
            lambda steps, api_call, return_value: change_attribute(steps, attribute='state', value=Step.INTERRUPTED)],
        post_processor=lambda steps, api_call, return_value: (list(extract_essential_tags(steps)), None, True),
    ),
    APICallName.RESUME: APICallDefinition(
        validators={
            APICallField.TAGS: validate_mapping,
            APICallField.DRY_RUN: validate_boolean},
        steps=lambda steps, api_call: search_steps_with_states(
            search_steps_with_tags(steps, api_call[APICallField.TAGS]),
            [Step.TOPAUSE, Step.PAUSED, Step.PAUSED_ON_FAIL, Step.INTERRUPTED]),
        predicate=lambda steps, api_call: not api_call[APICallField.DRY_RUN],
        handlers=[lambda steps, api_call: change_attribute(steps, attribute='state', value=Step.WAIT)],
        post_processor=lambda steps, api_call, return_value: (list(extract_essential_tags(steps)), None, True),
    ),
    APICallName.SKIP: APICallDefinition(
        validators={
            APICallField.TAGS: validate_mapping,
            APICallField.DRY_RUN: validate_boolean},
        steps=lambda steps, api_call: search_steps_with_states(
            search_steps_with_tags(steps, api_call[APICallField.TAGS]),
            [Step.READY, Step.WAIT, Step.TOPAUSE, Step.PAUSED, Step.PAUSED_ON_FAIL, Step.INTERRUPTED]),
        predicate=lambda steps, api_call: not api_call[APICallField.DRY_RUN],
        handlers=[lambda steps, api_call: change_attribute(steps, attribute='state', value=Step.TOSKIP)],
        post_processor=lambda steps, api_call, return_value: (list(extract_essential_tags(steps)), None, True),
    ),
    APICallName.UNSKIP: APICallDefinition(
        validators={
            APICallField.TAGS: validate_mapping,
            APICallField.DRY_RUN: validate_boolean},
        steps=lambda steps, api_call: search_steps_with_states(
            search_steps_with_tags(steps, api_call[APICallField.TAGS]),
            [Step.TOSKIP]),
        predicate=lambda steps, api_call: not api_call[APICallField.DRY_RUN],
        handlers=[lambda steps, api_call: change_attribute(steps, attribute='state', value=Step.WAIT)],
        post_processor=lambda steps, api_call, return_value: (list(extract_essential_tags(steps)), None, True),
    ),
    APICallName.BLOCK: APICallDefinition(
        validators={
            APICallField.TAGS: validate_mapping,
            APICallField.DRY_RUN: validate_boolean},
        steps=lambda steps, api_call: search_steps_with_states(
            search_steps_with_tags(steps, api_call[APICallField.TAGS]),
            [Step.READY, Step.WAIT, Step.TOPAUSE, Step.PAUSED, Step.PAUSED_ON_FAIL]),
        predicate=lambda steps, api_call: not api_call[APICallField.DRY_RUN],
        handlers=[lambda steps, api_call: change_attribute(steps, attribute='state', value=Step.TOBLOCK)],
        post_processor=lambda steps, api_call, return_value: (list(extract_essential_tags(steps)), None, True),
    ),
    APICallName.UNBLOCK: APICallDefinition(
        validators={
            APICallField.TAGS: validate_mapping,
            APICallField.DRY_RUN: validate_boolean},
        steps=lambda steps, api_call: search_steps_with_states(
            search_steps_with_tags(steps, api_call[APICallField.TAGS]),
            [Step.TOBLOCK]),
        predicate=lambda steps, api_call: not api_call[APICallField.DRY_RUN],
        handlers=[lambda steps, api_call: change_attribute(steps, attribute='state', value=Step.WAIT)],
        post_processor=lambda steps, api_call, return_value: (list(extract_essential_tags(steps)), None, True),
    ),
    APICallName.SET_PAUSE_ON_FAIL: APICallDefinition(
        validators={
            APICallField.TAGS: validate_mapping,
            APICallField.DRY_RUN: validate_boolean},
        steps=lambda steps, api_call: search_steps_with_states(
            search_steps_with_tags(steps, api_call[APICallField.TAGS]),
            [Step.READY, Step.WAIT, Step.INTERRUPTED, Step.TOPAUSE, Step.PAUSED]),
        predicate=lambda steps, api_call: not api_call[APICallField.DRY_RUN],
        handlers=[lambda steps, api_call: change_attribute(steps, attribute='pause_on_fail', value=True)],
        post_processor=lambda steps, api_call, return_value: (list(extract_essential_tags(steps)), None, True),
    ),
    APICallName.UNSET_PAUSE_ON_FAIL: APICallDefinition(
        validators={
            APICallField.TAGS: validate_mapping,
            APICallField.DRY_RUN: validate_boolean},
        steps=lambda steps, api_call: search_steps_with_states(
            search_steps_with_tags(steps, api_call[APICallField.TAGS]),
            [Step.READY, Step.WAIT, Step.INTERRUPTED, Step.TOPAUSE, Step.PAUSED]),
        predicate=lambda steps, api_call: not api_call[APICallField.DRY_RUN],
        handlers=[lambda steps, api_call: change_attribute(steps, attribute='pause_on_fail', value=False)],
        post_processor=lambda steps, api_call, return_value: (list(extract_essential_tags(steps)), None, True),
    ),
    APICallName.PAUSE_BRANCH: APICallDefinition(
        validators={
            APICallField.TAGS: validate_mapping,
            APICallField.DRY_RUN: validate_boolean},
        steps=lambda steps, api_call: search_steps_with_states(
            get_progeny(search_steps_with_tags(steps, api_call[APICallField.TAGS])),
            [Step.READY, Step.WAIT]),
        predicate=lambda steps, api_call: not api_call[APICallField.DRY_RUN],
        handlers=[lambda steps, api_call: change_attribute(steps, attribute='state', value=Step.TOPAUSE)],
        post_processor=lambda steps, api_call, return_value: (list(extract_essential_tags(steps)), None, True),
    ),
    APICallName.RESUME_BRANCH: APICallDefinition(
        validators={
            APICallField.TAGS: validate_mapping,
            APICallField.DRY_RUN: validate_boolean},
        steps=lambda steps, api_call: search_steps_with_states(
            get_progeny(search_steps_with_tags(steps, api_call[APICallField.TAGS])),
            [Step.TOPAUSE, Step.PAUSED, Step.PAUSED_ON_FAIL]),
        predicate=lambda steps, api_call: not api_call[APICallField.DRY_RUN],
        handlers=[lambda steps, api_call: change_attribute(steps, attribute='state', value=Step.WAIT)],
        post_processor=lambda steps, api_call, return_value: (list(extract_essential_tags(steps)), None, True),
    ),
    APICallName.NEXT_STEPS: APICallDefinition(
        validators={
            APICallField.STEP_COUNT: validate_number,
            APICallField.DRY_RUN: validate_boolean},
        steps=lambda steps, api_call: search_steps_with_states(
            steps[:api_call[APICallField.STEP_COUNT]],
            [Step.TOPAUSE, Step.PAUSED, Step.PAUSED_ON_FAIL]),
        predicate=lambda steps, api_call: not api_call[APICallField.DRY_RUN],
        handlers=[lambda steps, api_call: change_attribute(steps, attribute='state', value=Step.WAIT)],
        post_processor=lambda steps, api_call, return_value: (list(extract_essential_tags(steps)), None, True),
    ),
    APICallName.SEND_MESSAGE_TO_STEPS: APICallDefinition(
        validators={
            APICallField.MESSAGE: lambda api_call: None,
            APICallField.TAGS: validate_mapping,
            APICallField.DRY_RUN: validate_boolean},
        steps=lambda steps, api_call: search_steps_with_states(
            search_steps_with_tags(steps, api_call[APICallField.TAGS]),
            [Step.RUN]),
        predicate=lambda steps, api_call: not api_call[APICallField.DRY_RUN],
        handlers=[lambda steps, api_call: send_message_to_steps(steps, api_call[APICallField.MESSAGE])],
        post_processor=lambda steps, api_call, return_value: (list(extract_essential_tags(steps)), None, True),
    ),
}


def validate_api_call(api_call, api_call_definition):
    """Validate the api_call based on the provided api_call_definition.

    Arguments:
    api_call            -- A dictionary of the API call request.
    api_call_definition -- A APICallDefinition or similar data structure that has an attribute called 'validators',
                           which is a dictionary of the parameter mapped to the validation function.

    Returns:
    None                -- If the validation was successful.
    String              -- containing the error message if the api_call was invalid.
    """
    if api_call_definition is None:
        return 'API name {} is invalid.'.format(api_call.get(APICallField.NAME))

    for field, validator in api_call_definition.validators.iteritems():
        try:
            value = api_call.get(field)
            validator(value)
        except Exception as e:
            return ('API Call validation failed: The parameter {field} has an invalid value: {value}. Error: {error}'
                    '').format(field=field, value=value, error=str(e))


def handle_api_call(api_call, steps, api_call_definitions=API_CALL_DEFINITIONS):
    """Handler for API calls.
    This function is compliant to be used as a handler with the autotrail.core.socket_communication.serve_socket
    function.

    This function handles a single API call request by using the api_call_definitions, which provides the data for the
    execution of the business logic. See the documenation of APICallDefinition to know how the datastructure is used.

    Arguments:
    api_call             -- A dictionary of the API call request.
    steps                -- A topologically ordered iterable over steps.

    Keyword Arguments:
    api_call_definitions -- A dictionary that maps API call names to their API Call definitions.
                            The API call definition is looked up in this dictionary to control the business logic of
                            how this API call is handled.
                            See the documenation of APICallDefinition to understand how it is structured.

    Returns:
    HandlerResult        -- Containing the response to be sent to the user (result and error) and the return_value
                            that will be passed as-is to the caller of serve_socket.
    """
    api_call_definition = api_call_definitions.get(api_call.get(APICallField.NAME))

    validation_result = validate_api_call(api_call, api_call_definition)
    if validation_result:
        return HandlerResult(response=dict(name=api_call.get(APICallField.NAME), result=None, error=validation_result),
                             return_value=True)

    steps = list(api_call_definition.steps(steps, api_call)) if api_call_definition.steps else []
    handler_return_value = None
    if api_call_definition.predicate(steps, api_call):
        first_handler = api_call_definition.handlers[0]
        handlers = api_call_definition.handlers[1:]
        handler_return_value = first_handler(steps, api_call)
        for handler in handlers:
            handler_return_value = handler(steps, api_call, handler_return_value)

    result, error, return_value = api_call_definition.post_processor(steps, api_call, handler_return_value)

    logging.info(('Received API Request: {api_call} -- Sending response: result={result}, error={error}, '
                  'return_value={return_value}').format(api_call=str(api_call), result=str(result), error=str(error),
                                                        return_value=return_value))

    return HandlerResult(response=dict(name=api_call[APICallField.NAME], result=result, error=error),
                         return_value=return_value)


def log_step(log_function, step, message):
    """Write a log using the provided log_function by providing information about the step.

    Arguments:
    log_function -- The logging function to use. Eg., logging.info, logging.debug etc.
    step         -- A Step or Step like object with at least 'name' and 'n' tags defined.
    message      -- String - The message to log.

    Usage:
    Assume that example_step has the tags: {n=0, name='example_step'}, then the code:
        log_step(logging.info, example_step, 'Example log message')
    Will produce the following log message:
        [Step Name=example_step, n=0] Example log message
    """
    log_message = '[Step Name={name}, n={n}] {message}'.format(
        name=step.tags['name'], n=step.tags['n'], message=message)
    log_function(log_message)


def is_dict_subset_of(sub_dict, super_dict):
    """Checks if sub_dict is contained in the super_dict.

    Arguments:
    sub_dict   -- A dictionary like mapping that supports iteritems method.
    super_dict -- A dictionary like mapping that supports iteritems method.

    Returns:
    True  -- If all the key-value pairs of sub_dict are present in the super_dict.
    False -- Otherwise.
    """
    sub_set = set(sub_dict.iteritems())
    super_set = set(super_dict.iteritems())
    return sub_set.issubset(super_set)
