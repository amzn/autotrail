"""Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.
"""

import logging
import mock
import unittest

from mock import MagicMock, patch

from six.moves.queue import Empty as QueueEmpty

from autotrail.core.dag import Step
from autotrail.core.socket_communication import HandlerResult
from autotrail.layer1.api import (
    APICallDefinition,
    API_CALL_DEFINITIONS,
    APICallField,
    APICallName,
    change_attribute,
    extract_essential_tags,
    get_class_globals,
    get_progeny,
    get_status,
    get_unreplied_prompt_message,
    handle_api_call,
    interrupt,
    is_dict_subset_of,
    log_step,
    search_steps_with_states,
    search_steps_with_tags,
    send_message_to_steps,
    StatusField,
    step_to_stepstatus,
    validate_api_call,
    validate_boolean,
    validate_mapping,
    validate_number,
    validate_states,
    validate_status_fields)


class TestHelpers(unittest.TestCase):
    def setUp(self):
        # Create a simple trail (root_step)
        #           +--> step_b (group=1)
        # step_a -->|--> step_c
        #           +--> step_d (group=1)
        def action_function_a():
            pass

        def action_function_b():
            pass

        def action_function_c():
            pass

        def action_function_d():
            pass

        self.step_a = Step(action_function_a)
        self.step_b = Step(action_function_b, group=1)
        self.step_c = Step(action_function_c)
        self.step_d = Step(action_function_d, group=1)

    def test_get_class_globals(self):
        api_call_field_globals = get_class_globals(APICallField)
        self.assertEqual(api_call_field_globals, set([APICallField.NAME, APICallField.TAGS, APICallField.STEP_COUNT,
                                                      APICallField.DRY_RUN, APICallField.MESSAGE, APICallField.STATES,
                                                      APICallField.STATUS_FIELDS]))

        api_call_name_globals = get_class_globals(APICallName)
        self.assertEqual(api_call_name_globals, set([APICallName.START, APICallName.SHUTDOWN, APICallName.STATUS,
                                                     APICallName.LIST, APICallName.PAUSE, APICallName.INTERRUPT,
                                                     APICallName.NEXT_STEPS, APICallName.RESUME,
                                                     APICallName.SKIP, APICallName.UNSKIP, APICallName.BLOCK,
                                                     APICallName.UNBLOCK, APICallName.PAUSE_BRANCH,
                                                     APICallName.RESUME_BRANCH,
                                                     APICallName.SET_PAUSE_ON_FAIL, APICallName.UNSET_PAUSE_ON_FAIL,
                                                     APICallName.SEND_MESSAGE_TO_STEPS]))

    def test_is_dict_subset_of(self):
        check_tags_1 = {
            'c': 'C',
            'f': 'F',
        }
        check_tags_2 = {
            'x': 'X',
        }
        all_tags = {
            'a': 'A',
            'b': 'B',
            'c': 'C',
            'd': 'D',
            'e': 'E',
            'f': 'F',
        }
        self.assertTrue(is_dict_subset_of(check_tags_1, all_tags))
        self.assertFalse(is_dict_subset_of(check_tags_2, all_tags))

    def test_search_steps_with_tags(self):
        is_dict_subset_of_patcher = patch('autotrail.layer1.api.is_dict_subset_of',
                                          side_effect=[False, True, False, True])
        mock_is_dict_subset_of = is_dict_subset_of_patcher.start()

        dag = [self.step_a, self.step_b, self.step_c, self.step_d]
        steps = search_steps_with_tags(dag, {'group': 1})

        self.assertEqual(list(steps), [self.step_b, self.step_d])
        self.assertEqual(mock_is_dict_subset_of.mock_calls, [
            mock.call({'group': 1}, {'name': 'action_function_a'}),
            mock.call({'group': 1}, {'group': 1, 'name': 'action_function_b'}),
            mock.call({'group': 1}, {'name': 'action_function_c'}),
            mock.call({'group': 1}, {'group': 1, 'name': 'action_function_d'})])

        is_dict_subset_of_patcher.stop()

    def test_search_steps_with_tags_where_tags_is_empty(self):
        is_dict_subset_of_patcher = patch('autotrail.layer1.api.is_dict_subset_of', return_value=True)
        mock_is_dict_subset_of = is_dict_subset_of_patcher.start()

        dag = [self.step_a, self.step_b, self.step_c, self.step_d]
        steps = search_steps_with_tags(dag, {})

        self.assertEqual(list(steps), dag)
        self.assertEqual(mock_is_dict_subset_of.call_count, 0)

        is_dict_subset_of_patcher.stop()

    def test_get_progeny(self):
        topological_traverse_patcher = patch('autotrail.layer1.api.topological_traverse',
                                             return_value=['mock_step_1', 'mock_step_2', 'mock_step_3',
                                                           'mock_step_2', 'mock_step_1'])
        mock_topological_traverse = topological_traverse_patcher.start()
        steps = ['mock_step_1', 'mock_step_2', 'mock_step_3']

        progeny = list(get_progeny(steps))

        self.assertIn('mock_step_1', progeny)
        self.assertIn('mock_step_2', progeny)
        self.assertIn('mock_step_3', progeny)
        self.assertEqual(mock_topological_traverse.mock_calls, [mock.call('mock_step_1'), mock.call('mock_step_2'),
                                                                mock.call('mock_step_3')])

        topological_traverse_patcher.stop()

    def test_search_steps_with_states(self):
        step_running = Step(lambda x: x)
        step_running.state = Step.RUN

        step_paused = Step(lambda x: x)
        step_paused.state = Step.PAUSED

        steps = [step_running, step_paused]

        filtered_steps = search_steps_with_states(steps, [Step.PAUSED])

        self.assertEqual(list(filtered_steps), [step_paused])

    def test_search_steps_with_states_when_states_is_empty(self):
        steps = MagicMock()

        filtered_steps = search_steps_with_states(steps, [])

        self.assertEqual(filtered_steps, steps)
        self.assertEqual(steps.call_count, 0)

    def test_step_to_stepstatus_empty_fields(self):
        step = Step(lambda x: x, n=0)
        step_status = step_to_stepstatus(step, [])

        self.assertEqual(step_status, {
                StatusField.N: 0,
                StatusField.NAME: str(step)})

    def test_step_to_stepstatus_all_fields(self):
        step = Step(lambda x: x, n=0)
        step_status = step_to_stepstatus(step, [StatusField.STATE, StatusField.RETURN_VALUE, StatusField.TAGS,
                                                StatusField.OUTPUT_MESSAGES, StatusField.PROMPT_MESSAGES,
                                                StatusField.UNREPLIED_PROMPT_MESSAGE])

        self.assertEqual(step_status, { StatusField.N: 0,
                                        StatusField.NAME: str(step),
                                        StatusField.TAGS: step.tags,
                                        StatusField.STATE: Step.READY,
                                        StatusField.RETURN_VALUE: None,
                                        StatusField.OUTPUT_MESSAGES: [],
                                        StatusField.PROMPT_MESSAGES: [],
                                        StatusField.UNREPLIED_PROMPT_MESSAGE: None})

    def test_step_to_stepstatus_with_non_json_serializable_return_value(self):
        step = Step(lambda x: x, n=0)
        mock_exception = TypeError('is not JSON serializable')
        step.return_value = mock_exception

        step_status = step_to_stepstatus(step, [StatusField.STATE, StatusField.RETURN_VALUE, StatusField.TAGS,
                                                StatusField.OUTPUT_MESSAGES, StatusField.PROMPT_MESSAGES,
                                                StatusField.UNREPLIED_PROMPT_MESSAGE])

        self.assertEqual(step_status, { StatusField.N: 0,
                                        StatusField.NAME: str(step),
                                        StatusField.TAGS: step.tags,
                                        StatusField.STATE: Step.READY,
                                        StatusField.RETURN_VALUE: str(mock_exception),
                                        StatusField.OUTPUT_MESSAGES: [],
                                        StatusField.PROMPT_MESSAGES: [],
                                        StatusField.UNREPLIED_PROMPT_MESSAGE: None})

    def test_extract_essential_tags(self):
        step_1 = Step(lambda x: x, n=0)
        step_1.tags['name'] = 'step_1'
        step_2 = Step(lambda x: x, n=1)
        step_2.tags['name'] = 'step_2'

        essential_tags = list(extract_essential_tags([step_1, step_2]))

        self.assertEqual(essential_tags, [{'n': 0, 'name': 'step_1'}, {'n': 1, 'name': 'step_2'}])

    def test_log_step(self):
        def action_function(trail_env, context):
            return 'test return value {}'.format(context)

        step = Step(action_function)
        step.tags['n'] = 7 # Typically this is set automatically. We're setting this manually for testing purposes.

        mock_logger = MagicMock()

        log_step(mock_logger, step, 'mock message')

        mock_logger.assert_called_once_with('[Step Name=action_function, n=7] mock message')


class TestValidationFunctions(unittest.TestCase):
    def test_validate_states_with_not_an_iterable(self):
        with self.assertRaises(TypeError):
            validate_states(True)

        with self.assertRaises(TypeError):
            validate_states(None)

    def test_validate_states_with_invalid_state(self):
        with self.assertRaises(ValueError):
            validate_states(['invalid_state'])

    def test_validate_states_with_valid_state(self):
        self.assertEqual(None, validate_states([Step.TOSKIP]))

    def test_validate_status_fields_with_wrong_type(self):
        with self.assertRaises(TypeError):
            validate_status_fields(True)

    def test_validate_status_fields_with_invalid_field(self):
        with self.assertRaises(ValueError):
            validate_status_fields('invalid_field')

    def test_validate_status_fields(self):
        return_value = validate_status_fields([StatusField.RETURN_VALUE])

        self.assertEqual(return_value, None)

    def test_validate_mapping_with_valid_tags(self):
        self.assertEqual(None, validate_mapping(dict(foo='bar')))

    def test_validate_mapping_with_invalid_tags(self):
        with self.assertRaises(TypeError):
            validate_mapping(['foo', 'bar'])

        with self.assertRaises(TypeError):
            validate_mapping(True)

        with self.assertRaises(TypeError):
            validate_mapping(None)

    def test_validate_number_with_valid_number(self):
        self.assertEqual(None, validate_number(1))
        self.assertEqual(None, validate_number(0))
        self.assertEqual(None, validate_number(42))

    def test_validate_number_with_invalid_number(self):
        with self.assertRaises(TypeError):
            validate_number('5')

        with self.assertRaises(TypeError):
            validate_number([])

    def test_validate_boolean_with_boolean_values(self):
        self.assertEqual(None, validate_boolean(True))
        self.assertEqual(None, validate_boolean(False))

    def test_validate_boolean_with_invalid_boolean_values(self):
        with self.assertRaises(ValueError):
            validate_boolean(None)

        with self.assertRaises(ValueError):
            validate_boolean('')

        with self.assertRaises(ValueError):
            validate_boolean(0)

        with self.assertRaises(ValueError):
            validate_boolean(1)

        with self.assertRaises(ValueError):
            validate_boolean([])

    def test_validate_api_call_with_no_definition(self):
        api_call = {
            APICallField.NAME: 'non_existent_api'
        }

        return_value = validate_api_call(api_call, None)

        self.assertEqual(return_value, 'API name non_existent_api is invalid.')

    def test_validate_api_call_when_validation_succeeds(self):
        api_call = {
            APICallField.NAME: 'mock_api',
            'mock_field': 'mock_value',
        }
        mock_api_call_definition = MagicMock()
        mock_validator = MagicMock()
        mock_api_call_definition.validators = {'mock_field': mock_validator}

        return_value = validate_api_call(api_call, mock_api_call_definition)

        mock_validator.assert_called_once_with('mock_value')
        self.assertEqual(return_value, None)

    def test_validate_api_call_when_validator_fails(self):
        api_call = {
            APICallField.NAME: 'mock_api',
            'mock_field': 'mock_value',
        }
        mock_api_call_definition = MagicMock()
        mock_validator = MagicMock(side_effect=ValueError('mock_error'))
        mock_api_call_definition.validators = {'mock_field': mock_validator}

        return_value = validate_api_call(api_call, mock_api_call_definition)

        mock_validator.assert_called_once_with('mock_value')
        self.assertEqual(return_value, ('API Call validation failed: The parameter mock_field has an invalid value: '
                                        'mock_value. Error: mock_error'))


class TestHandlers(unittest.TestCase):
    def test_change_attribute(self):
        log_step_patcher = patch('autotrail.layer1.api.log_step')
        mock_log_step = log_step_patcher.start()
        step = MagicMock()
        step.test_attribute = 'old_mock_value'

        return_value = change_attribute([step], 'test_attribute', 'mock_value')

        self.assertEqual(step.test_attribute, 'mock_value')
        mock_log_step.assert_called_once_with(logging.debug, step, 'Changing attribute test_attribute to mock_value')
        self.assertEqual(return_value, None)

        log_step_patcher.stop()

    def test_interrupt(self):
        log_step_patcher = patch('autotrail.layer1.api.log_step')
        mock_log_step = log_step_patcher.start()
        step = MagicMock()

        return_value = interrupt([step])

        step.process.terminate.assert_called_once_with()
        self.assertEqual(return_value, None)

        log_step_patcher.stop()

    def test_get_status(self):
        mock_result = MagicMock()
        step_to_stepstatus_patcher = patch('autotrail.layer1.api.step_to_stepstatus', return_value=mock_result)
        mock_step_to_stepstatus = step_to_stepstatus_patcher.start()
        step = MagicMock()
        status_fields = [StatusField.RETURN_VALUE]

        return_value = get_status([step], status_fields)

        mock_step_to_stepstatus.assert_called_once_with(step, status_fields)
        self.assertEqual(return_value, [mock_result])

        step_to_stepstatus_patcher.stop()

    def test_send_message_to_steps(self):
        log_step_patcher = patch('autotrail.layer1.api.log_step')
        mock_log_step = log_step_patcher.start()
        step = MagicMock()

        return_value = send_message_to_steps([step], 'mock_message')

        step.input_queue.put.assert_called_once_with('mock_message')
        step.input_messages.append.assert_called_once_with('mock_message')
        self.assertEqual(return_value, None)
        mock_log_step.assert_called_once_with(logging.debug, step, 'Sent message to step. Message: mock_message')

        log_step_patcher.stop()

    def test_handle_api_call(self):
        mock_api_call = {
            APICallField.NAME: 'mock_api_call_name'
        }
        validate_api_call_patcher = patch('autotrail.layer1.api.validate_api_call', return_value=None)
        mock_validate_api_call = validate_api_call_patcher.start()
        mock_steps = MagicMock()
        mock_filtered_steps = MagicMock()
        mock_api_call_definition = APICallDefinition(
            validators=MagicMock(),
            steps=MagicMock(return_value=[mock_filtered_steps]),
            predicate=MagicMock(return_value=True),
            handlers=[MagicMock(return_value='mock_result')],
            post_processor=MagicMock(return_value=('mock_result', 'mock_error', 'mock_return_value')),
        )
        mock_api_call_definitions = dict(mock_api_call_name=mock_api_call_definition)

        handler_result = handle_api_call(mock_api_call, mock_steps, api_call_definitions=mock_api_call_definitions)

        mock_validate_api_call.assert_called_once_with(mock_api_call, mock_api_call_definition)
        mock_api_call_definition.steps.assert_called_once_with(mock_steps, mock_api_call)
        mock_api_call_definition.predicate.assert_called_once_with([mock_filtered_steps], mock_api_call)
        mock_api_call_definition.handlers[0].assert_called_once_with([mock_filtered_steps], mock_api_call)
        mock_api_call_definition.post_processor.assert_called_once_with(
            [mock_filtered_steps], mock_api_call, 'mock_result')
        expected_handler_result = HandlerResult(
            response=dict(name='mock_api_call_name', result='mock_result', error='mock_error'),
            return_value='mock_return_value')
        self.assertEqual(handler_result, expected_handler_result)

        validate_api_call_patcher.stop()

    def test_handle_api_call_when_validation_fails(self):
        mock_api_call = {
            APICallField.NAME: 'mock_api_call_name'
        }
        validate_api_call_patcher = patch('autotrail.layer1.api.validate_api_call',
                                          return_value='mock_validation_error')
        mock_validate_api_call = validate_api_call_patcher.start()
        mock_steps = MagicMock()
        mock_filtered_steps = MagicMock()
        mock_api_call_definition = APICallDefinition(
            validators=MagicMock(),
            steps=MagicMock(return_value=[mock_filtered_steps]),
            predicate=MagicMock(return_value=True),
            handlers=[MagicMock(return_value='mock_result')],
            post_processor=MagicMock(return_value=('mock_result', None, 'mock_return_value')),
        )
        mock_api_call_definitions = dict(mock_api_call_name=mock_api_call_definition)

        handler_result = handle_api_call(mock_api_call, mock_steps, api_call_definitions=mock_api_call_definitions)

        mock_validate_api_call.assert_called_once_with(mock_api_call, mock_api_call_definition)
        self.assertEqual(mock_api_call_definition.steps.call_count, 0)
        self.assertEqual(mock_api_call_definition.predicate.call_count, 0)
        self.assertEqual(mock_api_call_definition.handlers[0].call_count, 0)
        self.assertEqual(mock_api_call_definition.post_processor.call_count, 0)

        expected_handler_result = HandlerResult(
            response=dict(name='mock_api_call_name', result=None, error='mock_validation_error'),
            return_value=True)
        self.assertEqual(handler_result, expected_handler_result)

        validate_api_call_patcher.stop()

    def test_handle_api_call_with_multiple_handlers(self):
        mock_api_call = {
            APICallField.NAME: 'mock_api_call_name'
        }
        validate_api_call_patcher = patch('autotrail.layer1.api.validate_api_call', return_value=None)
        mock_validate_api_call = validate_api_call_patcher.start()
        mock_steps = MagicMock()
        mock_filtered_steps = MagicMock()
        mock_api_call_definition = APICallDefinition(
            validators=MagicMock(),
            steps=MagicMock(return_value=[mock_filtered_steps]),
            predicate=MagicMock(return_value=True),
            handlers=[MagicMock(return_value='mock_result_1'), MagicMock(return_value='mock_result_2')],
            post_processor=MagicMock(return_value=('mock_result', 'mock_error', 'mock_return_value')),
        )
        mock_api_call_definitions = dict(mock_api_call_name=mock_api_call_definition)

        handler_result = handle_api_call(mock_api_call, mock_steps, api_call_definitions=mock_api_call_definitions)

        mock_validate_api_call.assert_called_once_with(mock_api_call, mock_api_call_definition)
        mock_api_call_definition.steps.assert_called_once_with(mock_steps, mock_api_call)
        mock_api_call_definition.predicate.assert_called_once_with([mock_filtered_steps], mock_api_call)
        mock_api_call_definition.handlers[0].assert_called_once_with([mock_filtered_steps], mock_api_call)
        mock_api_call_definition.handlers[1].assert_called_once_with(
            [mock_filtered_steps], mock_api_call, 'mock_result_1')
        mock_api_call_definition.post_processor.assert_called_once_with(
            [mock_filtered_steps], mock_api_call, 'mock_result_2')
        expected_handler_result = HandlerResult(
            response=dict(name='mock_api_call_name', result='mock_result', error='mock_error'),
            return_value='mock_return_value')
        self.assertEqual(handler_result, expected_handler_result)

        validate_api_call_patcher.stop()

    def test_handle_api_call_when_predicate_fails(self):
        mock_api_call = {
            APICallField.NAME: 'mock_api_call_name'
        }
        validate_api_call_patcher = patch('autotrail.layer1.api.validate_api_call', return_value=None)
        mock_validate_api_call = validate_api_call_patcher.start()
        mock_steps = MagicMock()
        mock_filtered_steps = MagicMock()
        mock_api_call_definition = APICallDefinition(
            validators=MagicMock(),
            steps=MagicMock(return_value=[mock_filtered_steps]),
            predicate=MagicMock(return_value=False),
            handlers=[MagicMock(return_value='mock_result')],
            post_processor=MagicMock(return_value=('mock_result', 'mock_error', 'mock_return_value')),
        )
        mock_api_call_definitions = dict(mock_api_call_name=mock_api_call_definition)

        handler_result = handle_api_call(mock_api_call, mock_steps, api_call_definitions=mock_api_call_definitions)

        mock_validate_api_call.assert_called_once_with(mock_api_call, mock_api_call_definition)
        mock_api_call_definition.steps.assert_called_once_with(mock_steps, mock_api_call)
        mock_api_call_definition.predicate.assert_called_once_with([mock_filtered_steps], mock_api_call)
        self.assertEqual(mock_api_call_definition.handlers[0].call_count, 0)
        mock_api_call_definition.post_processor.assert_called_once_with(
            [mock_filtered_steps], mock_api_call, None)
        expected_handler_result = HandlerResult(
            response=dict(name='mock_api_call_name', result='mock_result', error='mock_error'),
            return_value='mock_return_value')
        self.assertEqual(handler_result, expected_handler_result)

        validate_api_call_patcher.stop()


class TestAPICallDefinitions(unittest.TestCase):
    def setUp(self):
        self.mock_steps_with_tags = MagicMock()
        self.mock_steps_with_states = MagicMock()
        self.mock_essential_tags = MagicMock()

        self.search_steps_with_tags_patcher = patch('autotrail.layer1.api.search_steps_with_tags',
                                                    return_value=self.mock_steps_with_tags)
        self.mock_search_steps_with_tags = self.search_steps_with_tags_patcher.start()

        self.search_steps_with_states_patcher = patch('autotrail.layer1.api.search_steps_with_states',
                                                      return_value=self.mock_steps_with_states)
        self.mock_search_steps_with_states = self.search_steps_with_states_patcher.start()

        self.extract_essential_tags_patcher = patch('autotrail.layer1.api.extract_essential_tags',
                                                    return_value=self.mock_essential_tags)
        self.mock_extract_essential_tags = self.extract_essential_tags_patcher.start()

        self.change_attribute_patcher = patch('autotrail.layer1.api.change_attribute')
        self.mock_change_attribute = self.change_attribute_patcher.start()

        self.mock_api_call = {
            APICallField.TAGS: 'mock_tags',
            APICallField.STEP_COUNT: 3,
            APICallField.DRY_RUN: False,
            APICallField.MESSAGE: 'mock_message',
            APICallField.STATES: 'mock_states',
            APICallField.STATUS_FIELDS: 'mock_status_fields',
        }
        self.mock_steps = MagicMock()

    def tearDown(self):
        self.search_steps_with_tags_patcher.stop()
        self.extract_essential_tags_patcher.stop()
        self.change_attribute_patcher.stop()
        self.search_steps_with_states_patcher.stop()

    def test_definition_start(self):
        api_call_definition = API_CALL_DEFINITIONS[APICallName.START]

        result = api_call_definition.steps(self.mock_steps, self.mock_api_call)
        self.mock_search_steps_with_states.assert_called_once_with(self.mock_steps, [Step.READY])
        self.assertEqual(result, self.mock_steps_with_states)

        result = api_call_definition.predicate(self.mock_steps, self.mock_api_call)
        self.assertTrue(result)

        self.assertEqual(len(api_call_definition.handlers), 1)

        result = api_call_definition.handlers[0](self.mock_steps, self.mock_api_call)

        self.mock_change_attribute.assert_called_once_with(self.mock_steps, attribute='state', value=Step.WAIT)

        result, error, return_value = api_call_definition.post_processor(self.mock_steps, self.mock_api_call, result)
        self.mock_extract_essential_tags.assert_called_once_with(self.mock_steps)
        self.assertEqual(result, list(self.mock_essential_tags))
        self.assertEqual(error, None)
        self.assertEqual(return_value, True)

    def test_definition_shutdown(self):
        api_call_definition = API_CALL_DEFINITIONS[APICallName.SHUTDOWN]

        self.assertEqual(api_call_definition.steps, None)

        result = api_call_definition.predicate(self.mock_steps, self.mock_api_call)
        self.assertFalse(result)

        self.assertEqual(api_call_definition.handlers, None)

        result, error, return_value = api_call_definition.post_processor(self.mock_steps, self.mock_api_call, result)
        self.assertEqual(result, True)
        self.assertEqual(error, None)
        self.assertEqual(return_value, False)

    def test_definition_list(self):
        api_call_definition = API_CALL_DEFINITIONS[APICallName.LIST]
        mock_step = MagicMock()

        result = api_call_definition.steps([mock_step], self.mock_api_call)
        self.mock_search_steps_with_tags.assert_called_once_with([mock_step], 'mock_tags')
        self.assertEqual(result, self.mock_steps_with_tags)

        result = api_call_definition.predicate([mock_step], self.mock_api_call)
        self.assertFalse(result)

        self.assertEqual(api_call_definition.handlers, None)

        result, error, return_value = api_call_definition.post_processor([mock_step], self.mock_api_call, result)
        self.assertEqual(result, [mock_step.tags])
        self.assertEqual(error, None)
        self.assertEqual(return_value, True)

    def test_definition_status(self):
        get_status_patcher = patch('autotrail.layer1.api.get_status', return_value='mock_statuses')
        mock_get_status = get_status_patcher.start()

        api_call_definition = API_CALL_DEFINITIONS[APICallName.STATUS]

        result = api_call_definition.steps(self.mock_steps, self.mock_api_call)
        self.mock_search_steps_with_states.assert_called_once_with(self.mock_steps_with_tags,
                                                                   self.mock_api_call[APICallField.STATES])
        self.mock_search_steps_with_tags.assert_called_once_with(self.mock_steps, self.mock_api_call[APICallField.TAGS])
        self.assertEqual(result, self.mock_steps_with_states)

        result = api_call_definition.predicate(self.mock_steps, self.mock_api_call)
        self.assertTrue(result)

        self.assertEqual(len(api_call_definition.handlers), 1)

        result = api_call_definition.handlers[0](self.mock_steps, self.mock_api_call)
        mock_get_status.assert_called_once_with(self.mock_steps, self.mock_api_call[APICallField.STATUS_FIELDS])
        self.assertEqual(result, 'mock_statuses')

        result, error, return_value = api_call_definition.post_processor(self.mock_steps, self.mock_api_call, result)
        self.assertEqual(result, 'mock_statuses')
        self.assertEqual(error, None)
        self.assertEqual(return_value, True)

        get_status_patcher.stop()

    def check_api_definition(self, api_call_name, from_states, attribute, to_value):
        api_call_definition = API_CALL_DEFINITIONS[api_call_name]

        result = api_call_definition.steps(self.mock_steps, self.mock_api_call)
        self.mock_search_steps_with_states.assert_called_once_with(self.mock_steps_with_tags,
                                                                   from_states)
        self.mock_search_steps_with_tags.assert_called_once_with(self.mock_steps, self.mock_api_call[APICallField.TAGS])
        self.assertEqual(result, self.mock_steps_with_states)

        result = api_call_definition.predicate(self.mock_steps, self.mock_api_call)
        self.assertTrue(result)

        self.assertEqual(len(api_call_definition.handlers), 1)

        result = api_call_definition.handlers[0](self.mock_steps, self.mock_api_call)
        self.mock_change_attribute.assert_called_once_with(self.mock_steps, attribute=attribute, value=to_value)

        result, error, return_value = api_call_definition.post_processor(self.mock_steps, self.mock_api_call, result)
        self.mock_extract_essential_tags.assert_called_once_with(self.mock_steps)
        self.assertEqual(result, list(self.mock_essential_tags))
        self.assertEqual(error, None)
        self.assertEqual(return_value, True)

    def test_definition_pause(self):
        self.check_api_definition(APICallName.PAUSE, [Step.READY, Step.WAIT], 'state', Step.TOPAUSE)

    def test_definition_interrupt(self):
        interrupt_patcher = patch('autotrail.layer1.api.interrupt', return_value='mock_interrupt_return_value')
        mock_interrupt = interrupt_patcher.start()

        api_call_definition = API_CALL_DEFINITIONS[APICallName.INTERRUPT]

        result = api_call_definition.steps(self.mock_steps, self.mock_api_call)
        self.mock_search_steps_with_states.assert_called_once_with(self.mock_steps_with_tags, [Step.RUN])
        self.mock_search_steps_with_tags.assert_called_once_with(self.mock_steps, self.mock_api_call[APICallField.TAGS])
        self.assertEqual(result, self.mock_steps_with_states)

        result = api_call_definition.predicate(self.mock_steps, self.mock_api_call)
        self.assertTrue(result)

        self.assertEqual(len(api_call_definition.handlers), 2)

        result = api_call_definition.handlers[0](self.mock_steps, self.mock_api_call)
        mock_interrupt.assert_called_once_with(self.mock_steps)
        self.assertEqual(result, 'mock_interrupt_return_value')

        result = api_call_definition.handlers[1](self.mock_steps, self.mock_api_call, 'mock_interrupt_return_value')
        self.mock_change_attribute.assert_called_once_with(self.mock_steps, attribute='state', value=Step.INTERRUPTED)

        result, error, return_value = api_call_definition.post_processor(self.mock_steps, self.mock_api_call, result)
        self.mock_extract_essential_tags.assert_called_once_with(self.mock_steps)
        self.assertEqual(result, list(self.mock_essential_tags))
        self.assertEqual(error, None)
        self.assertEqual(return_value, True)

        interrupt_patcher.stop()

    def test_definition_resume(self):
        self.check_api_definition(APICallName.RESUME,
                                  [Step.TOPAUSE, Step.PAUSED, Step.PAUSED_ON_FAIL, Step.INTERRUPTED],
                                  'state',
                                  Step.WAIT)

    def test_definition_skip(self):
        self.check_api_definition(APICallName.SKIP,
                                  [Step.READY, Step.WAIT, Step.TOPAUSE, Step.PAUSED, Step.PAUSED_ON_FAIL,
                                   Step.INTERRUPTED],
                                  'state',
                                  Step.TOSKIP)

    def test_definition_unskip(self):
        self.check_api_definition(APICallName.UNSKIP, [Step.TOSKIP], 'state', Step.WAIT)

    def test_definition_block(self):
        self.check_api_definition(APICallName.BLOCK,
                                  [Step.READY, Step.WAIT, Step.TOPAUSE, Step.PAUSED, Step.PAUSED_ON_FAIL,
                                   Step.INTERRUPTED],
                                  'state',
                                  Step.TOBLOCK)

    def test_definition_unblock(self):
        self.check_api_definition(APICallName.UNBLOCK, [Step.TOBLOCK], 'state', Step.WAIT)

    def test_definition_set_pause_on_fail(self):
        self.check_api_definition(APICallName.SET_PAUSE_ON_FAIL,
                                  [Step.READY, Step.WAIT, Step.INTERRUPTED, Step.TOPAUSE, Step.PAUSED],
                                  'pause_on_fail',
                                  True)

    def test_definition_unset_pause_on_fail(self):
        self.check_api_definition(APICallName.UNSET_PAUSE_ON_FAIL,
                                  [Step.READY, Step.WAIT, Step.INTERRUPTED, Step.TOPAUSE, Step.PAUSED],
                                  'pause_on_fail',
                                  False)

    def test_definition_pause_branch(self):
        mock_progeny = MagicMock()
        get_progeny_patcher = patch('autotrail.layer1.api.get_progeny', return_value=mock_progeny)
        mock_get_progeny = get_progeny_patcher.start()

        api_call_definition = API_CALL_DEFINITIONS[APICallName.PAUSE_BRANCH]

        result = api_call_definition.steps(self.mock_steps, self.mock_api_call)
        self.mock_search_steps_with_tags.assert_called_once_with(self.mock_steps, self.mock_api_call[APICallField.TAGS])
        mock_get_progeny.assert_called_once_with(self.mock_steps_with_tags)
        self.mock_search_steps_with_states.assert_called_once_with(mock_progeny, [Step.READY, Step.WAIT])
        self.assertEqual(result, self.mock_steps_with_states)

        result = api_call_definition.predicate(self.mock_steps, self.mock_api_call)
        self.assertTrue(result)

        self.assertEqual(len(api_call_definition.handlers), 1)

        result = api_call_definition.handlers[0](self.mock_steps, self.mock_api_call)
        self.mock_change_attribute.assert_called_once_with(self.mock_steps, attribute='state', value=Step.TOPAUSE)

        result, error, return_value = api_call_definition.post_processor(self.mock_steps, self.mock_api_call, result)
        self.mock_extract_essential_tags.assert_called_once_with(self.mock_steps)
        self.assertEqual(result, list(self.mock_essential_tags))
        self.assertEqual(error, None)
        self.assertEqual(return_value, True)

        get_progeny_patcher.stop()

    def test_definition_resume_branch(self):
        mock_progeny = MagicMock()
        get_progeny_patcher = patch('autotrail.layer1.api.get_progeny', return_value=mock_progeny)
        mock_get_progeny = get_progeny_patcher.start()

        api_call_definition = API_CALL_DEFINITIONS[APICallName.RESUME_BRANCH]

        result = api_call_definition.steps(self.mock_steps, self.mock_api_call)
        self.mock_search_steps_with_tags.assert_called_once_with(self.mock_steps, self.mock_api_call[APICallField.TAGS])
        mock_get_progeny.assert_called_once_with(self.mock_steps_with_tags)
        self.mock_search_steps_with_states.assert_called_once_with(
            mock_progeny, [Step.TOPAUSE, Step.PAUSED, Step.PAUSED_ON_FAIL])
        self.assertEqual(result, self.mock_steps_with_states)

        result = api_call_definition.predicate(self.mock_steps, self.mock_api_call)
        self.assertTrue(result)

        self.assertEqual(len(api_call_definition.handlers), 1)

        result = api_call_definition.handlers[0](self.mock_steps, self.mock_api_call)
        self.mock_change_attribute.assert_called_once_with(self.mock_steps, attribute='state', value=Step.WAIT)

        result, error, return_value = api_call_definition.post_processor(self.mock_steps, self.mock_api_call, result)
        self.mock_extract_essential_tags.assert_called_once_with(self.mock_steps)
        self.assertEqual(result, list(self.mock_essential_tags))
        self.assertEqual(error, None)
        self.assertEqual(return_value, True)

        get_progeny_patcher.stop()

    def test_definition_next_steps(self):
        api_call_definition = API_CALL_DEFINITIONS[APICallName.NEXT_STEPS]

        mock_steps = ['step_1', 'step_2', 'step_3', 'step_4', 'step_5']

        result = api_call_definition.steps(mock_steps, self.mock_api_call)
        self.mock_search_steps_with_states.assert_called_once_with(['step_1', 'step_2', 'step_3'],
                                                                   [Step.TOPAUSE, Step.PAUSED, Step.PAUSED_ON_FAIL])
        self.assertEqual(result, self.mock_steps_with_states)

        result = api_call_definition.predicate(self.mock_steps, self.mock_api_call)
        self.assertTrue(result)

        self.assertEqual(len(api_call_definition.handlers), 1)

        result = api_call_definition.handlers[0](self.mock_steps, self.mock_api_call)
        self.mock_change_attribute.assert_called_once_with(self.mock_steps, attribute='state', value=Step.WAIT)

        result, error, return_value = api_call_definition.post_processor(self.mock_steps, self.mock_api_call, result)
        self.mock_extract_essential_tags.assert_called_once_with(self.mock_steps)
        self.assertEqual(result, list(self.mock_essential_tags))
        self.assertEqual(error, None)
        self.assertEqual(return_value, True)

    def test_definition_send_message_to_steps(self):
        send_message_to_steps_patcher = patch('autotrail.layer1.api.send_message_to_steps')
        mock_send_message_to_steps = send_message_to_steps_patcher.start()

        api_call_definition = API_CALL_DEFINITIONS[APICallName.SEND_MESSAGE_TO_STEPS]

        message_validator = api_call_definition.validators[APICallField.MESSAGE]
        self.assertEqual(message_validator(self.mock_api_call), None)

        result = api_call_definition.steps(self.mock_steps, self.mock_api_call)
        self.mock_search_steps_with_tags.assert_called_once_with(self.mock_steps, self.mock_api_call[APICallField.TAGS])
        self.mock_search_steps_with_states.assert_called_once_with(self.mock_steps_with_tags, [Step.RUN])
        self.assertEqual(result, self.mock_steps_with_states)

        result = api_call_definition.predicate(self.mock_steps, self.mock_api_call)
        self.assertTrue(result)

        self.assertEqual(len(api_call_definition.handlers), 1)

        result = api_call_definition.handlers[0](self.mock_steps, self.mock_api_call)
        mock_send_message_to_steps.assert_called_once_with(self.mock_steps, self.mock_api_call[APICallField.MESSAGE])

        result, error, return_value = api_call_definition.post_processor(self.mock_steps, self.mock_api_call, result)
        self.mock_extract_essential_tags.assert_called_once_with(self.mock_steps)
        self.assertEqual(result, list(self.mock_essential_tags))
        self.assertEqual(error, None)
        self.assertEqual(return_value, True)

        send_message_to_steps_patcher.stop()
