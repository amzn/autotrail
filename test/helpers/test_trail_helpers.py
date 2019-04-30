"""Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.
"""

import mock
import unittest

from mock import MagicMock, patch

from autotrail.core.dag import Step
from autotrail.layer1.api import StatusField
from autotrail.helpers.trail import (
    determine_group_state,
    diff_lists,
    filtered_handlers,
    filter_status_by_states,
    historian,
    monitor,
    monitor_trail,
    search_for_any_element)


class TestFunctions(unittest.TestCase):
    def test_historian_with_high_limit(self):
        function = MagicMock(side_effect=[1, 2, 3])

        histories = []
        for history in historian(function, 6):
            histories.append(list(history))

        self.assertEqual(histories, [[1], [1, 2], [1, 2, 3]])

    def test_historian_with_low_limit(self):
        function = MagicMock(side_effect=[1, 2, 3])

        histories = []
        for history in historian(function, 2):
            histories.append(list(history))

        self.assertEqual(histories, [[1], [1, 2], [2, 3]])

    def test_filtered_handlers(self):
        mock_value = MagicMock()
        mock_filter_with_true_value = MagicMock(return_value=mock_value)
        mock_filter_with_false_value = MagicMock(return_value=[])
        mock_handler = MagicMock()
        mock_filter_handler_pairs = [
            (mock_filter_with_true_value, mock_handler),
            (mock_filter_with_false_value, mock_handler)]

        filtered_handlers(mock_value, mock_filter_handler_pairs)

        mock_filter_with_true_value.assert_called_once_with(mock_value)
        mock_filter_with_false_value.assert_called_once_with(mock_value)
        mock_handler.assert_called_once_with(mock_value)


    def test_monitor(self):
        mock_history = MagicMock()
        historian_patcher = patch('autotrail.helpers.trail.historian',
                                  return_value=[mock_history, mock_history, mock_history])
        mock_historian = historian_patcher.start()
        filtered_handlers_patcher = patch('autotrail.helpers.trail.filtered_handlers')
        mock_filtered_handlers = filtered_handlers_patcher.start()
        sleep_patcher = patch('autotrail.helpers.trail.sleep')
        mock_sleep = sleep_patcher.start()

        mock_function = MagicMock()
        mock_result_predicate = MagicMock(side_effect=[True, False])
        mock_value = MagicMock()
        mock_pre_processor = MagicMock(side_effect=[mock_value, None])
        mock_filter_handler_pairs = MagicMock()

        monitor(mock_function, mock_result_predicate, mock_pre_processor, mock_filter_handler_pairs, delay=50,
                history_limit=7)

        mock_historian.assert_called_once_with(mock_function, 7)
        self.assertEqual(mock_result_predicate.mock_calls, [mock.call(mock_history), mock.call(mock_history)])
        mock_pre_processor.assert_called_once_with(mock_history)
        # This should be called only once because we made the pre_processor return None on one instance.
        mock_filtered_handlers.assert_called_once_with(mock_value, mock_filter_handler_pairs)
        mock_sleep.assert_called_once_with(50)

        sleep_patcher.stop()
        historian_patcher.stop()
        filtered_handlers_patcher.stop()

    def test_monitor_when_no_iteration_happens(self):
        mock_history = MagicMock()
        historian_patcher = patch('autotrail.helpers.trail.historian', return_value=[])
        mock_historian = historian_patcher.start()
        filtered_handlers_patcher = patch('autotrail.helpers.trail.filtered_handlers')
        mock_filtered_handlers = filtered_handlers_patcher.start()
        sleep_patcher = patch('autotrail.helpers.trail.sleep')
        mock_sleep = sleep_patcher.start()

        mock_function = MagicMock()
        mock_result_predicate = MagicMock(side_effect=[True, False])
        mock_value = MagicMock()
        mock_pre_processor = MagicMock(side_effect=[mock_value, None])
        mock_filter_handler_pairs = MagicMock()

        monitor(mock_function, mock_result_predicate, mock_pre_processor, mock_filter_handler_pairs, delay=50,
                history_limit=7)

        mock_historian.assert_called_once_with(mock_function, 7)
        self.assertEqual(mock_result_predicate.call_count, 0)
        self.assertEqual(mock_pre_processor.call_count, 0)
        self.assertEqual(mock_filtered_handlers.call_count, 0)
        self.assertEqual(mock_sleep.call_count, 0)

        sleep_patcher.stop()
        historian_patcher.stop()
        filtered_handlers_patcher.stop()

    def test_monitor_when_pre_processor_returns_no_result(self):
        mock_history = MagicMock()
        historian_patcher = patch('autotrail.helpers.trail.historian',
                                  return_value=[mock_history, mock_history, mock_history])
        mock_historian = historian_patcher.start()
        filtered_handlers_patcher = patch('autotrail.helpers.trail.filtered_handlers')
        mock_filtered_handlers = filtered_handlers_patcher.start()
        sleep_patcher = patch('autotrail.helpers.trail.sleep')
        mock_sleep = sleep_patcher.start()

        mock_function = MagicMock()
        mock_result_predicate = MagicMock(side_effect=[True, False])
        mock_value = MagicMock()
        mock_pre_processor = MagicMock(return_value=[])
        mock_filter_handler_pairs = MagicMock()

        monitor(mock_function, mock_result_predicate, mock_pre_processor, mock_filter_handler_pairs, delay=50,
                history_limit=7)

        mock_historian.assert_called_once_with(mock_function, 7)
        self.assertEqual(mock_result_predicate.mock_calls, [mock.call(mock_history), mock.call(mock_history)])
        mock_pre_processor.assert_called_once_with(mock_history)
        # This should be called only once because we made the pre_processor return None on one instance.
        self.assertEqual(mock_filtered_handlers.call_count, 0)
        mock_sleep.assert_called_once_with(50)

        sleep_patcher.stop()
        historian_patcher.stop()
        filtered_handlers_patcher.stop()

    def test_diff_lists(self):
        lists = [
            ['a', 'b'],
            ['a', 'c'],
        ]

        new_elements = diff_lists(lists)

        self.assertEqual(new_elements, ['c'])

    def test_diff_lists_with_only_one_list(self):
        lists = [
            ['a', 'b'],
        ]

        new_elements = diff_lists(lists)

        self.assertEqual(new_elements, ['a', 'b'])

    def test_diff_lists_when_last_list_is_none(self):
        lists = [
            ['a', 'b'],
            None,
        ]

        new_elements = diff_lists(lists)

        self.assertEqual(new_elements, [])

    def test_diff_lists_when_previous_list_is_none(self):
        lists = [
            None,
            ['a', 'c'],
        ]

        new_elements = diff_lists(lists)

        self.assertEqual(new_elements, ['a', 'c'])

    def test_monitor_trail_with_defaults(self):
        mock_trail_client = MagicMock()
        mock_trail_client.status = MagicMock(return_value='mock_trail_client_result')
        mock_status_filter_handler_pairs = MagicMock()
        monitor_patcher = patch('autotrail.helpers.trail.monitor')
        mock_monitor = monitor_patcher.start()
        diff_lists_patcher = patch('autotrail.helpers.trail.diff_lists')
        mock_diff_lists = diff_lists_patcher.start()

        monitor_trail(mock_trail_client, mock_status_filter_handler_pairs)

        kwargs = mock_monitor.call_args[1]
        function = kwargs['function']
        result_predicate = kwargs['result_predicate']
        filter_handler_pairs = kwargs['filter_handler_pairs']
        pre_processor = kwargs['pre_processor']
        delay = kwargs['delay']
        history_limit = kwargs['history_limit']

        self.assertEqual(delay, 60)
        self.assertEqual(history_limit, 4) # History will be max_tries + 1
        self.assertEqual(filter_handler_pairs, mock_status_filter_handler_pairs)

        result = function()
        self.assertEqual(result, 'mock_trail_client_result')
        mock_trail_client.status.assert_called_once_with(
            fields=[StatusField.STATE, StatusField.UNREPLIED_PROMPT_MESSAGE, StatusField.OUTPUT_MESSAGES,
                    StatusField.RETURN_VALUE],
            states=[Step.RUN, Step.FAILURE, Step.PAUSED, Step.PAUSED_ON_FAIL])

        self.assertTrue(result_predicate([None, None, 'result']))
        self.assertTrue(result_predicate([None, 'result', None]))
        self.assertTrue(result_predicate(['result']))
        self.assertFalse(result_predicate([None, None, None]))

        pre_processor([1, 2, 3])
        mock_diff_lists.assert_called_once_with([2, 3])
        pre_processor([2, 3])
        mock_diff_lists.assert_called_with([2, 3])
        pre_processor([3])
        mock_diff_lists.assert_called_with([3])
        pre_processor([None])
        mock_diff_lists.assert_called_with([None])
        diff_lists_patcher.stop()
        monitor_patcher.stop()


    def test_monitor_trail_with_custom_arguments(self):
        mock_trail_client = MagicMock()
        mock_trail_client.status = MagicMock(return_value='mock_trail_client_result')
        mock_status_filter_handler_pairs = MagicMock()
        monitor_patcher = patch('autotrail.helpers.trail.monitor')
        mock_monitor = monitor_patcher.start()
        diff_lists_patcher = patch('autotrail.helpers.trail.diff_lists')
        mock_diff_lists = diff_lists_patcher.start()

        monitor_trail(mock_trail_client, mock_status_filter_handler_pairs, status_kwargs=dict(foo='bar'), delay=55,
                      max_tries=1)

        kwargs = mock_monitor.call_args[1]
        function = kwargs['function']
        result_predicate = kwargs['result_predicate']
        filter_handler_pairs = kwargs['filter_handler_pairs']
        pre_processor = kwargs['pre_processor']
        delay = kwargs['delay']
        history_limit = kwargs['history_limit']

        self.assertEqual(delay, 55)
        self.assertEqual(history_limit, 2) # History will be max_tries + 1
        self.assertEqual(filter_handler_pairs, mock_status_filter_handler_pairs)

        result = function()
        self.assertEqual(result, 'mock_trail_client_result')
        mock_trail_client.status.assert_called_once_with(foo='bar')

        self.assertTrue(result_predicate([None, 'result']))
        self.assertTrue(result_predicate(['result']))
        self.assertFalse(result_predicate(['result', None]))
        self.assertFalse(result_predicate([None, None, None]))

        pre_processor([1, 2, 3])
        mock_diff_lists.assert_called_once_with([2, 3])
        pre_processor([2, 3])
        mock_diff_lists.assert_called_with([2, 3])
        pre_processor([3])
        mock_diff_lists.assert_called_with([3])
        pre_processor([None])
        mock_diff_lists.assert_called_with([None])
        diff_lists_patcher.stop()
        monitor_patcher.stop()

    def test_search_for_any_element(self):
        list_a = ['d', 'a', 'e']
        list_b = ['a', 'b', 'c']
        element = search_for_any_element(list_a, list_b)

        self.assertEqual(element, 'a')

    def test_search_for_any_element_when_not_found(self):
        list_a = ['d', 'e', 'f']
        list_b = ['a', 'b', 'c']
        element = search_for_any_element(list_a, list_b)

        self.assertEqual(element, None)

    def test_search_for_any_element_with_iterators(self):
        a = iter(['d', 'a', 'e'])
        b = iter(['a', 'b', 'c'])
        element = search_for_any_element(a, b)

        self.assertEqual(element, 'a')

    def test_determine_group_state(self):
        search_for_any_element_patcher = patch('autotrail.helpers.trail.search_for_any_element',
                                               return_value='mock group status')
        mock_search_for_any_element = search_for_any_element_patcher.start()
        mock_states = MagicMock()
        mock_group_state_order = MagicMock()

        status = determine_group_state(mock_states, group_state_order=mock_group_state_order)

        mock_search_for_any_element.assert_called_once_with(mock_group_state_order, mock_states)
        self.assertEqual(status, 'mock group status')

        search_for_any_element_patcher.stop()

    def test_determine_group_state_with_invalid_state(self):
        search_for_any_element_patcher = patch('autotrail.helpers.trail.search_for_any_element',
                                               return_value=None)
        mock_search_for_any_element = search_for_any_element_patcher.start()
        mock_states = MagicMock()
        mock_group_state_order = MagicMock()

        with self.assertRaises(ValueError):
            status = determine_group_state(mock_states, group_state_order=mock_group_state_order)

        mock_search_for_any_element.assert_called_once_with(mock_group_state_order, mock_states)

        search_for_any_element_patcher.stop()

    def test_filter_status_by_states(self):
        statuses = [
            {StatusField.N: 0, StatusField.NAME: 'step_a', StatusField.STATE: Step.READY},
            {StatusField.N: 1, StatusField.NAME: 'step_b', StatusField.STATE: Step.WAIT},
            {StatusField.N: 2, StatusField.NAME: 'step_c', StatusField.STATE: Step.RUN},
            {StatusField.N: 3, StatusField.NAME: 'step_d', StatusField.STATE: Step.INTERRUPTED},
            {StatusField.N: 4, StatusField.NAME: 'step_e', StatusField.STATE: Step.BLOCKED},
        ]

        result = filter_status_by_states(statuses, [Step.READY, Step.RUN])

        self.assertEqual(result, [
            {StatusField.N: 0, StatusField.NAME: 'step_a', StatusField.STATE: Step.READY},
            {StatusField.N: 2, StatusField.NAME: 'step_c', StatusField.STATE: Step.RUN},
        ])
