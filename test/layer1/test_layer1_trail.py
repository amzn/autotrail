"""Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.
"""

import json
import logging
import signal
import unittest

from functools import partial
from mock import MagicMock, patch, call
from Queue import Empty as QueueEmpty
from socket import SHUT_RDWR

from autotrail.core.dag import Step
from autotrail.layer1.api import StatusField, handle_api_call
from autotrail.layer1.trail import (
    assign_sequence_numbers_to_steps,
    check_running_step,
    collect_output_messages_from_step,
    collect_prompt_messages_from_step,
    CyclicException,
    make_dag,
    run_step,
    skip_progeny,
    STATE_TRANSITIONS,
    step_manager,
    StepResult,
    TrailEnvironment,
    trail_manager)


class TestStepResult(unittest.TestCase):
    def test_instance(self):
        step_result = StepResult(result='foo', return_value='bar')

        # Check attributes
        self.assertEqual(step_result.result, 'foo')
        self.assertEqual(step_result.return_value, 'bar')

        self.assertEqual(repr(step_result), 'StepResult(result=\'foo\', return_value=\'bar\')')

    def test_equality(self):
        step_result_1 = StepResult(result='foo', return_value='bar')
        step_result_2 = StepResult(result='foo', return_value='bar')
        self.assertEqual(step_result_1, step_result_2)

        step_result_3 = StepResult(result='new_foo', return_value='bar')
        self.assertNotEqual(step_result_1, step_result_3)


class TestStepManager(unittest.TestCase):
    def test_step_manager(self):
        def action_function(trail_env, context):
            return 'test return value {}'.format(context)

        step = Step(action_function)
        step.tags['n'] = 7 # Typically this is set automatically. We're setting this manually for testing purposes.
        step.result_queue = MagicMock()
        trail_environment = MagicMock()
        step_manager(step, trail_environment, context='foo')

        expected_result = StepResult(result=Step.SUCCESS, return_value='test return value foo')
        step.result_queue.put.assert_called_once_with(expected_result)

    def test_step_manager_with_exception(self):
        def action_function(trail_env, context):
            raise Exception('test exception')
            return 'test return value'

        step = Step(action_function)
        step.tags['n'] = 7 # Typically this is set automatically. We're setting this manually for testing purposes.
        step.result_queue = MagicMock()
        trail_environment = MagicMock()
        step_manager(step, trail_environment, context='foo')

        expected_result = StepResult(result=Step.PAUSED_ON_FAIL, return_value='test exception')
        step.result_queue.put.assert_called_once_with(expected_result)

    def test_step_manager_with_unset_pause_on_fail(self):
        def action_function(trail_env, context):
            raise Exception('test exception')
            return 'test return value'

        step = Step(action_function)
        step.tags['n'] = 7 # Typically this is set automatically. We're setting this manually for testing purposes.
        step.pause_on_fail = False
        step.result_queue = MagicMock()
        trail_environment = MagicMock()
        step_manager(step, trail_environment, context='foo')

        expected_result = StepResult(result=Step.FAILURE, return_value='test exception')
        step.result_queue.put.assert_called_once_with(expected_result)


class TestHelperFunctions(unittest.TestCase):
    def test_run_step(self):
        def action_function_a():
            return 'test return value'

        step = Step(action_function_a)
        step.tags['n'] = 0

        process_patcher = patch('autotrail.layer1.trail.Process')
        mock_process = process_patcher.start()

        run_step(step, 'mock_context')

        process_patcher.stop()

        args, kwargs = mock_process.call_args
        arg_step, arg_trail_env, arg_context = kwargs['args']
        self.assertEqual(step, arg_step)
        self.assertIsInstance(arg_trail_env, TrailEnvironment)
        self.assertEqual(arg_context, 'mock_context')
        self.assertEqual(kwargs['target'], step_manager)

        self.assertIn(call().start(), mock_process.mock_calls)
        self.assertEqual(step.state, step.RUN)

    def test_run_step_with_rerun_of_a_step(self):
        def action_function_a():
            return 'test return value'

        step = Step(action_function_a)
        step.tags['n'] = 0

        # Setup some old values for the step. Similar to the effect of running the step might have.
        step.return_value = 'Some old return value.'
        step.prompt_messages = ['old message 1', 'old message 2']
        step.input_messages = ['response to old message 1']

        process_patcher = patch('autotrail.layer1.trail.Process')
        mock_process = process_patcher.start()

        run_step(step, 'mock_context')

        process_patcher.stop()

        args, kwargs = mock_process.call_args
        arg_step, arg_trail_env, arg_context = kwargs['args']
        self.assertEqual(step, arg_step)
        self.assertIsInstance(arg_trail_env, TrailEnvironment)
        self.assertEqual(arg_context, 'mock_context')
        self.assertEqual(kwargs['target'], step_manager)

        self.assertIn(call().start(), mock_process.mock_calls)
        self.assertEqual(step.state, step.RUN)

        # Because the step is being re-run, the old values of the following attributes should get reset.
        self.assertEqual(step.return_value, None)
        self.assertEqual(step.prompt_messages, [])
        self.assertEqual(step.input_messages, [])

    def test_skip_progeny(self):
        mock_step = MagicMock()
        mock_step.state = Step.WAIT
        search_steps_with_states_patcher = patch('autotrail.layer1.trail.search_steps_with_states',
                                                 return_value=[mock_step])
        mock_search_steps_with_states = search_steps_with_states_patcher.start()
        get_progeny_patcher = patch('autotrail.layer1.trail.get_progeny', return_value='mock_progeny')
        mock_get_progeny = get_progeny_patcher.start()

        skip_progeny(mock_step)

        mock_get_progeny.assert_called_once_with([mock_step])
        mock_search_steps_with_states.assert_called_once_with('mock_progeny', states=[Step.WAIT, Step.TOPAUSE])
        self.assertEqual(mock_step.state, Step.SKIPPED)

        get_progeny_patcher.stop()
        search_steps_with_states_patcher.stop()

    def test_collect_output_messages_from_step(self):
        step = Step(lambda x: x)
        step.tags['n'] = 0
        step.output_queue = MagicMock()
        step.output_queue.get_nowait = MagicMock(side_effect=['foo', QueueEmpty()])

        collect_output_messages_from_step(step)

        self.assertEqual(step.output_queue.get_nowait.mock_calls, [call(), call()])

        expected_messages = ['foo']

        self.assertEqual(step.output_messages, expected_messages)

    def test_collect_prompt_messages_from_step(self):
        step = Step(lambda x: x)
        step.tags['n'] = 0
        step.prompt_queue = MagicMock()
        step.prompt_queue.get_nowait = MagicMock(side_effect=['foo', QueueEmpty()])

        collect_prompt_messages_from_step(step)

        self.assertEqual(step.prompt_queue.get_nowait.mock_calls, [call(), call()])

        expected_messages = ['foo']

        self.assertEqual(step.prompt_messages, expected_messages)

    def test_make_dag(self):
        # Make this DAG:
        #           +--> step_b -->+
        # step_a -->|              |--> step_d
        #           +--> step_c -->+
        step_a = Step(lambda x: x)
        step_b = Step(lambda x: x)
        step_c = Step(lambda x: x)
        step_d = Step(lambda x: x)

        trail_definition = [
            (step_a, step_b),
            (step_a, step_c),
            (step_b, step_d),
            (step_c, step_d),
        ]

        root_step = make_dag(trail_definition)
        self.assertEqual(root_step, step_a)

        self.assertEqual(step_a.tags['n'], 0)
        self.assertEqual(step_d.tags['n'], 3)

    def test_make_dag_cyclic(self):
        # Make this cyclic DAG:
        # step_a --> step_b -->+
        #    ^                 |
        #    |                 v
        #    +<----- step_c <--+
        step_a = Step(lambda x: x)
        step_b = Step(lambda x: x)
        step_c = Step(lambda x: x)

        trail_definition = [
            (step_a, step_b),
            (step_b, step_c),
            (step_c, step_a),
        ]

        with self.assertRaises(CyclicException):
            root_step = make_dag(trail_definition)

    def test_assign_sequence_numbers_to_steps(self):
        # Make this DAG:
        #           +--> step_b -->+
        # step_a -->|              |--> step_d
        #           +--> step_c -->+
        step_a = Step(lambda x: x)
        step_b = Step(lambda x: x)
        step_c = Step(lambda x: x)
        step_d = Step(lambda x: x)

        trail_definition = [
            (step_a, step_b),
            (step_a, step_c),
            (step_b, step_d),
            (step_c, step_d),
        ]

        root_step = make_dag(trail_definition)
        assign_sequence_numbers_to_steps(root_step)

        get_number = lambda x: x.tags['n']

        self.assertEqual(get_number(step_a), 0)
        self.assertEqual(get_number(step_d), 3)

        self.assertGreater(get_number(step_d), get_number(step_b))
        self.assertGreater(get_number(step_d), get_number(step_c))

        self.assertLess(get_number(step_a), get_number(step_b))
        self.assertLess(get_number(step_a), get_number(step_c))

        self.assertNotEqual(get_number(step_b), get_number(step_c))


class TestTrailEnvironment(unittest.TestCase):
    def setUp(self):
        self.input_queue = MagicMock()
        self.output_queue = MagicMock()
        self.prompt_queue = MagicMock()
        self.trail_environment = TrailEnvironment(self.prompt_queue, self.input_queue, self.output_queue)

    def test_instance(self):
        self.assertIsInstance(self.trail_environment, TrailEnvironment)

    def test_input(self):
        return_value = self.trail_environment.input('foo')

        self.prompt_queue.put.assert_called_once_with('foo')
        self.input_queue.get.assert_called_once_with(timeout=None)
        self.assertEqual(return_value, self.input_queue.get())

    def test_input_with_timeout(self):
        return_value = self.trail_environment.input('foo', timeout=1)

        self.prompt_queue.put.assert_called_once_with('foo')
        self.input_queue.get.assert_called_once_with(timeout=1)
        self.assertEqual(return_value, self.input_queue.get())

    def test_output(self):
        self.trail_environment.output('foo')

        self.output_queue.put.assert_called_once_with('foo')


class TestCheckRunningStep(unittest.TestCase):
    def setUp(self):
        self.log_step_patcher = patch('autotrail.layer1.trail.log_step')
        self.mock_log_step = self.log_step_patcher.start()

        self.skip_progeny_patcher = patch('autotrail.layer1.trail.skip_progeny')
        self.mock_skip_progeny = self.skip_progeny_patcher.start()

        self.collect_output_messages_from_step_patcher = patch(
            'autotrail.layer1.trail.collect_output_messages_from_step')
        self.mock_collect_output_messages_from_step = self.collect_output_messages_from_step_patcher.start()

        self.collect_prompt_messages_from_step_patcher = patch(
            'autotrail.layer1.trail.collect_prompt_messages_from_step')
        self.mock_collect_prompt_messages_from_step = self.collect_prompt_messages_from_step_patcher.start()

        self.mock_result = MagicMock()
        self.mock_result.result = 'mock_result'
        self.mock_result.return_value = 'mock_return_value'
        self.mock_step = Step(lambda x: x)
        self.mock_step.skip_progeny_on_failure = False
        self.mock_step.result_queue = MagicMock()
        self.mock_step.result_queue.get_nowait = MagicMock(return_value=self.mock_result)

    def tearDown(self):
        self.collect_prompt_messages_from_step_patcher.stop()
        self.collect_output_messages_from_step_patcher.stop()
        self.skip_progeny_patcher.stop()
        self.log_step_patcher.stop()

    def test_when_step_returns_result(self):
        check_running_step(self.mock_step)

        self.assertEqual(self.mock_step.state, 'mock_result')
        self.assertEqual(self.mock_step.return_value, 'mock_return_value')
        self.mock_log_step.assert_called_once_with(logging.debug, self.mock_step, (
            'Step has completed. Changed state to: mock_result. Setting return value to: mock_return_value'))
        self.assertEqual(self.mock_skip_progeny.call_count, 0)
        self.mock_collect_output_messages_from_step.assert_called_once_with(self.mock_step)
        self.mock_collect_prompt_messages_from_step.assert_called_once_with(self.mock_step)

    def test_when_step_does_not_return_a_result(self):
        self.mock_step.result_queue.get_nowait = MagicMock(side_effect=QueueEmpty)

        check_running_step(self.mock_step)

        self.assertNotEqual(self.mock_step.state, 'mock_result')
        self.assertNotEqual(self.mock_step.return_value, 'mock_return_value')
        self.assertEqual(self.mock_log_step.call_count, 0)
        self.assertEqual(self.mock_skip_progeny.call_count, 0)
        self.mock_collect_output_messages_from_step.assert_called_once_with(self.mock_step)
        self.mock_collect_prompt_messages_from_step.assert_called_once_with(self.mock_step)

    def test_when_step_fails_and_progeny_need_to_be_skipped(self):
        self.mock_result.result = Step.FAILURE
        self.mock_result.return_value = 'mock_return_value'
        self.mock_step.skip_progeny_on_failure = True
        self.mock_step.state = Step.RUN
        self.mock_step.return_value = None
        self.mock_step.result_queue.get_nowait = MagicMock(return_value=self.mock_result)

        check_running_step(self.mock_step)

        self.assertEqual(self.mock_step.state, Step.FAILURE)
        self.assertEqual(self.mock_step.return_value, 'mock_return_value')
        self.mock_log_step.assert_called_once_with(logging.debug, self.mock_step, (
            'Step has completed. Changed state to: {}. Setting return value to: mock_return_value').format(
                Step.FAILURE))
        self.mock_skip_progeny.assert_called_once_with(self.mock_step)
        self.mock_collect_output_messages_from_step.assert_called_once_with(self.mock_step)
        self.mock_collect_prompt_messages_from_step.assert_called_once_with(self.mock_step)

class TestStateTransitions(unittest.TestCase):
    def test_when_state_is_running(self):
        mock_step = MagicMock()
        mock_context = MagicMock()
        check_running_step_patcher = patch('autotrail.layer1.trail.check_running_step', return_value=None)
        mock_check_running_step = check_running_step_patcher.start()

        transition_function = STATE_TRANSITIONS[Step.RUN]

        return_value = transition_function(mock_step, mock_context)
        self.assertFalse(return_value)
        mock_check_running_step.assert_called_once_with(mock_step)

        check_running_step_patcher.stop()

    def test_when_state_is_to_skip(self):
        mock_step = MagicMock()
        mock_step.state = 'mock_state'
        mock_context = MagicMock()

        transition_function = STATE_TRANSITIONS[Step.TOSKIP]

        return_value = transition_function(mock_step, mock_context)
        self.assertFalse(return_value)
        self.assertEqual(mock_step.state, Step.SKIPPED)

    def test_when_state_is_to_pause(self):
        mock_step = MagicMock()
        mock_step.state = 'mock_state'
        mock_context = MagicMock()

        transition_function = STATE_TRANSITIONS[Step.TOPAUSE]

        return_value = transition_function(mock_step, mock_context)
        self.assertFalse(return_value)
        self.assertEqual(mock_step.state, Step.PAUSED)

    def test_when_state_is_to_block(self):
        mock_step = MagicMock()
        mock_step.state = 'mock_state'
        mock_context = MagicMock()

        transition_function = STATE_TRANSITIONS[Step.TOBLOCK]

        return_value = transition_function(mock_step, mock_context)
        self.assertFalse(return_value)
        self.assertEqual(mock_step.state, Step.BLOCKED)


class TestTrailManager(unittest.TestCase):
    def setUp(self):
        self.mock_root_step = MagicMock()
        self.mock_context = MagicMock()
        self.mock_step = MagicMock()
        self.mock_step.state = 'mock_state'
        self.step_iterator = iter([self.mock_step])
        self.topological_while_patcher = patch('autotrail.layer1.trail.topological_while',
                                                return_value=self.step_iterator)
        self.mock_topological_while = self.topological_while_patcher.start()
        self.topological_traverse_patcher = patch('autotrail.layer1.trail.topological_traverse',
                                                  return_value=iter([self.mock_step]))
        self.mock_topological_traverse = self.topological_traverse_patcher.start()
        self.handle_api_call_patcher = patch('autotrail.layer1.trail.handle_api_call')
        self.mock_handle_api_call = self.handle_api_call_patcher.start()
        # serve_socket needs to return False for the trail_manager to stop.
        self.serve_socket_patcher = patch('autotrail.layer1.trail.serve_socket', side_effect=[True, False])
        self.mock_serve_socket = self.serve_socket_patcher.start()
        self.mock_backup = MagicMock()
        self.mock_api_socket = MagicMock()
        self.mock_state_transition = MagicMock(return_value='mock_state_function_return_value')
        self.mock_state_transitions = dict(mock_state=self.mock_state_transition)
        self.sleep_patcher = patch('autotrail.layer1.trail.sleep')
        self.mock_sleep = self.sleep_patcher.start()

    def tearDown(self):
        self.handle_api_call_patcher.stop()
        self.topological_traverse_patcher.stop()
        self.sleep_patcher.stop()
        self.serve_socket_patcher.stop()
        self.topological_while_patcher.stop()

    def test_normal_run(self):
        trail_manager(self.mock_root_step, self.mock_api_socket, self.mock_backup, delay=12, context=self.mock_context,
                      state_transitions=self.mock_state_transitions)

        self.mock_topological_traverse.assert_called_once_with(self.mock_root_step)
        self.mock_state_transition.assert_called_once_with(self.mock_step, self.mock_context)

        root_step, done_check, ignore_check = self.mock_topological_while.call_args[0]
        self.assertEqual(root_step, self.mock_root_step)

        # This table is used to check the possible results of done_check and ignore_check functions
        check_table = [
            # State,                Expected result of done_check,      Expected result of ignore_check
            (Step.SUCCESS,           True,                               False),
            (Step.SKIPPED,           True,                               False),
            (Step.READY,             False,                              False),
            (Step.RUN,               False,                              False),
            (Step.FAILURE,           False,                              True),
            (Step.BLOCKED,           False,                              True),
            (Step.PAUSED,            False,                              False),
            (Step.INTERRUPTED,       False,                              False),
            (Step.PAUSED_ON_FAIL,    False,                              False),
            (Step.WAIT,              False,                              False),
            (Step.TOSKIP,            False,                              False),
            (Step.TOBLOCK,           False,                              False),
            (Step.TOPAUSE,           False,                              False),
        ]
        for state, done_check_result, ignore_check_result in check_table:
            step = Step(lambda: True)
            step.state = state
            self.assertEqual(done_check(step), done_check_result)
            self.assertEqual(ignore_check(step), ignore_check_result)

        self.assertEqual(len(self.mock_serve_socket.mock_calls), 2)
        for mock_call in self.mock_serve_socket.mock_calls:
            socket, handler = mock_call[1]
            self.assertEqual(socket, self.mock_api_socket)
            mock_request = MagicMock()
            handler(mock_request)
            self.mock_handle_api_call.assert_called_with(mock_request, steps=[self.mock_step])

        self.assertEqual(list(self.step_iterator), []) # Make sure the iterator is drained.
        self.mock_backup.assert_called_once_with(self.mock_root_step)
        self.mock_sleep.assert_called_once_with(12)
        self.mock_api_socket.shutdown.assert_called_once_with(SHUT_RDWR)

    def test_when_step_state_is_not_transitioned(self):
        mock_state_transitions = dict(state_not_reached=self.mock_state_transition)
        trail_manager(self.mock_root_step, self.mock_api_socket, self.mock_backup, delay=12, context=self.mock_context,
                      state_transitions=mock_state_transitions)

        self.mock_topological_traverse.assert_called_once_with(self.mock_root_step)
        root_step = self.mock_topological_while.call_args[0][0]
        self.assertEqual(root_step, self.mock_root_step)
        self.assertEqual(self.mock_state_transition.call_count, 0)

        self.assertEqual(len(self.mock_serve_socket.mock_calls), 2)
        for mock_call in self.mock_serve_socket.mock_calls:
            socket, handler = mock_call[1]
            self.assertEqual(socket, self.mock_api_socket)
            mock_request = MagicMock()
            handler(mock_request)
            self.mock_handle_api_call.assert_called_with(mock_request, steps=[self.mock_step])

        self.assertEqual(list(self.step_iterator), []) # Make sure the iterator is drained.
        self.mock_backup.assert_called_once_with(self.mock_root_step)
        self.mock_sleep.assert_called_once_with(12)
        self.mock_api_socket.shutdown.assert_called_once_with(SHUT_RDWR)
