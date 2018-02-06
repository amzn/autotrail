"""Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.
"""

import socket
import sys
import unittest

from mock import MagicMock, patch, call
from Queue import Empty as QueueEmpty
from StringIO import StringIO

from autotrail.core.dag import Step, topological_traverse
from autotrail.layer1.api import APICallField, APICallName, StatusField
from autotrail.layer1.trail import trail_manager, make_dag
from autotrail.layer2.trail import (MatchTrailsException, TrailClient, TrailClientError, TrailServer, backup_trail,
                                    deserialize_trail, restore_trail, serialize_trail, sigint_resistant_function,
                                    wait_till_change)


class TestTrailServer(unittest.TestCase):
    def setUp(self):
        self.mock_process_object = MagicMock()
        self.process_patcher = patch('autotrail.layer2.trail.Process', return_value=self.mock_process_object)
        self.mock_process = self.process_patcher.start()
        self.mock_root_step = MagicMock()
        self.make_dag_patcher = patch('autotrail.layer2.trail.make_dag', return_value=self.mock_root_step)
        self.mock_make_dag = self.make_dag_patcher.start()
        self.mktemp_patcher = patch('autotrail.layer2.trail.mktemp', return_value='mock_temp_file')
        self.mock_mktemp = self.mktemp_patcher.start()
        self.mock_api_socket = MagicMock()
        self.socket_server_setup_patcher = patch('autotrail.layer2.trail.socket_server_setup',
                                                  return_value=self.mock_api_socket)
        self.mock_socket_server_setup = self.socket_server_setup_patcher.start()
        self.mock_trail_definition = MagicMock() # We don't have to provide a valid trail definition here.
        self.backup_trail_patcher = patch('autotrail.layer2.trail.backup_trail')
        self.mock_backup_trail = self.backup_trail_patcher.start()

    def tearDown(self):
        self.backup_trail_patcher.stop()
        self.socket_server_setup_patcher.stop()
        self.mktemp_patcher.stop()
        self.process_patcher.stop()
        self.make_dag_patcher.stop()

    def test_initialize_with_all_parameters(self):
        mock_socket_file = MagicMock()
        mock_dag_file = MagicMock()
        mock_context = MagicMock()
        mock_delay = MagicMock()
        mock_timeout = MagicMock()
        mock_backlog_count = MagicMock()

        trail_server = TrailServer(self.mock_trail_definition, socket_file=mock_socket_file,
                                        dag_file=mock_dag_file, context=mock_context, delay=mock_delay,
                                        timeout=mock_timeout, backlog_count=mock_backlog_count)

        self.assertEqual(trail_server.context, mock_context)
        self.mock_make_dag.assert_called_once_with(self.mock_trail_definition)
        self.assertEqual(trail_server.root_step, self.mock_root_step)
        self.assertFalse(trail_server.started)
        self.assertEqual(trail_server.timeout, mock_timeout)
        self.assertEqual(trail_server.backlog_count, mock_backlog_count)
        self.assertEqual(trail_server.trail_process, None)
        self.assertEqual(trail_server.delay, mock_delay)
        self.assertEqual(trail_server.dag_file, mock_dag_file)
        self.assertEqual(trail_server.socket_file, mock_socket_file)
        self.mock_socket_server_setup.assert_called_once_with(
            mock_socket_file, backlog_count=mock_backlog_count, timeout=mock_timeout)
        self.assertEqual(trail_server.api_socket, self.mock_api_socket)
        self.assertEqual(self.mock_mktemp.call_count, 0)

    def test_initialize_with_required_parameters(self):
        trail_server = TrailServer(self.mock_trail_definition)

        self.assertEqual(trail_server.context, None)
        self.mock_make_dag.assert_called_once_with(self.mock_trail_definition)
        self.assertEqual(trail_server.root_step, self.mock_root_step)
        self.assertFalse(trail_server.started)
        self.assertEqual(trail_server.timeout, 0.0001)
        self.assertEqual(trail_server.backlog_count, 1)
        self.assertEqual(trail_server.trail_process, None)
        self.assertEqual(trail_server.delay, 1)

        self.mock_mktemp.assert_any_call_with(prefix='autotrail.')
        self.assertEqual(trail_server.dag_file, 'mock_temp_file')
        self.mock_mktemp.assert_any_call_with(prefix='autotrail.socket.')
        self.assertEqual(trail_server.socket_file, 'mock_temp_file')

        self.mock_socket_server_setup.assert_called_once_with('mock_temp_file', backlog_count=1, timeout=0.0001)
        self.assertEqual(trail_server.api_socket, self.mock_api_socket)
        self.assertEqual(self.mock_mktemp.call_count, 2)

    def test_serve_threaded(self):
        mock_context = MagicMock()
        mock_delay = MagicMock()
        trail_server = TrailServer(self.mock_trail_definition, context=mock_context, delay=mock_delay)
        trail_server.serve(threaded=True)

        args = self.mock_process.call_args[0]
        kwargs = self.mock_process.call_args[1]

        self.assertEqual(args, tuple())
        self.assertEqual(kwargs['target'], sigint_resistant_function)
        args_for_sigint_resistant_function = kwargs['args']
        kwargs_for_sigint_resistant_function = kwargs['kwargs']
        passed_trail_manager, passed_root_step, passed_socket, passed_backup_function = \
            args_for_sigint_resistant_function
        self.assertEqual(passed_trail_manager, trail_manager)
        self.assertEqual(passed_root_step, self.mock_root_step)
        self.assertEqual(passed_socket, self.mock_api_socket)
        passed_backup_function(self.mock_root_step)
        self.mock_backup_trail.assert_called_once_with(self.mock_root_step, trail_server.dag_file)

        self.assertEqual(kwargs_for_sigint_resistant_function, dict(context=mock_context, delay=mock_delay))

        self.mock_process_object.start.assert_called_once_with()

    def test_serve_not_threaded(self):
        trail_manager_patcher = patch('autotrail.layer2.trail.trail_manager')
        mock_trail_manager = trail_manager_patcher.start()

        mock_context = MagicMock()
        mock_delay = MagicMock()
        trail_server = TrailServer(self.mock_trail_definition, context=mock_context, delay=mock_delay)
        trail_server.serve()

        root_step, api_socket, backup_function = mock_trail_manager.call_args[0]
        self.assertEqual(root_step, self.mock_root_step)
        self.assertEqual(api_socket, self.mock_api_socket)
        backup_function(self.mock_root_step)
        self.mock_backup_trail.assert_called_once_with(self.mock_root_step, trail_server.dag_file)

        kwargs = mock_trail_manager.call_args[1]
        self.assertEqual(kwargs, dict(context=mock_context, delay=mock_delay))

        trail_manager_patcher.stop()

    def test_restore_trail(self):
        restore_trail_patcher = patch('autotrail.layer2.trail.restore_trail')
        mock_restore_trail = restore_trail_patcher.start()

        trail_server = TrailServer(self.mock_trail_definition)
        trail_server.restore_trail('mock_dag_file')

        mock_restore_trail.assert_called_once_with(self.mock_root_step, 'mock_dag_file',
                                                   {Step.BLOCKED: Step.PAUSED, Step.RUN: Step.PAUSED})

        restore_trail_patcher.stop()


class TestTrailClientInternalMethods(unittest.TestCase):
    def setUp(self):
        self.socket_file = MagicMock()
        self.trail_client = TrailClient(self.socket_file)

    def test__make_api_call(self):
        mock_response = {'result': 'mock_result', 'error': None}
        send_request_patcher = patch('autotrail.layer2.trail.send_request', return_value=mock_response)
        mock_send_request = send_request_patcher.start()
        mock_api_call = MagicMock()

        result = self.trail_client._make_api_call(mock_api_call)

        mock_send_request.assert_called_once_with(self.socket_file, mock_api_call)
        self.assertEqual(result, mock_response['result'])

        send_request_patcher.stop()

    def test__make_api_call_with_error_in_response(self):
        mock_response = {'result': None, 'error': 'mock_error'}
        send_request_patcher = patch('autotrail.layer2.trail.send_request', return_value=mock_response)
        mock_send_request = send_request_patcher.start()
        mock_api_call = MagicMock()

        with self.assertRaises(TrailClientError) as e:
            result = self.trail_client._make_api_call(mock_api_call)
            self.assertEqual(str(e), 'mock_error')

        mock_send_request.assert_called_once_with(self.socket_file, mock_api_call)

        send_request_patcher.stop()

    def test__make_api_call_with_socket_error(self):
        send_request_patcher = patch('autotrail.layer2.trail.send_request', side_effect=socket.error)
        mock_send_request = send_request_patcher.start()
        mock_api_call = MagicMock()

        result = self.trail_client._make_api_call(mock_api_call)

        mock_send_request.assert_called_once_with(self.socket_file, mock_api_call)
        self.assertEqual(result, None)

        send_request_patcher.stop()


class TestTrailClient(unittest.TestCase):
    def setUp(self):
        self.socket_file = MagicMock()
        self.trail_client = TrailClient(self.socket_file)
        self.mock_result = MagicMock()
        self.trail_client._make_api_call = MagicMock(return_value=self.mock_result)
        self.mock_dry_run = MagicMock()
        self.mock_tags = dict(mock_tag_key='mock_tag_value')

    def test_start_stepped(self):
        result = self.trail_client.start()

        self.trail_client._make_api_call.assert_called_once_with({
            APICallField.NAME: APICallName.START,
            APICallField.DRY_RUN: False})
        self.assertEqual(result, self.mock_result)

    def test_stop(self):
        self.trail_client.interrupt = MagicMock()
        self.trail_client.block = MagicMock(return_value='mock_result')

        result = self.trail_client.stop()

        self.trail_client.interrupt.assert_not_called()
        self.trail_client.block.assert_called_once_with(dry_run=False)
        self.assertEqual(result, 'mock_result')

    def test_stop_with_interrupt(self):
        self.trail_client.interrupt = MagicMock()
        self.trail_client.block = MagicMock(return_value='mock_result')

        result = self.trail_client.stop(interrupt=True)

        self.trail_client.interrupt.assert_called_once_with(dry_run=False)
        self.trail_client.block.assert_called_once_with(dry_run=False)
        self.assertEqual(result, 'mock_result')

    def test_shutdown(self):
        result = self.trail_client.shutdown(dry_run=self.mock_dry_run)

        self.trail_client._make_api_call.assert_called_once_with({
            APICallField.NAME: APICallName.SHUTDOWN,
            APICallField.DRY_RUN: self.mock_dry_run})
        self.assertEqual(result, self.mock_result)

    def test_next_step_with_defaults(self):
        result = self.trail_client.next_step()

        self.trail_client._make_api_call.assert_called_once_with({
            APICallField.NAME: APICallName.NEXT_STEPS,
            APICallField.STEP_COUNT: 1,
            APICallField.DRY_RUN: True})
        self.assertEqual(result, self.mock_result)

    def test_next_step_with_count_and_no_dry_run(self):
        result = self.trail_client.next_step(step_count=5, dry_run=self.mock_dry_run)

        self.trail_client._make_api_call.assert_called_once_with({
            APICallField.NAME: APICallName.NEXT_STEPS,
            APICallField.STEP_COUNT: 5,
            APICallField.DRY_RUN: self.mock_dry_run})
        self.assertEqual(result, self.mock_result)

    def test_send_message_to_steps(self):
        result = self.trail_client.send_message_to_steps('mock_message', dry_run=self.mock_dry_run, **self.mock_tags)

        self.trail_client._make_api_call.assert_called_once_with({
            APICallField.NAME: APICallName.SEND_MESSAGE_TO_STEPS,
            APICallField.DRY_RUN: self.mock_dry_run,
            APICallField.MESSAGE: 'mock_message',
            APICallField.TAGS: self.mock_tags})
        self.assertEqual(result, self.mock_result)

    def test_status(self):
        mock_fields = MagicMock()
        mock_states = MagicMock()
        result = self.trail_client.status(fields=mock_fields, states=mock_states, **self.mock_tags)

        self.trail_client._make_api_call.assert_called_once_with({
            APICallField.NAME: APICallName.STATUS,
            APICallField.STATUS_FIELDS: mock_fields,
            APICallField.STATES: mock_states,
            APICallField.TAGS: self.mock_tags})
        self.assertEqual(result, self.mock_result)

    def test_status_with_default_args(self):
        result = self.trail_client.status()

        self.trail_client._make_api_call.assert_called_once_with({
            APICallField.NAME: APICallName.STATUS,
            APICallField.STATUS_FIELDS: [StatusField.STATE, StatusField.UNREPLIED_PROMPT_MESSAGE,
                                         StatusField.OUTPUT_MESSAGES, StatusField.RETURN_VALUE],
            APICallField.STATES: [],
            APICallField.TAGS: {}})
        self.assertEqual(result, self.mock_result)

    def test_steps_waiting_for_user_input(self):
        statuses = [
            {'n': 0, 'name': 'waiting_for_user', StatusField.UNREPLIED_PROMPT_MESSAGE: 'message_to_user'},
            {'n': 1, 'name': 'not_waiting_for_user', StatusField.UNREPLIED_PROMPT_MESSAGE: None},
        ]
        self.trail_client.status = MagicMock(return_value=statuses)

        result = self.trail_client.steps_waiting_for_user_input()

        self.trail_client.status.assert_called_once_with(
            fields=[StatusField.UNREPLIED_PROMPT_MESSAGE], states=[Step.RUN])
        self.assertEqual(
            result,
            [{'n': 0, 'name': 'waiting_for_user', StatusField.UNREPLIED_PROMPT_MESSAGE: 'message_to_user'}])

    def test_steps_waiting_for_user_input_with_no_result(self):
        self.trail_client.status = MagicMock(return_value=None)

        result = self.trail_client.steps_waiting_for_user_input()

        self.trail_client.status.assert_called_once_with(
            fields=[StatusField.UNREPLIED_PROMPT_MESSAGE], states=[Step.RUN])
        self.assertEqual(result, None)

    def test_pause(self):
        result = self.trail_client.pause(dry_run=self.mock_dry_run, **self.mock_tags)

        self.trail_client._make_api_call.assert_called_once_with({
            APICallField.NAME: APICallName.PAUSE,
            APICallField.DRY_RUN: self.mock_dry_run,
            APICallField.TAGS: self.mock_tags})
        self.assertEqual(result, self.mock_result)

    def test_pause_branch(self):
        result = self.trail_client.pause_branch(dry_run=self.mock_dry_run, **self.mock_tags)

        self.trail_client._make_api_call.assert_called_once_with({
            APICallField.NAME: APICallName.PAUSE_BRANCH,
            APICallField.DRY_RUN: self.mock_dry_run,
            APICallField.TAGS: self.mock_tags})
        self.assertEqual(result, self.mock_result)

    def test_set_pause_on_fail(self):
        result = self.trail_client.set_pause_on_fail(dry_run=self.mock_dry_run, **self.mock_tags)

        self.trail_client._make_api_call.assert_called_once_with({
            APICallField.NAME: APICallName.SET_PAUSE_ON_FAIL,
            APICallField.DRY_RUN: self.mock_dry_run,
            APICallField.TAGS: self.mock_tags})
        self.assertEqual(result, self.mock_result)

    def test_unset_pause_on_fail(self):
        result = self.trail_client.unset_pause_on_fail(dry_run=self.mock_dry_run, **self.mock_tags)

        self.trail_client._make_api_call.assert_called_once_with({
            APICallField.NAME: APICallName.UNSET_PAUSE_ON_FAIL,
            APICallField.DRY_RUN: self.mock_dry_run,
            APICallField.TAGS: self.mock_tags})
        self.assertEqual(result, self.mock_result)

    def test_interrupt(self):
        result = self.trail_client.interrupt(dry_run=self.mock_dry_run, **self.mock_tags)

        self.trail_client._make_api_call.assert_called_once_with({
            APICallField.NAME: APICallName.INTERRUPT,
            APICallField.DRY_RUN: self.mock_dry_run,
            APICallField.TAGS: self.mock_tags})
        self.assertEqual(result, self.mock_result)

    def test_resume(self):
        result = self.trail_client.resume(dry_run=self.mock_dry_run, **self.mock_tags)

        self.trail_client._make_api_call.assert_called_once_with({
            APICallField.NAME: APICallName.RESUME,
            APICallField.DRY_RUN: self.mock_dry_run,
            APICallField.TAGS: self.mock_tags})
        self.assertEqual(result, self.mock_result)

    def test_resume_branch(self):
        result = self.trail_client.resume_branch(dry_run=self.mock_dry_run, **self.mock_tags)

        self.trail_client._make_api_call.assert_called_once_with({
            APICallField.NAME: APICallName.RESUME_BRANCH,
            APICallField.DRY_RUN: self.mock_dry_run,
            APICallField.TAGS: self.mock_tags})
        self.assertEqual(result, self.mock_result)

    def test_list(self):
        result = self.trail_client.list(**self.mock_tags)

        self.trail_client._make_api_call.assert_called_once_with({
            APICallField.NAME: APICallName.LIST,
            APICallField.TAGS: self.mock_tags})
        self.assertEqual(result, self.mock_result)

    def test_skip(self):
        result = self.trail_client.skip(dry_run=self.mock_dry_run, **self.mock_tags)

        self.trail_client._make_api_call.assert_called_once_with({
            APICallField.NAME: APICallName.SKIP,
            APICallField.DRY_RUN: self.mock_dry_run,
            APICallField.TAGS: self.mock_tags})
        self.assertEqual(result, self.mock_result)

    def test_unskip(self):
        result = self.trail_client.unskip(dry_run=self.mock_dry_run, **self.mock_tags)

        self.trail_client._make_api_call.assert_called_once_with({
            APICallField.NAME: APICallName.UNSKIP,
            APICallField.DRY_RUN: self.mock_dry_run,
            APICallField.TAGS: self.mock_tags})
        self.assertEqual(result, self.mock_result)

    def test_block(self):
        result = self.trail_client.block(dry_run=self.mock_dry_run, **self.mock_tags)

        self.trail_client._make_api_call.assert_called_once_with({
            APICallField.NAME: APICallName.BLOCK,
            APICallField.DRY_RUN: self.mock_dry_run,
            APICallField.TAGS: self.mock_tags})
        self.assertEqual(result, self.mock_result)

    def test_unblock(self):
        result = self.trail_client.unblock(dry_run=self.mock_dry_run, **self.mock_tags)

        self.trail_client._make_api_call.assert_called_once_with({
            APICallField.NAME: APICallName.UNBLOCK,
            APICallField.DRY_RUN: self.mock_dry_run,
            APICallField.TAGS: self.mock_tags})
        self.assertEqual(result, self.mock_result)


class TestTrailBackupRestore(unittest.TestCase):
    def setUp(self):
        # Make this DAG:
        #           +--> step_b -->+
        # step_a -->|              |--> step_d
        #           +--> step_c -->+
        def action_a():
            pass

        def action_b():
            pass

        def action_c():
            pass

        def action_d():
            pass

        self.step_a = Step(action_a)
        self.step_b = Step(action_b)
        self.step_c = Step(action_c)
        self.step_d = Step(action_d)

        trail_definition = [
            (self.step_a, self.step_b),
            (self.step_a, self.step_c),
            (self.step_b, self.step_d),
            (self.step_c, self.step_d),
        ]

        self.root_step = make_dag(trail_definition)
        self.step_a.return_value = 'mock return value'
        self.step_a.prompt_messages = ['mock prompt_messages']
        self.step_a.output_messages = ['mock output_messages']
        self.step_a.input_messages = ['mock input_messages']

        self.trail_data = serialize_trail(self.root_step)

    def test_deserialize_trail(self):
        self.assertEqual(deserialize_trail(self.root_step, self.trail_data, {}), None)

    def test_deserialize_trail_missing_step(self):
        del(self.trail_data[str(self.step_d)])
        with self.assertRaises(MatchTrailsException):
            self.assertTrue(deserialize_trail(self.root_step, self.trail_data, {}))

    def test_deserialize_trail_mismatched_trail(self):
        # Make another DAG:
        #           +--> step_d -->+
        # step_a -->|              |--> step_b
        #           +--> step_c -->+
        def action_a():
            pass

        def action_b():
            pass

        def action_c():
            pass

        def action_d():
            pass

        step_a = Step(action_a)
        step_b = Step(action_b)
        step_c = Step(action_c)
        step_d = Step(action_d)

        trail_definition = [
            (step_a, step_d),
            (step_a, step_c),
            (step_d, step_b),
            (step_c, step_b),
        ]

        mismatching_trail_data = serialize_trail(make_dag(trail_definition))

        with self.assertRaises(MatchTrailsException):
            deserialize_trail(self.root_step, mismatching_trail_data, {})

    def test_deserialize_trail_load_all_attributes(self):
        self.step_a.state = self.step_a.SUCCESS
        self.step_a.return_value = 'mock return value 2'
        self.step_a.prompt_messages = ['mock prompt_messages 2']
        self.step_a.output_messages = ['mock output_messages 2']
        self.step_a.input_messages = ['mock input_messages 2']
        deserialize_trail(self.root_step, self.trail_data, {})
        self.assertEqual(self.step_a.state, self.step_a.READY)
        self.assertEqual(self.step_a.return_value, 'mock return value')
        self.assertEqual(self.step_a.prompt_messages, ['mock prompt_messages'])
        self.assertEqual(self.step_a.output_messages, ['mock output_messages'])
        self.assertEqual(self.step_a.input_messages, ['mock input_messages'])

    def test_deserialize_trail_with_state_transformation_mapping(self):
        self.trail_data[str(self.step_a)][StatusField.STATE] = Step.BLOCKED
        deserialize_trail(self.root_step, self.trail_data, {Step.BLOCKED: Step.PAUSED})
        self.assertEqual(self.step_a.state, self.step_a.PAUSED)

    def test_restore_trail(self):
        open_patcher = patch('__builtin__.open')
        mock_open = open_patcher.start()
        json_load_patcher = patch('autotrail.layer2.trail.json.load', return_value='mock_trail_data')
        mock_json_load = json_load_patcher.start()
        deserialize_trail_patcher = patch('autotrail.layer2.trail.deserialize_trail')
        mock_deserialize_trail = deserialize_trail_patcher.start()

        restore_trail(self.step_a, 'mock_file', {Step.BLOCKED: Step.PAUSED})

        self.assertEqual(mock_open.mock_calls,
                         [call('mock_file', 'r'), call().__enter__(), call().__exit__(None, None, None)])
        mock_json_load.assert_called_once_with(mock_open().__enter__())
        mock_deserialize_trail.assert_called_once_with(self.step_a, 'mock_trail_data', {Step.BLOCKED: Step.PAUSED})

        deserialize_trail_patcher.stop()
        json_load_patcher.stop()
        open_patcher.stop()

    def test_backup_trail(self):
        open_patcher = patch('__builtin__.open')
        mock_open = open_patcher.start()
        json_dump_patcher = patch('autotrail.layer2.trail.json.dump')
        mock_json_dump = json_dump_patcher.start()

        backup_trail(self.root_step, 'foo')

        mock_open.assert_called_once_with('foo', 'w')
        mock_open().__exit__.assert_called_once_with(None, None, None)
        mock_json_dump.assert_called_once_with(serialize_trail(self.root_step), mock_open().__enter__())

        json_dump_patcher.stop()
        open_patcher.stop()

    def test_serialize_trail(self):
        # Make this DAG:
        #           +--> step_b -->+
        # step_a -->|              |--> step_d
        #           +--> step_c -->+
        def action_a():
            pass

        def action_b():
            pass

        def action_c():
            pass

        def action_d():
            pass

        step_a = Step(action_a)
        step_b = Step(action_b)
        step_c = Step(action_c)
        step_d = Step(action_d)

        trail_definition = [
            (step_a, step_b),
            (step_a, step_c),
            (step_b, step_d),
            (step_c, step_d),
        ]

        root_step = make_dag(trail_definition)
        trail_data = serialize_trail(root_step)

        for step in [step_a, step_b, step_c, step_d]:
            self.assertIn(str(step), trail_data)
            self.assertEqual(trail_data[str(step)][StatusField.STATE], str(step.state))
            self.assertEqual(trail_data[str(step)][StatusField.RETURN_VALUE], str(step.return_value))
            for parent in step.parents:
                self.assertIn(str(parent), trail_data[str(step)]['parents'])


class TestFunctions(unittest.TestCase):
    def test_sigint_resistant_function(self):
        signal_patcher = patch('autotrail.layer2.trail.signal')
        mock_signal = signal_patcher.start()
        mock_function = MagicMock()

        sigint_resistant_function(mock_function, 'arg', 1, True, mock_kwarg_1='mock_value_1')

        mock_signal.signal.assert_called_once_with(mock_signal.SIGINT, mock_signal.SIG_IGN)
        mock_function.assert_called_once_with('arg', 1, True, mock_kwarg_1='mock_value_1')

        signal_patcher.stop()

    def test_wait_till_change(self):
        func = MagicMock(side_effect=[1, 1, 2, 2, 2, 3, 4, 4])
        predicate = MagicMock(return_value=True)
        validate = MagicMock(return_value=True)

        values = [value for value in wait_till_change(func, predicate, validate, delay=0.01, max_tries=99)]

        self.assertEqual(values, [2, 3, 4])
        self.assertEqual(func.call_count, 9)
        self.assertEqual(predicate.call_count, 8)
        self.assertEqual(validate.call_count, 7)
        self.assertEqual(validate.mock_calls, [call(1), call(2), call(2), call(2), call(3), call(4), call(4)])

    def test_wait_till_change_max_tries_exhausted(self):
        func = MagicMock(return_value='foo')
        predicate = MagicMock(return_value=True)
        validate = MagicMock(return_value=True)

        values = [value for value in wait_till_change(func, predicate, validate, delay=0.01, max_tries=2)]

        self.assertEqual(values, [])
        self.assertEqual(func.call_count, 3)
        self.assertEqual(predicate.call_count, 2)
        self.assertEqual(validate.call_count, 2)
        self.assertEqual(validate.mock_calls, [call('foo'), call('foo')])

    def test_wait_till_change_predicate_fails(self):
        func = MagicMock(return_value='foo')
        predicate = MagicMock(return_value=False)
        validate = MagicMock(return_value=True)

        values = [value for value in wait_till_change(func, predicate, validate, delay=0.01, max_tries=5)]

        self.assertEqual(values, [])
        self.assertEqual(func.call_count, 1)
        self.assertEqual(predicate.call_count, 1)
        self.assertEqual(validate.call_count, 0)

    def test_wait_till_change_validate_fails(self):
        func = MagicMock(return_value='foo')
        predicate = MagicMock(return_value=True)
        validate = MagicMock(return_value=False)

        values = [value for value in wait_till_change(func, predicate, validate, delay=0.01, max_tries=5)]

        self.assertEqual(values, [])
        self.assertEqual(func.call_count, 6)
        self.assertEqual(predicate.call_count, 5)
        self.assertEqual(validate.call_count, 5)
        self.assertEqual(validate.mock_calls, [call('foo'), call('foo'), call('foo'), call('foo'), call('foo')])

    def test_wait_till_change_max_tries_is_negative(self):
        func = MagicMock()
        predicate = MagicMock(return_value=True)
        validate = MagicMock(return_value=True)

        values = [value for value in wait_till_change(func, predicate, validate, delay=0.01, max_tries=-1)]

        self.assertEqual(values, [])
        self.assertEqual(func.call_count, 1)
        self.assertEqual(predicate.call_count, 0)
        self.assertEqual(validate.call_count, 0)
