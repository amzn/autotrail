"""Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.
"""

import unittest

from mock import MagicMock, patch

from six import StringIO

from autotrail.core.dag import Step
from autotrail.helpers.io import (
    create_namespaces_for_tag_keys_and_values,
    InteractiveTrail,
    Names,
    print_affected_steps,
    print_api_result,
    print_error,
    print_step_statuses,
    print_step_list,
    print_no_result,
    write_trail_definition_as_dot)
from autotrail.layer1.api import APICallName, StatusField


class TestFunctions(unittest.TestCase):
    def test_write_trail_definition_as_dot(self):
        # Make this DAG:
        # step_a --> step_b
        def a():
            pass

        def b():
            pass

        step_a = Step(a)
        step_b = Step(b)

        trail_definition = [(step_a, step_b)]

        dot_file = StringIO()
        write_trail_definition_as_dot(trail_definition, to=dot_file)

        self.assertEqual(dot_file.getvalue(), 'digraph trail {\nnode [style=rounded, shape=box];\n"a" -> "b";\n}\n')


class TestInteractiveTrailHelperFunctions(unittest.TestCase):
    def test_print_step_statuses(self):
        step_status = {
            StatusField.N: 0,
            StatusField.NAME: 'test_step',
            StatusField.RETURN_VALUE: 'test_return_value',
            StatusField.OUTPUT_MESSAGES: ['output message 1', 'output message 2'],
            StatusField.PROMPT_MESSAGES: ['prompt message 1', 'prompt message 2'],
            StatusField.UNREPLIED_PROMPT_MESSAGE: 'prompt message 2',
        }
        output = StringIO()

        print_step_statuses([step_status], output)

        # The following output should be produced (Excluding the space after the '#'):
        # - Name                : test_step
        #   n                   : 0
        #   Prompt messages     : prompt message 1
        #                       : prompt message 2
        #   Return value        : test_return_value
        #   Waiting for response: prompt message 2
        #   Output messages     : output message 1
        #                       : output message 2
        self.assertIn('- Name                : test_step', output.getvalue())
        self.assertIn('  n                   : 0', output.getvalue())
        self.assertIn('  Prompt messages     : prompt message 1', output.getvalue())
        self.assertIn('                      : prompt message 2', output.getvalue())
        self.assertIn('  Return value        : test_return_value', output.getvalue())
        self.assertIn('  Waiting for response: prompt message 2', output.getvalue())
        self.assertIn('  Output messages     : output message 1', output.getvalue())
        self.assertIn('                      : output message 2', output.getvalue())

    def test_print_api_result(self):
        output = StringIO()

        print_api_result('mock_method', 'mock_result', output)

        # The following output should be printed (Excluding the space after the '#'):
        # - API Call: mock_method
        #   Result: mock_result
        self.assertIn('- API Call: mock_method', output.getvalue())
        self.assertIn('  Result: mock_result', output.getvalue())

    def test_print_affected_steps_with_dry_run(self):
        output = StringIO()
        step_tags = [{'name':'step_1', 'n': 0}, {'name':'step_2', 'n': 1}]

        print_affected_steps(APICallName.PAUSE, step_tags, True, output)

        # The following output should be printed (Excluding the space after the '#'):
        # - API Call: Pause trail
        #   Mode: Dry Run
        #   List of step names that are affected:
        #     # 0 - step_1
        #     # 1 - step_2
        self.assertIn('- API Call: pause', output.getvalue())
        self.assertIn('  Mode: Dry Run', output.getvalue())
        self.assertIn('  List of step names that are affected:', output.getvalue())
        self.assertIn('    # 0 - step_1', output.getvalue())
        self.assertIn('    # 1 - step_2', output.getvalue())

    def test_print_affected_steps_without_dry_run(self):
        output = StringIO()
        step_tags = [{'name':'step_1', 'n': 0}, {'name':'step_2', 'n': 1}]

        print_affected_steps(APICallName.PAUSE, step_tags, False, output)

        # The following output should be printed (Excluding the space after the '#'):
        # - API Call: Pause trail
        #   Mode: Actual Run
        #   List of step names that are affected:
        #     # 0 - step_1
        #     # 1 - step_2
        self.assertIn('- API Call: pause', output.getvalue())
        self.assertIn('  Mode: Actual Run', output.getvalue())
        self.assertIn('  List of step names that are affected:', output.getvalue())
        self.assertIn('    # 0 - step_1', output.getvalue())
        self.assertIn('    # 1 - step_2', output.getvalue())

    def test_print_step_list(self):
        output = StringIO()

        print_step_list([dict(name='step_1', n=0), dict(name='step_2', n=1, foo='bar')], to=output)

        # The following output should be printed (Excluding the space after the '#'):
        # Exception: List of steps and their tags:
        # - Name: step_1
        #   n: 0
        # - Name: step_2
        #   n: 1
        #   foo: bar
        self.assertIn('- Name: step_1', output.getvalue())
        self.assertIn('  n: 0', output.getvalue())
        self.assertIn('- Name: step_2', output.getvalue())
        self.assertIn('  n: 1', output.getvalue())
        self.assertIn('  foo: bar', output.getvalue())

    def test_print_error(self):
        output = StringIO()

        print_error('mock_method', 'mock_error', to=output)

        # The following output should be printed (Excluding the space after the '#'):
        # - API Call: mock_method
        #   Error: mock_error
        self.assertIn('- API Call: mock_method', output.getvalue())
        self.assertIn('  Error: mock_error', output.getvalue())

    def test_print_no_result(self):
        output = StringIO()

        print_no_result('mock_method', to=output)

        # The following output should be printed (Excluding the space after the '#'):
        # - API Call: mock_method
        #   No result received for the API call.
        self.assertIn('- API Call: mock_method', output.getvalue())
        self.assertIn('  No result received for the API call.', output.getvalue())


class TestInteractiveTrailInternalMethods(unittest.TestCase):
    def setUp(self):
        self.mock_stdout = MagicMock()
        self.mock_stderr = MagicMock()
        self.mock_status_printer = MagicMock()
        self.mock_affected_steps_printer = MagicMock()
        self.mock_step_list_printer = MagicMock()
        self.mock_error_printer = MagicMock()
        self.mock_no_result_printer = MagicMock()
        self.mock_generic_printer = MagicMock()
        self.mock_trail_client = MagicMock()

        self.interactive_trail = InteractiveTrail(self.mock_trail_client, stdout=self.mock_stdout,
                                                  stderr=self.mock_stderr,
                                                  status_printer=self.mock_status_printer,
                                                  affected_steps_printer=self.mock_affected_steps_printer,
                                                  step_list_printer=self.mock_step_list_printer,
                                                  error_printer=self.mock_error_printer,
                                                  no_result_printer=self.mock_no_result_printer,
                                                  generic_printer=self.mock_generic_printer)

    def test_instance(self):
        self.assertEqual(self.interactive_trail.trail_client, self.mock_trail_client)
        self.assertEqual(self.interactive_trail.stdout, self.mock_stdout)
        self.assertEqual(self.interactive_trail.stderr, self.mock_stderr)
        self.assertEqual(self.interactive_trail.error_printer, self.mock_error_printer)
        self.assertEqual(self.interactive_trail.no_result_printer, self.mock_no_result_printer)
        self.assertEqual(self.interactive_trail.status_printer, self.mock_status_printer)
        self.assertEqual(self.interactive_trail.affected_steps_printer, self.mock_affected_steps_printer)
        self.assertEqual(self.interactive_trail.step_list_printer, self.mock_step_list_printer)
        self.assertEqual(self.interactive_trail.generic_printer, self.mock_generic_printer)

    def test_call_trail_client_method(self):
        self.mock_trail_client.mock_method = MagicMock(return_value='mock_result')
        mock_printer = MagicMock()
        mock_arg = 'mock_arg'
        mock_kwargs = dict(mock_key='mock_value')

        self.interactive_trail._call_trail_client_method('mock_method', mock_printer, mock_arg, **mock_kwargs)

        self.mock_trail_client.mock_method.assert_called_once_with(mock_arg, **mock_kwargs)
        mock_printer.assert_called_once_with('mock_result')
        self.assertEqual(self.mock_error_printer.call_count, 0)
        self.assertEqual(self.mock_no_result_printer.call_count, 0)
        self.assertEqual(self.mock_stderr.call_count, 0)

    def test_call_trail_client_method_raises_type_error(self):
        self.mock_trail_client.mock_method = MagicMock(side_effect=TypeError('mock_type_error'))
        mock_printer = MagicMock()
        mock_arg = 'mock_arg'
        mock_kwargs = dict(mock_key='mock_value')

        self.interactive_trail._call_trail_client_method('mock_method', mock_printer, mock_arg, **mock_kwargs)

        self.mock_trail_client.mock_method.assert_called_once_with(mock_arg, **mock_kwargs)
        self.assertEqual(mock_printer.call_count, 0)
        self.mock_error_printer.assert_called_once_with('mock_method', 'mock_type_error', self.mock_stderr)
        self.assertEqual(self.mock_no_result_printer.call_count, 0)

    def test_call_trail_client_method_raises_value_error(self):
        self.mock_trail_client.mock_method = MagicMock(side_effect=ValueError('mock_value_error'))
        mock_printer = MagicMock()
        mock_arg = 'mock_arg'
        mock_kwargs = dict(mock_key='mock_value')

        self.interactive_trail._call_trail_client_method('mock_method', mock_printer, mock_arg, **mock_kwargs)

        self.mock_trail_client.mock_method.assert_called_once_with(mock_arg, **mock_kwargs)
        self.assertEqual(mock_printer.call_count, 0)
        self.mock_error_printer.assert_called_once_with('mock_method', 'mock_value_error', self.mock_stderr)
        self.assertEqual(self.mock_no_result_printer.call_count, 0)

    def test_call_trail_client_method_when_no_result_is_returned(self):
        self.mock_trail_client.mock_method = MagicMock(return_value=None)
        mock_printer = MagicMock()
        mock_arg = 'mock_arg'
        mock_kwargs = dict(mock_key='mock_value')

        self.interactive_trail._call_trail_client_method('mock_method', mock_printer, mock_arg, **mock_kwargs)

        self.mock_trail_client.mock_method.assert_called_once_with(mock_arg, **mock_kwargs)
        self.assertEqual(mock_printer.call_count, 0)
        self.assertEqual(self.mock_error_printer.call_count, 0)
        self.mock_no_result_printer.assert_called_once_with('mock_method', self.mock_stderr)
        self.assertEqual(self.mock_stderr.call_count, 0)

    def test_call_trail_client_method_when_error_is_returned(self):
        self.mock_trail_client.mock_method = MagicMock(side_effect=Exception('mock_error'))
        mock_printer = MagicMock()
        mock_arg = 'mock_arg'
        mock_kwargs = dict(mock_key='mock_value')

        self.interactive_trail._call_trail_client_method('mock_method', mock_printer, mock_arg, **mock_kwargs)

        self.mock_trail_client.mock_method.assert_called_once_with(mock_arg, **mock_kwargs)
        self.assertEqual(mock_printer.call_count, 0)
        self.mock_error_printer.assert_called_once_with('mock_method', 'mock_error', self.mock_stderr)
        self.assertEqual(self.mock_no_result_printer.call_count, 0)
        self.assertEqual(self.mock_stderr.call_count, 0)


class TestInteractiveTrail(unittest.TestCase):
    def setUp(self):
        self.mock_stdout = MagicMock()
        self.mock_stderr = MagicMock()
        self.mock_status_printer = MagicMock()
        self.mock_affected_steps_printer = MagicMock()
        self.mock_step_list_printer = MagicMock()
        self.mock_error_printer = MagicMock()
        self.mock_no_result_printer = MagicMock()
        self.mock_generic_printer = MagicMock()
        self.mock_trail_client = MagicMock()
        self.mock_call_trail_client_method = MagicMock()

        self.interactive_trail = InteractiveTrail(self.mock_trail_client, stdout=self.mock_stdout,
                                                  stderr=self.mock_stderr,
                                                  status_printer=self.mock_status_printer,
                                                  affected_steps_printer=self.mock_affected_steps_printer,
                                                  step_list_printer=self.mock_step_list_printer,
                                                  error_printer=self.mock_error_printer,
                                                  no_result_printer=self.mock_no_result_printer,
                                                  generic_printer=self.mock_generic_printer)
        self.interactive_trail._call_trail_client_method = self.mock_call_trail_client_method

    def test_start(self):
        self.interactive_trail.start()

        (method, printer), kwargs = self.mock_call_trail_client_method.call_args
        self.assertEqual(method, 'start')
        printer('mock_result')
        self.assertEqual(kwargs, {})
        self.mock_affected_steps_printer.assert_called_once_with('start', 'mock_result', False, self.mock_stdout)

    def test_shutdown(self):
        self.interactive_trail.shutdown(dry_run=False)

        (method, printer), kwargs = self.mock_call_trail_client_method.call_args
        self.assertEqual(method, 'shutdown')
        printer('mock_result')
        self.assertEqual(kwargs, dict(dry_run=False))
        self.mock_generic_printer.assert_called_once_with('shutdown', 'mock_result', self.mock_stdout)

    def test_stop(self):
        self.interactive_trail.stop(dry_run=False)

        (method, printer), kwargs = self.mock_call_trail_client_method.call_args
        self.assertEqual(method, 'stop')
        self.assertEqual(kwargs, dict(dry_run=False))
        printer('mock_result')
        self.mock_affected_steps_printer.assert_called_once_with('stop', 'mock_result', False, self.mock_stdout)

    def test_next_step(self):
        self.interactive_trail.next_step(step_count=7, dry_run=False)

        (method, printer), kwargs = self.mock_call_trail_client_method.call_args
        self.assertEqual(method, 'next_step')
        self.assertEqual(kwargs, dict(step_count=7, dry_run=False))
        printer('mock_result')
        self.mock_affected_steps_printer.assert_called_once_with('next_step', 'mock_result', False, self.mock_stdout)

    def test_send_message_to_steps(self):
        self.interactive_trail.send_message_to_steps('mock message', dry_run=False, foo='bar')

        (method, printer, message), kwargs = self.mock_call_trail_client_method.call_args
        self.assertEqual(method, 'send_message_to_steps')
        self.assertEqual(message, 'mock message')
        self.assertEqual(kwargs, dict(foo='bar', dry_run=False))
        printer('mock_result')
        self.mock_affected_steps_printer.assert_called_once_with('send_message_to_steps', 'mock_result', False,
                                                                 self.mock_stdout)

    def test_status(self):
        self.interactive_trail.status(fields=['field'], states=['state'], foo='bar')

        (method, printer), kwargs = self.mock_call_trail_client_method.call_args
        self.assertEqual(method, 'status')
        self.assertEqual(kwargs, dict(foo='bar', fields=['field'], states=['state']))
        printer('mock_result')
        self.mock_status_printer.assert_called_once_with('mock_result', self.mock_stdout)

    def test_steps_waiting_for_user_input(self):
        self.interactive_trail.steps_waiting_for_user_input(foo='bar')

        (method, printer), kwargs = self.mock_call_trail_client_method.call_args
        self.assertEqual(method, 'steps_waiting_for_user_input')
        self.assertEqual(kwargs, dict(foo='bar'))
        printer('mock_result')
        self.mock_status_printer.assert_called_once_with('mock_result', self.mock_stdout)

    def test_list(self):
        self.interactive_trail.list(foo='bar')

        (method, printer), kwargs = self.mock_call_trail_client_method.call_args
        self.assertEqual(method, 'list')
        self.assertEqual(kwargs, dict(foo='bar'))
        printer('mock_result')
        self.mock_step_list_printer.assert_called_once_with('mock_result', self.mock_stdout)

    def test_all_other_methods(self):
        method_list = [
            'pause',
            'pause_branch',
            'set_pause_on_fail',
            'unset_pause_on_fail',
            'interrupt',
            'resume',
            'resume_branch',
            'skip',
            'unskip',
            'block',
            'unblock',
        ]

        for method in method_list:
            getattr(self.interactive_trail, method)(foo='bar', dry_run=False)
            (called_method, printer), kwargs = self.mock_call_trail_client_method.call_args
            self.assertEqual(called_method, method)
            self.assertEqual(kwargs, dict(foo='bar', dry_run=False))
            printer('mock_result')
            self.mock_affected_steps_printer.assert_called_with(called_method, 'mock_result', False, self.mock_stdout)

        self.assertEqual(len(self.mock_call_trail_client_method.mock_calls), len(method_list))


class TestNamespace(unittest.TestCase):
    def test_Names(self):
        names = Names(['foo', 'Foo', 'with-a-hyphen', 'with % special characters', '0starting_with_zero'])
        self.assertEqual(names.foo, 'foo')
        self.assertEqual(names.Foo, 'Foo')
        self.assertEqual(names.with_a_hyphen, 'with-a-hyphen')
        self.assertEqual(names.with___special_characters, 'with % special characters')
        self.assertEqual(names._starting_with_zero, '0starting_with_zero')

    def test_create_namespaces_for_tag_keys_and_values(self):
        def action_a():
            pass

        step_a = Step(action_a, foo='bar', n=5)

        keys, values = create_namespaces_for_tag_keys_and_values(step_a)

        self.assertEqual(keys.foo, 'foo')
        self.assertEqual(keys.name, 'name')
        with self.assertRaises(AttributeError):
            keys.n

        self.assertEqual(values.bar, 'bar')
        self.assertEqual(values.action_a, 'action_a')
