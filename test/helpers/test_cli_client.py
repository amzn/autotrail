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

from autotrail.helpers.cli_client import (add_arguments_to_parser, cli_args_for_tag_keys_and_values,
                                         extract_args_and_kwargs, get_tag_keys_and_values, get_tags_from_namespace,
                                         make_api_call, PARAMETER_EXTRACTOR_MAPPING, setup_cli_args_for_trail_client,
                                         substitute_cli_switches)


class TestHelperFunctions(unittest.TestCase):
    def test_extract_args_and_kwargs_with_nothing(self):
        args, kwargs = extract_args_and_kwargs()

        self.assertEqual(args, ())
        self.assertEqual(kwargs, {})

    def test_extract_args_and_kwargs(self):
        args, kwargs = extract_args_and_kwargs('a', 10, True, b='b', twenty=20, false=False)

        self.assertEqual(args, ('a', 10, True))
        self.assertEqual(kwargs, dict(b='b', twenty=20, false=False))

    def test_get_tag_keys_and_values(self):
        list_of_tags = [{'n': 0, 'foo': 'bar'}, {'n': 1}, {'n': 2, 'foo': 'bar'} ]

        tag_key_values = get_tag_keys_and_values(list_of_tags)

        self.assertEqual(tag_key_values, {'n': [0, 1, 2], 'foo': ['bar']})

    def test_get_tag_keys_and_values_with_nothing(self):
        tag_key_values = get_tag_keys_and_values([])

        self.assertEqual(tag_key_values, {})

    def test_get_tags_from_namespace(self):
        mock_namespace = MagicMock()
        mock_namespace.n = 0
        mock_namespace.foo = 'bar'

        tag_keys_and_values = {'n': [0, 1, 2], 'foo': ['bar']}

        tags = get_tags_from_namespace(tag_keys_and_values, mock_namespace)

        self.assertEqual(tags, dict(n=0, foo='bar'))

    def test_get_tags_from_namespace_with_namespace_containing_undefined_tags(self):
        mock_namespace = MagicMock()
        mock_namespace.n = 0
        mock_namespace.foo = 'bar'
        mock_namespace.undefined = 'undefined'

        tag_keys_and_values = {'n': [0, 1, 2], 'foo': ['bar']}

        tags = get_tags_from_namespace(tag_keys_and_values, mock_namespace)

        self.assertEqual(tags, dict(n=0, foo='bar'))

    def test_get_tags_from_namespace_with_namespace_containing_invalid_tags_value(self):
        mock_namespace = MagicMock()
        mock_namespace.n = 3
        mock_namespace.foo = 'bar'

        tag_keys_and_values = {'n': [0, 1, 2], 'foo': ['bar']}

        with self.assertRaises(ValueError):
            tags = get_tags_from_namespace(tag_keys_and_values, mock_namespace)

    def test_get_tags_from_namespace_with_valid_tag_having_none_value(self):
        mock_namespace = MagicMock()
        mock_namespace.n = 0
        mock_namespace.foo = None

        tag_keys_and_values = {'n': [0, 1, 2], 'foo': ['bar']}

        tags = get_tags_from_namespace(tag_keys_and_values, mock_namespace)

        self.assertEqual(tags, dict(n=0))


class TestCLIHelpers(unittest.TestCase):
    def test_cli_args_for_tag_keys_and_values(self):
        mock_extracted_args_and_kwargs = MagicMock()
        extract_args_and_kwargs_patcher = patch('autotrail.helpers.cli_client.extract_args_and_kwargs',
                                                return_value=mock_extracted_args_and_kwargs)
        mock_extract_args_and_kwargs = extract_args_and_kwargs_patcher.start()


        tag_key_values = {'n': [0, 1, 2], 'foo': ['bar']}
        cli_args = list(cli_args_for_tag_keys_and_values(tag_key_values))

        self.assertEqual(cli_args, [mock_extracted_args_and_kwargs, mock_extracted_args_and_kwargs])
        mock_extract_args_and_kwargs.assert_any_call('--n', type=int, choices=[0, 1, 2])
        mock_extract_args_and_kwargs.assert_any_call('--foo', type=str, choices=['bar'])

        extract_args_and_kwargs_patcher.stop()

    def test_substitute_cli_switches(self):
        methods_cli_switch_mapping = {
            'mock_method': ('mock help', [
                extract_args_and_kwargs('--mock-switch', type=int, default=1, help='mock switch help'),
                'substitute_this']),
        }
        substitution_mapping = {
            'substitute_this': ['This is the substituted value.'],
        }

        substitute_cli_switches(methods_cli_switch_mapping, substitution_mapping)

        self.assertEqual(methods_cli_switch_mapping, {
            'mock_method': ('mock help', [
                extract_args_and_kwargs('--mock-switch', type=int, default=1, help='mock switch help'),
                'This is the substituted value.']),
        })

    def test_substitute_cli_switches_with_one_to_many_substitution(self):
        methods_cli_switch_mapping = {
            'mock_method': ('mock help', [
                extract_args_and_kwargs('--mock-switch', type=int, default=1, help='mock switch help'),
                'substitute_this']),
        }
        substitution_mapping = {
            'substitute_this': ['This is the substituted value 1.', 'This is the substituted value 2.'],
        }

        substitute_cli_switches(methods_cli_switch_mapping, substitution_mapping)

        self.assertEqual(methods_cli_switch_mapping, {
            'mock_method': ('mock help', [
                extract_args_and_kwargs('--mock-switch', type=int, default=1, help='mock switch help'),
                'This is the substituted value 1.', 'This is the substituted value 2.']),
        })

    def test_substitute_cli_switches_with_substitution_not_defined(self):
        methods_cli_switch_mapping = {
            'mock_method': ('mock help', [
                extract_args_and_kwargs('--mock-switch', type=int, default=1, help='mock switch help'),
                'substitute_this', 'undefined_substitution']),
        }
        substitution_mapping = {
            'substitute_this': ['This is the substituted value.'],
        }

        substitute_cli_switches(methods_cli_switch_mapping, substitution_mapping)

        self.assertEqual(methods_cli_switch_mapping, {
            'mock_method': ('mock help', [
                extract_args_and_kwargs('--mock-switch', type=int, default=1, help='mock switch help'),
                'This is the substituted value.', 'undefined_substitution']),
        })

    def test_add_arguments_to_parser(self):
        mock_parser = MagicMock()
        args_and_kwargs_list = [extract_args_and_kwargs('--mock-switch', type=int, default=1, help='mock switch help')]

        add_arguments_to_parser(mock_parser, args_and_kwargs_list)

        mock_parser.add_argument.assert_called_once_with('--mock-switch', type=int, default=1, help='mock switch help')

    def test_setup_cli_args_for_trail_client(self):
        mock_parser = MagicMock()
        method_parser = MagicMock()
        mock_parser.add_subparsers = MagicMock(return_value=method_parser)
        sub_parser = MagicMock()
        method_parser.add_parser = MagicMock(return_value=sub_parser)

        methods_cli_switch_mapping = {
            'mock_method': ('mock help', [
                extract_args_and_kwargs('--mock-switch', type=int, default=1, help='mock switch help')]),
        }
        substitution_mapping = {
            'substitute_this': ['This is the substituted value.'],
        }

        substitute_cli_switches_patcher = patch('autotrail.helpers.cli_client.substitute_cli_switches')
        mock_substitute_cli_switches = substitute_cli_switches_patcher.start()

        add_arguments_to_parser_patcher = patch('autotrail.helpers.cli_client.add_arguments_to_parser')
        mock_add_arguments_to_parser = add_arguments_to_parser_patcher.start()

        setup_cli_args_for_trail_client(mock_parser, substitution_mapping, methods_cli_switch_mapping)

        mock_substitute_cli_switches.assert_called_once_with(methods_cli_switch_mapping, substitution_mapping)
        mock_parser.add_subparsers.assert_called_once_with(help='Choose one of the methods.', dest='method')
        method_parser.add_parser.assert_called_once_with('mock_method', help='mock help')
        mock_add_arguments_to_parser.assert_called_once_with(
            sub_parser, methods_cli_switch_mapping['mock_method'][1])

        add_arguments_to_parser_patcher.stop()
        substitute_cli_switches_patcher.stop()

    def test_make_api_call(self):
        namespace = MagicMock()
        namespace.method = 'mock_method'
        tags = MagicMock()
        client = MagicMock()
        args = ('mock_arg',)
        kwargs = dict(mock_key='mock_value')
        mock_extractor = MagicMock(return_value=(args, kwargs))
        parameter_extractor_mapping = {'mock_method': mock_extractor}

        make_api_call(namespace, tags, client, parameter_extractor_mapping=parameter_extractor_mapping)

        mock_extractor.assert_called_once_with(namespace, tags)
        client.mock_method.assert_called_once_with(*args, **kwargs)

class TestParameterExtractorMappingLambdas(unittest.TestCase):
    def setUp(self):
        self.namespace = MagicMock()
        self.tags = dict(mock_key='mock_value')

    def test_start(self):
        args, kwargs = PARAMETER_EXTRACTOR_MAPPING['start'](self.namespace, self.tags)

        self.assertEqual(kwargs, dict())
        self.assertEqual(args, ())

    def test_stop(self):
        args, kwargs = PARAMETER_EXTRACTOR_MAPPING['stop'](self.namespace, self.tags)

        self.assertEqual(kwargs, dict(dry_run=self.namespace.dry_run))
        self.assertEqual(args, ())

    def test_shutdown(self):
        args, kwargs = PARAMETER_EXTRACTOR_MAPPING['shutdown'](self.namespace, self.tags)

        self.assertEqual(kwargs, dict(dry_run=self.namespace.dry_run))
        self.assertEqual(args, ())

    def test_next_step(self):
        args, kwargs = PARAMETER_EXTRACTOR_MAPPING['next_step'](self.namespace, self.tags)

        self.assertEqual(kwargs, dict(step_count=self.namespace.step_count, dry_run=self.namespace.dry_run))
        self.assertEqual(args, ())

    def test_send_message_to_steps(self):
        self.namespace.type = 'str'
        self.namespace.message = 'mock_message'

        args, kwargs = PARAMETER_EXTRACTOR_MAPPING['send_message_to_steps'](self.namespace, self.tags)

        self.assertEqual(kwargs, dict(message='mock_message', dry_run=self.namespace.dry_run, mock_key='mock_value'))
        self.assertEqual(args, ())

    def test_status(self):
        self.namespace.fields = 'mock_fields'
        self.namespace.state = 'mock_state'

        args, kwargs = PARAMETER_EXTRACTOR_MAPPING['status'](self.namespace, self.tags)

        self.assertEqual(kwargs, dict(fields='mock_fields', states='mock_state', mock_key='mock_value'))
        self.assertEqual(args, ())

    def test_steps_waiting_for_user_input(self):
        args, kwargs = PARAMETER_EXTRACTOR_MAPPING['steps_waiting_for_user_input'](self.namespace, self.tags)

        self.assertEqual(kwargs, dict(mock_key='mock_value'))
        self.assertEqual(args, ())

    def test_pause(self):
        args, kwargs = PARAMETER_EXTRACTOR_MAPPING['pause'](self.namespace, self.tags)

        self.assertEqual(kwargs, dict(dry_run=self.namespace.dry_run, mock_key='mock_value'))
        self.assertEqual(args, ())

    def test_pause_branch(self):
        args, kwargs = PARAMETER_EXTRACTOR_MAPPING['pause_branch'](self.namespace, self.tags)

        self.assertEqual(kwargs, dict(dry_run=self.namespace.dry_run, mock_key='mock_value'))
        self.assertEqual(args, ())

    def test_set_pause_on_fail(self):
        args, kwargs = PARAMETER_EXTRACTOR_MAPPING['set_pause_on_fail'](self.namespace, self.tags)

        self.assertEqual(kwargs, dict(dry_run=self.namespace.dry_run, mock_key='mock_value'))
        self.assertEqual(args, ())

    def test_unset_pause_on_fail(self):
        args, kwargs = PARAMETER_EXTRACTOR_MAPPING['unset_pause_on_fail'](self.namespace, self.tags)

        self.assertEqual(kwargs, dict(dry_run=self.namespace.dry_run, mock_key='mock_value'))
        self.assertEqual(args, ())

    def test_interrupt(self):
        args, kwargs = PARAMETER_EXTRACTOR_MAPPING['interrupt'](self.namespace, self.tags)

        self.assertEqual(kwargs, dict(dry_run=self.namespace.dry_run, mock_key='mock_value'))
        self.assertEqual(args, ())

    def test_resume(self):
        args, kwargs = PARAMETER_EXTRACTOR_MAPPING['resume'](self.namespace, self.tags)

        self.assertEqual(kwargs, dict(dry_run=self.namespace.dry_run, mock_key='mock_value'))
        self.assertEqual(args, ())

    def test_resume_branch(self):
        args, kwargs = PARAMETER_EXTRACTOR_MAPPING['resume_branch'](self.namespace, self.tags)

        self.assertEqual(kwargs, dict(dry_run=self.namespace.dry_run, mock_key='mock_value'))
        self.assertEqual(args, ())

    def test_list(self):
        args, kwargs = PARAMETER_EXTRACTOR_MAPPING['list'](self.namespace, self.tags)

        self.assertEqual(kwargs, dict(mock_key='mock_value'))
        self.assertEqual(args, ())

    def test_skip(self):
        args, kwargs = PARAMETER_EXTRACTOR_MAPPING['skip'](self.namespace, self.tags)

        self.assertEqual(kwargs, dict(dry_run=self.namespace.dry_run, mock_key='mock_value'))
        self.assertEqual(args, ())

    def test_unskip(self):
        args, kwargs = PARAMETER_EXTRACTOR_MAPPING['unskip'](self.namespace, self.tags)

        self.assertEqual(kwargs, dict(dry_run=self.namespace.dry_run, mock_key='mock_value'))
        self.assertEqual(args, ())

    def test_block(self):
        args, kwargs = PARAMETER_EXTRACTOR_MAPPING['block'](self.namespace, self.tags)

        self.assertEqual(kwargs, dict(dry_run=self.namespace.dry_run, mock_key='mock_value'))
        self.assertEqual(args, ())

    def test_unblock(self):
        args, kwargs = PARAMETER_EXTRACTOR_MAPPING['unblock'](self.namespace, self.tags)

        self.assertEqual(kwargs, dict(dry_run=self.namespace.dry_run, mock_key='mock_value'))
        self.assertEqual(args, ())
