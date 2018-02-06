"""Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.
"""

import unittest

from mock import MagicMock, patch, call
from Queue import Empty as QueueEmpty
from subprocess import PIPE

from autotrail.helpers.step import (
    accepts_context,
    accepts_environment,
    accepts_nothing,
    apply_object_attributes_to_template,
    create_conditional_step,
    create_stream_listener,
    create_stream_writer,
    extraction_wrapper,
    get_messages_from_queue,
    Instruction,
    InstructionNotCompletedError,
    make_context_attribute_based_templating_function,
    make_simple_templating_function,
    run_shell_command,
    ShellCommand,
    ShellCommandFailedError,
    stream_reader,
    stream_writer,
    tail_command)


class TestFactoryFunctions(unittest.TestCase):
    def test_create_conditional_step(self):
        def a():
            pass

        step_a = create_conditional_step(a, foo='bar')

        self.assertEqual(step_a.action_function, a)
        self.assertFalse(step_a.pause_on_fail)
        self.assertTrue(step_a.skip_progeny_on_failure)


class TestExtractionWrapper(unittest.TestCase):
    def test_extraction_wrapper_for_function_with_return_extractor(self):
        def foo_bar_func(foo, bar, other='Hello'):
            return (foo, bar, other)

        wrapped_foo_bar_func = extraction_wrapper(
            foo_bar_func,
            [lambda env, context: dict(foo=context.get_foo()), lambda env, context: dict(bar=env.get_bar())],
            return_handlers=[lambda rval, context: context.add_value(rval[2])],
            return_value_extractor=lambda rval, context: rval[2])

        context = MagicMock()
        context.get_foo = MagicMock(return_value='mock foo')
        environment = MagicMock()
        environment.get_bar = MagicMock(return_value='mock bar')

        returned_value = wrapped_foo_bar_func(environment, context)

        context.get_foo.assert_called_once_with()
        environment.get_bar.assert_called_once_with()
        context.add_value.assert_called_once_with('Hello')
        self.assertEqual(returned_value, 'Hello')

    def test_extraction_wrapper_for_function_with_default_extractor_and_no_return_handlers(self):
        def foo_bar_func(foo, bar, other='Hello'):
            return (foo, bar, other)

        wrapped_foo_bar_func = extraction_wrapper(
            foo_bar_func,
            [lambda env, context: dict(foo=context.get_foo()), lambda env, context: dict(bar=env.get_bar())])

        context = MagicMock()
        context.get_foo = MagicMock(return_value='mock foo')
        environment = MagicMock()
        environment.get_bar = MagicMock(return_value='mock bar')

        returned_value = wrapped_foo_bar_func(environment, context)

        context.get_foo.assert_called_once_with()
        environment.get_bar.assert_called_once_with()
        self.assertEqual(context.add_value.call_count, 0)
        self.assertEqual(returned_value, ('mock foo', 'mock bar', 'Hello'))

    def test_extraction_wrapper_for_object_with_call_method(self):
        class FooBar(object):
            def __call__(self, foo, bar, other='Hello'):
                return (foo, bar, other)

        FooBar.__call__ = extraction_wrapper(
            FooBar.__call__,
            [lambda env, context: dict(foo=context.get_foo()), lambda env, context: dict(bar=env.get_bar())],
            function=False,
            return_handlers=[lambda rval, context: context.add_value(rval)])

        context = MagicMock()
        context.get_foo = MagicMock(return_value='mock foo')
        environment = MagicMock()
        environment.get_bar = MagicMock(return_value='mock bar')

        foo_bar = FooBar()
        returned_value = foo_bar(environment, context)

        context.get_foo.assert_called_once_with()
        environment.get_bar.assert_called_once_with()
        context.add_value.assert_called_once_with(
            ('mock foo', 'mock bar', 'Hello'))
        self.assertEqual(returned_value, ('mock foo', 'mock bar', 'Hello'))

    def test_extraction_wrapper_with_exception_handlers(self):
        def foo_bar_func(foo, bar, other='Hello'):
            raise ValueError('Some exception message')
            return (foo, bar, other)

        wrapped_foo_bar_func = extraction_wrapper(
            foo_bar_func,
            [lambda env, context: dict(foo=context.get_foo()), lambda env, context: dict(bar=env.get_bar())],
            exception_handlers=[(ValueError, lambda e, context: 'Handled ValueError')])

        context = MagicMock()
        context.get_foo = MagicMock(return_value='mock foo')
        environment = MagicMock()
        environment.get_bar = MagicMock(return_value='mock bar')

        returned_value = wrapped_foo_bar_func(environment, context)

        context.get_foo.assert_called_once_with()
        environment.get_bar.assert_called_once_with()
        self.assertEqual(context.add_value.call_count, 0)
        self.assertEqual(returned_value, 'Handled ValueError')

    def test_extraction_wrapper_with_exception_handlers_unhandled_exception(self):
        def foo_bar_func(foo, bar, other='Hello'):
            raise TypeError('Some exception message')
            return (foo, bar, other)

        wrapped_foo_bar_func = extraction_wrapper(
            foo_bar_func,
            [lambda env, context: dict(foo=context.get_foo()), lambda env, context: dict(bar=env.get_bar())],
            exception_handlers=[(ValueError, lambda e, context: 'Handled ValueError')])

        context = MagicMock()
        context.get_foo = MagicMock(return_value='mock foo')
        environment = MagicMock()
        environment.get_bar = MagicMock(return_value='mock bar')

        with self.assertRaises(TypeError):
            returned_value = wrapped_foo_bar_func(environment, context)

        context.get_foo.assert_called_once_with()
        environment.get_bar.assert_called_once_with()
        self.assertEqual(context.add_value.call_count, 0)


class TestDecorators(unittest.TestCase):
    def test_accepts_context(self):
        @accepts_context
        def func_accepting_only_context(context):
            return context

        trail_env = MagicMock()
        context = 'mock_context'

        return_value = func_accepting_only_context(trail_env, context)

        self.assertEqual(return_value, context)

    def test_accepts_environment(self):
        @accepts_environment
        def func_accepting_only_environment(environment):
            return environment

        trail_env = 'mock_environment'
        context = MagicMock()

        return_value = func_accepting_only_environment(trail_env, context)

        self.assertEqual(return_value, trail_env)

    def test_accepts_nothing(self):
        @accepts_nothing
        def func_accepting_nothing():
            return 'Nothing'

        trail_env = MagicMock()
        context = MagicMock()

        return_value = func_accepting_nothing(trail_env, context)

        self.assertEqual(return_value, 'Nothing')


class Context(object):
    """This is a context class meant for testing Instruction and ShellCommand."""
    def __init__(self, value):
        self.value = value


class TestInstruction(unittest.TestCase):
    def test_instruction_instance(self):
        mock_instruction_function = MagicMock(return_value='mock instruction')
        instruction = Instruction('mock_instruction', mock_instruction_function)

        self.assertEqual(instruction.name, 'mock_instruction')
        self.assertEqual(instruction.instruction_function, mock_instruction_function)
        self.assertEqual(instruction.reply_needed, True)
        self.assertEqual(str(instruction), instruction.name)

    def test_instruction_with_reply_needed_with_reply_true(self):
        mock_instruction_function = MagicMock(return_value='mock instruction')
        instruction = Instruction('mock_instruction', mock_instruction_function)
        trail_env = MagicMock()
        trail_env.input = MagicMock(return_value=True)
        context = MagicMock()

        instruction(trail_env, context)

        mock_instruction_function.assert_called_once_with(trail_env, context)
        trail_env.input.assert_called_once_with('mock instruction')

    def test_instruction_with_reply_needed_with_reply_false(self):
        mock_instruction_function = MagicMock(return_value='mock instruction')
        instruction = Instruction('mock_instruction', mock_instruction_function)
        trail_env = MagicMock()
        trail_env.input = MagicMock(return_value=False)
        context = MagicMock()

        with self.assertRaises(InstructionNotCompletedError):
            instruction(trail_env, context)

        mock_instruction_function.assert_called_once_with(trail_env, context)
        trail_env.input.assert_called_once_with('mock instruction')

    def test_instruction_with_reply_not_needed(self):
        mock_instruction_function = MagicMock(return_value='mock instruction')
        instruction = Instruction('mock_instruction', mock_instruction_function, reply_needed=False)
        trail_env = MagicMock()
        context = MagicMock()

        instruction(trail_env, context)

        mock_instruction_function.assert_called_once_with(trail_env, context)
        trail_env.output.assert_called_once_with('mock instruction')
        self.assertEqual(trail_env.input.call_count, 0)

    def test_instruction_with_instruction_function_returning_none(self):
        mock_instruction_function = MagicMock(return_value=None)
        instruction = Instruction('mock_instruction', mock_instruction_function)
        trail_env = MagicMock()
        trail_env.input = MagicMock(return_value=True)
        context = MagicMock()

        instruction(trail_env, context)

        mock_instruction_function.assert_called_once_with(trail_env, context)
        self.assertEqual(trail_env.input.call_count, 0)
        self.assertEqual(trail_env.output.call_count, 0)


class TestShellCommandInstance(unittest.TestCase):
    def setUp(self):
        self.mock_command_function = MagicMock(return_value='mock command')
        self.shell_command = ShellCommand('mock_command', self.mock_command_function)

    def test_shell_command_instance(self):
        self.assertEqual(self.shell_command.name, 'mock_command')
        self.assertEqual(self.shell_command.command_function, self.mock_command_function)
        self.assertEqual(self.shell_command.delay, 1)
        self.assertEqual(str(self.shell_command), self.shell_command.name)

    def test_shell_command_default_predicate_functions(self):
        self.assertFalse(self.shell_command.is_error_ignorable('foo'))
        self.assertFalse(self.shell_command.is_output_ignorable('foo'))

        self.assertTrue(self.shell_command.is_error_ignorable(''))
        self.assertTrue(self.shell_command.is_output_ignorable(''))

        self.assertFalse(self.shell_command.is_error_fatal(''))
        self.assertFalse(self.shell_command.is_error_fatal('foo'))

        self.assertFalse(self.shell_command.is_exit_code_ignorable(1))
        self.assertTrue(self.shell_command.is_exit_code_ignorable(0))


class TestShellCommandMethods(unittest.TestCase):
    def setUp(self):
        self.stdin = MagicMock()
        self.stdout = MagicMock()
        self.stderr = MagicMock()
        self.queue = MagicMock()
        self.listener = MagicMock()
        self.command_process = MagicMock()
        self.writer_process = MagicMock()

        self.run_shell_command_patcher = patch('autotrail.helpers.step.run_shell_command', return_value=(
            self.command_process, self.stdin, self.stdout, self.stderr))
        self.mock_run_shell_command = self.run_shell_command_patcher.start()


        self.create_stream_listener_patcher = patch('autotrail.helpers.step.create_stream_listener', return_value=(
            self.listener, self.queue))
        self.mock_create_stream_listener = self.create_stream_listener_patcher.start()

        self.create_stream_writer_patcher = patch('autotrail.helpers.step.create_stream_writer',
                                                  return_value=self.writer_process)
        self.mock_create_stream_writer = self.create_stream_writer_patcher.start()

        def mimick_tail_command():
            stdout_messages = ['stdout_1', 'stdout_2']
            stderr_messages = ['stderr_1']
            yield (stdout_messages, stderr_messages)

        self.tail_command_patcher = patch('autotrail.helpers.step.tail_command', return_value=mimick_tail_command())
        self.mock_tail_command =  self.tail_command_patcher.start()

        mock_command_function = MagicMock(return_value='mock command')

        self.shell_command = ShellCommand('mock_command', mock_command_function, delay=0.1)

    def tearDown(self):
        self.run_shell_command_patcher.stop()
        self.create_stream_listener_patcher.stop()
        self.create_stream_writer_patcher.stop()
        self.tail_command_patcher.stop()

    def test_send_messages(self):
        trail_env = MagicMock()
        self.shell_command.send_messages(trail_env, 'mock_prefix', ['mock message'])

        trail_env.output.assert_called_once_with('[mock_prefix] -- mock message')

    def test_get_valid_stdout_messages(self):
        self.shell_command.is_output_ignorable = lambda x: True if 'mock' in x else False

        mesages = self.shell_command.get_valid_stdout_messages(['mock', 'foo', 'some mock'])

        self.assertEqual(mesages, ['foo'])

    def test_get_valid_stderr_messages(self):
        self.shell_command.is_error_ignorable = lambda x: True if 'mock' in x else False

        mesages = self.shell_command.get_valid_stderr_messages(['mock', 'foo', 'some mock'])

        self.assertEqual(mesages, ['foo'])

    def test_get_latest_fatal_error(self):
        self.shell_command.is_error_fatal = lambda x: True if 'fatal' in x else False

        fatal_message = self.shell_command.get_latest_fatal_error(
            ['mock error', 'some fatal error', 'another error', 'yet another fatal error'])

        self.assertEqual(fatal_message, 'yet another fatal error')


    def test_generate_failure_message(self):
        command_process = MagicMock()
        command_process.poll = MagicMock(return_value=100)

        message = self.shell_command.generate_failure_message('mock command', command_process, 'mock fatal error')

        self.assertEqual(
            message, '[Command: mock command] -- Failed with exit code: 100, STDERR (Last line): mock fatal error')

    def test_generate_success_message(self):
        command_process = MagicMock()
        command_process.poll = MagicMock(return_value=100)

        message = self.shell_command.generate_success_message('mock command', command_process)

        self.assertEqual(message, '[Command: mock command] -- Succeeded with exit code: 100.')

    def test_was_run_successful_with_success(self):
        command_process = MagicMock()
        command_process.poll = MagicMock(return_value=0)

        self.assertTrue(self.shell_command.was_run_successful(command_process, None))

        command_process.poll.assert_called_once_with()

    def test_was_run_successful_with_failure_due_to_exit_code(self):
        command_process = MagicMock()
        command_process.poll = MagicMock(return_value=100)

        self.assertFalse(self.shell_command.was_run_successful(command_process, None))

        command_process.poll.assert_called_once_with()

    def test_was_run_successful_with_failure_due_to_fatal_error(self):
        command_process = MagicMock()
        command_process.poll = MagicMock(return_value=0)

        self.assertFalse(self.shell_command.was_run_successful(command_process, 'Mock fatal error'))

        command_process.poll.assert_called_once_with()

    def test_run_with_success(self):
        self.shell_command.get_valid_stdout_messages = MagicMock()
        self.shell_command.get_valid_stderr_messages = MagicMock()
        self.shell_command.get_latest_fatal_error = MagicMock(return_value=None)
        self.command_process.poll = MagicMock()
        self.shell_command.was_run_successful = MagicMock(return_value=True)
        self.shell_command.generate_success_message = MagicMock(return_value='mock success message')
        self.shell_command.generate_failure_message = MagicMock()
        trail_env = MagicMock()
        context = MagicMock()

        return_value = self.shell_command(trail_env, context)

        self.mock_run_shell_command.assert_called_once_with('mock command')
        self.mock_create_stream_listener.assert_any_call(self.stdout)
        self.mock_create_stream_listener.assert_any_call(self.stderr)
        self.mock_create_stream_writer.assert_called_once_with(trail_env.input_queue, self.stdin)
        self.mock_tail_command.assert_called_once_with(self.command_process, self.queue, self.queue, delay=0.1)
        self.shell_command.get_valid_stdout_messages.assert_called_once_with(['stdout_1', 'stdout_2'])
        self.shell_command.get_valid_stderr_messages.assert_called_once_with(['stderr_1'])
        self.shell_command.get_latest_fatal_error.assert_called_once_with(
            self.shell_command.get_valid_stderr_messages())
        self.assertEqual(self.listener.terminate.call_count, 2)
        self.writer_process.terminate.assert_called_once_with()
        self.shell_command.was_run_successful.assert_called_once_with(
            self.command_process, self.shell_command.get_latest_fatal_error())
        self.shell_command.generate_success_message.assert_called_once_with('mock command', self.command_process)
        self.assertEqual(self.shell_command.generate_failure_message.call_count, 0)
        # Count here will be 2 because we have mocked out send_messages.
        self.assertEqual(trail_env.output.call_count, 2)
        start_notification = trail_env.output.call_args_list[0]
        end_notification = trail_env.output.call_args_list[1]
        self.assertEqual(start_notification, call('[Command: mock command] -- Starting.'))
        self.assertEqual(end_notification, call('mock success message'))

    def test_run_with_failure(self):
        self.shell_command.get_valid_stdout_messages = MagicMock()
        self.shell_command.get_valid_stderr_messages = MagicMock()
        self.shell_command.get_latest_fatal_error = MagicMock(return_value=None)
        self.command_process.poll = MagicMock()
        self.shell_command.was_run_successful = MagicMock(return_value=False)
        self.shell_command.generate_success_message = MagicMock()
        self.shell_command.generate_failure_message = MagicMock(return_value='mock failure message')
        trail_env = MagicMock()
        context = MagicMock()

        with self.assertRaises(ShellCommandFailedError):
            self.shell_command(trail_env, context)

        self.mock_run_shell_command.assert_called_once_with('mock command')
        self.mock_create_stream_listener.assert_any_call(self.stdout)
        self.mock_create_stream_listener.assert_any_call(self.stderr)
        self.mock_create_stream_writer.assert_called_once_with(trail_env.input_queue, self.stdin)
        self.mock_tail_command.assert_called_once_with(self.command_process, self.queue, self.queue, delay=0.1)
        self.shell_command.get_valid_stdout_messages.assert_called_once_with(['stdout_1', 'stdout_2'])
        self.shell_command.get_valid_stderr_messages.assert_called_once_with(['stderr_1'])
        self.shell_command.get_latest_fatal_error.assert_called_once_with(
            self.shell_command.get_valid_stderr_messages())
        self.assertEqual(self.listener.terminate.call_count, 2)
        self.writer_process.terminate.assert_called_once_with()
        self.shell_command.was_run_successful.assert_called_once_with(
            self.command_process, self.shell_command.get_latest_fatal_error())
        self.shell_command.generate_failure_message.assert_called_once_with(
            'mock command', self.command_process, self.shell_command.get_latest_fatal_error())
        self.assertEqual(self.shell_command.generate_success_message.call_count, 0)
        # Count here will be 2 because we have mocked out send_messages.
        self.assertEqual(trail_env.output.call_count, 2)
        start_notification = trail_env.output.call_args_list[0]
        end_notification = trail_env.output.call_args_list[1]
        self.assertEqual(start_notification, call('[Command: mock command] -- Starting.'))
        self.assertEqual(end_notification, call('mock failure message'))

    def test_run_with_no_command(self):
        self.shell_command.get_valid_stdout_messages = MagicMock()
        self.shell_command.get_valid_stderr_messages = MagicMock()
        self.shell_command.get_latest_fatal_error = MagicMock(return_value=None)
        self.shell_command.command_function = MagicMock(return_value=None)
        self.command_process.poll = MagicMock()
        self.shell_command.was_run_successful = MagicMock(return_value=True)
        self.shell_command.generate_success_message = MagicMock(return_value='mock success message')
        self.shell_command.generate_failure_message = MagicMock()
        trail_env = MagicMock()
        context = MagicMock()

        return_value = self.shell_command(trail_env, context)

        self.assertEqual(return_value, 'No command to run.')
        self.shell_command.command_function.assert_called_once_with(trail_env, context)
        self.mock_run_shell_command.assert_not_called()
        self.assertEqual(self.mock_run_shell_command.call_count, 0)
        self.assertEqual(self.mock_create_stream_listener.call_count, 0)
        self.assertEqual(self.mock_create_stream_writer.call_count, 0)
        self.assertEqual(self.mock_tail_command.call_count, 0)
        self.assertEqual(self.listener.terminate.call_count, 0)
        self.assertEqual(self.writer_process.terminate.call_count, 0)
        self.assertEqual(self.shell_command.was_run_successful.call_count, 0)
        self.assertEqual(self.shell_command.generate_success_message.call_count, 0)
        self.assertEqual(self.shell_command.generate_failure_message.call_count, 0)
        self.assertEqual(trail_env.output.call_count, 0)


class TestFunctions(unittest.TestCase):
    def test_apply_object_attributes_to_template_no_context_and_no_template(self):
        test_string = apply_object_attributes_to_template('This is not a templated string.', None)
        self.assertEqual(test_string, 'This is not a templated string.')

    def test_apply_object_attributes_to_template_no_context_and_template(self):
        with self.assertRaises(AttributeError):
            test_string = apply_object_attributes_to_template('This is a {templated} string.', None)

    def test_apply_object_attributes_to_template_no_template_and_context(self):
        context = Context('mock-value')

        test_string = apply_object_attributes_to_template('templated string without placeholder', context)
        self.assertEqual(test_string, 'templated string without placeholder')

    def test_apply_object_attributes_to_template_template_and_context(self):
        context = Context('mock-value')

        test_string = apply_object_attributes_to_template('templated string with placeholder: {value}', context)
        self.assertEqual(test_string, 'templated string with placeholder: mock-value')

    def test_make_simple_templating_function(self):
        apply_object_attributes_to_template_patcher = patch(
            'autotrail.helpers.step.apply_object_attributes_to_template', return_value='mock return')
        mock_apply_object_attributes_to_template = apply_object_attributes_to_template_patcher.start()
        trail_env = MagicMock()
        context = MagicMock()

        templating_function = make_simple_templating_function('mock function using {value}')
        return_value = templating_function(trail_env, context)

        mock_apply_object_attributes_to_template.assert_called_once_with('mock function using {value}', context)
        self.assertEqual(return_value, 'mock return')
        self.assertEqual(trail_env.call_count, 0)

        apply_object_attributes_to_template_patcher.stop()

    def test_make_context_attribute_based_templating_function_with_default_expected_value(self):
        apply_object_attributes_to_template_patcher = patch(
            'autotrail.helpers.step.apply_object_attributes_to_template', return_value='mock return')
        mock_apply_object_attributes_to_template = apply_object_attributes_to_template_patcher.start()
        trail_env = MagicMock()
        context = MagicMock()
        context.mock_attribute = True

        templating_function = make_context_attribute_based_templating_function(
            'mock_attribute', 'mock instruction using {value}')
        return_value = templating_function(trail_env, context)

        mock_apply_object_attributes_to_template.assert_called_once_with('mock instruction using {value}', context)
        self.assertEqual(return_value, 'mock return')
        self.assertEqual(len(trail_env.mock_calls), 0)

        apply_object_attributes_to_template_patcher.stop()

    def test_make_context_attribute_based_templating_function_with_custom_expected_value(self):
        apply_object_attributes_to_template_patcher = patch(
            'autotrail.helpers.step.apply_object_attributes_to_template', return_value='mock return')
        mock_apply_object_attributes_to_template = apply_object_attributes_to_template_patcher.start()
        trail_env = MagicMock()
        context = MagicMock()
        context.mock_attribute = 'mock expected value'

        templating_function = make_context_attribute_based_templating_function(
            'mock_attribute', 'mock instruction using {value}', expected_value='mock expected value')
        return_value = templating_function(trail_env, context)

        mock_apply_object_attributes_to_template.assert_called_once_with('mock instruction using {value}', context)
        self.assertEqual(return_value, 'mock return')
        self.assertEqual(len(trail_env.mock_calls), 0)

        apply_object_attributes_to_template_patcher.stop()

    def test_make_context_attribute_based_templating_function_when_values_do_not_match(self):
        apply_object_attributes_to_template_patcher = patch(
            'autotrail.helpers.step.apply_object_attributes_to_template', return_value='mock return')
        mock_apply_object_attributes_to_template = apply_object_attributes_to_template_patcher.start()
        trail_env = MagicMock()
        trail_env.output = MagicMock()
        context = MagicMock()
        context.mock_attribute = MagicMock(return_value='expected value 1')

        templating_function = make_context_attribute_based_templating_function(
            'mock_attribute', 'mock instruction using {value}', expected_value='expected value 2')
        return_value = templating_function(trail_env, context)

        self.assertEqual(mock_apply_object_attributes_to_template.call_count, 0)
        self.assertEqual(return_value, None)
        self.assertEqual(trail_env.output.call_count, 1)

        apply_object_attributes_to_template_patcher.stop()

    def test_make_context_attribute_based_templating_function_when_values_do_not_match_and_notify_user_is_false(self):
        apply_object_attributes_to_template_patcher = patch(
            'autotrail.helpers.step.apply_object_attributes_to_template', return_value='mock return')
        mock_apply_object_attributes_to_template = apply_object_attributes_to_template_patcher.start()
        trail_env = MagicMock()
        trail_env.output = MagicMock()
        context = MagicMock()
        context.mock_attribute = MagicMock(return_value='expected value 1')

        templating_function = make_context_attribute_based_templating_function(
            'mock_attribute', 'mock instruction using {value}', expected_value='expected value 2', notify_user=False)
        return_value = templating_function(trail_env, context)

        self.assertEqual(mock_apply_object_attributes_to_template.call_count, 0)
        self.assertEqual(return_value, None)
        self.assertEqual(len(trail_env.mock_calls), 0)

        apply_object_attributes_to_template_patcher.stop()

    def test_stream_reader(self):
        # Since stream_reader is a non-terminating worker, we need to raise an Exception for it to stop.
        # This is done using the side_effect attribute of MagicMock.
        stream = MagicMock()
        stream.readline = MagicMock(side_effect = ['', 'value1', ' ', 'value2', '\n', QueueEmpty])
        queue = MagicMock()

        with self.assertRaises(QueueEmpty):
            stream_reader(stream, queue)

        self.assertEqual(stream.readline.call_count, 6)
        queue.put.assert_any_call('value1')
        queue.put.assert_any_call('value2')
        self.assertEqual(queue.put.call_count, 2)

    def test_stream_writer(self):
        # Since stream_writer is a non-terminating worker, we need to raise an Exception for it to stop.
        # This is done using the side_effect attribute of MagicMock.
        queue = MagicMock()
        queue.get = MagicMock(side_effect = ['', 'value1', ' ', 'value2', '\n', QueueEmpty])
        stream = MagicMock()

        with self.assertRaises(QueueEmpty):
            stream_writer(queue, stream)

        self.assertEqual(queue.get.call_count, 6)
        stream.write.assert_any_call('value1')
        stream.write.assert_any_call('value2')
        # stream_writer should write all the values, even empty ones like '' and '\n'.
        self.assertEqual(stream.write.call_count, 5)

    def test_create_stream_listener(self):
        queue_patcher = patch('autotrail.helpers.step.ProcessQueue')
        mock_queue = queue_patcher.start()
        process_patcher = patch('autotrail.helpers.step.Process')
        mock_process = process_patcher.start()
        stream = MagicMock()

        listener, queue = create_stream_listener(stream)

        mock_process.assert_called_once_with(target=stream_reader, args=(stream, mock_queue()))

        mock_process().start.assert_called_once_with()
        self.assertEqual(listener, mock_process())
        self.assertEqual(queue, mock_queue())

        process_patcher.stop()
        queue_patcher.stop()

    def test_create_stream_writer(self):
        process_patcher = patch('autotrail.helpers.step.Process')
        mock_process = process_patcher.start()
        queue = MagicMock()
        stream = MagicMock()

        writer_process = create_stream_writer(queue, stream)

        mock_process.assert_called_once_with(target=stream_writer, args=(queue, stream))

        mock_process().start.assert_called_once_with()
        self.assertEqual(writer_process, mock_process())

        process_patcher.stop()

    def test_run_shell_command(self):
        popen_patcher = patch('autotrail.helpers.step.Popen')
        mock_popen = popen_patcher.start()

        command_process, stdin, stdout, stderr = run_shell_command('ls')

        mock_popen.assert_called_once_with(['ls'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
        self.assertEqual(command_process, mock_popen())
        self.assertEqual(stdout, command_process.stdout)
        self.assertEqual(stderr, command_process.stderr)

        popen_patcher.stop()

    def test_tail_command(self):
        command_process = MagicMock()
        command_process.poll = MagicMock(side_effect=[None, 0])
        stdout_queue = MagicMock()
        stderr_queue = MagicMock()

        get_messages_from_queue_patcher = patch('autotrail.helpers.step.get_messages_from_queue')
        mock_get_messages_from_queue = get_messages_from_queue_patcher.start()

        messages_tuples = list(tail_command(command_process, stdout_queue, stderr_queue, delay=0.1))

        self.assertEqual(messages_tuples, [(mock_get_messages_from_queue(), mock_get_messages_from_queue())])
        get_messages_from_queue_patcher.stop()

    def test_get_messages_from_queue(self):
        queue = MagicMock()
        queue.get_nowait = MagicMock(side_effect=['mock message', QueueEmpty])

        messages = get_messages_from_queue(queue)

        self.assertEqual(messages, ['mock message'])
