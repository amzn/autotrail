"""Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

"""
import unittest

from multiprocessing import Pipe, Process

from autotrail.core.api.management import read_messages
from autotrail.workflow.helpers.step import ShellCommand, ShellCommandFailedError, Step


DELAY = 0.5


class ShellCommandTests(unittest.TestCase):
    def test_successful_run(self):
        shell_command = ShellCommand(delay=DELAY)
        output_reader, output_writer = Pipe(duplex=False)
        shell_command(output_writer, 'echo "Hello"')
        self.assertEqual(list(read_messages(output_reader, timeout=DELAY)), [
            '[Command: echo "Hello"] -- Starting...',
            '[STDOUT] -- Hello'])

    def test_output_filter(self):
        shell_command = ShellCommand(stdout_filter=lambda x: x if 'Hello' not in x else None, delay=DELAY)
        output_reader, output_writer = Pipe(duplex=False)
        shell_command(output_writer, 'echo "Hello\nBye"')
        self.assertEqual(list(read_messages(output_reader, timeout=DELAY)), [
            '[Command: echo "Hello\nBye"] -- Starting...',
            '[STDOUT] -- Bye'])

    def test_output_tailing(self):
        shell_command = ShellCommand(delay=DELAY)
        output_reader, output_writer = Pipe(duplex=False)
        command = 'echo "Hello"; sleep 1; echo "Bye"'
        process = Process(target=shell_command, args=(output_writer, command), kwargs=dict(shell=True))
        process.start()

        self.assertEqual(list(read_messages(output_reader, timeout=DELAY)), [
            '[Command: {}] -- Starting...'.format(command),
            '[STDOUT] -- Hello'])
        process.join()
        self.assertEqual(list(read_messages(output_reader, timeout=DELAY)), ['[STDOUT] -- Bye'])

    def test_error_detection(self):
        shell_command = ShellCommand(error_filter=lambda x: x if 'Error' in x else None, delay=DELAY)
        output_reader, output_writer = Pipe(duplex=False)

        with self.assertRaises(ShellCommandFailedError):
            shell_command(output_writer, 'echo "Hello\nSome Error"')

        self.assertEqual(list(read_messages(output_reader, timeout=DELAY)), [
            '[Command: echo "Hello\nSome Error"] -- Starting...',
            '[STDOUT] -- Hello'])

    def test_exit_code_check(self):
        shell_command = ShellCommand(delay=DELAY)
        output_reader, output_writer = Pipe(duplex=False)
        filename = 'foo_bar_baz'
        command = 'ls {}'.format(filename)

        with self.assertRaises(ShellCommandFailedError):
            shell_command(output_writer, command)

        stdout = list(read_messages(output_reader, timeout=DELAY))
        self.assertEqual(stdout[0], '[Command: {}] -- Starting...'.format(command))
        self.assertIn('[STDERR] -- ', stdout[1])
        self.assertIn('No such file or directory', stdout[1])

    def test_error_filter(self):
        shell_command = ShellCommand(stderr_filter=lambda x: None if 'No such file' in x else x, delay=DELAY)
        output_reader, output_writer = Pipe(duplex=False)
        filename = 'foo_bar_baz'
        command = 'ls {}'.format(filename)

        with self.assertRaises(ShellCommandFailedError):
            shell_command(output_writer, command)

        self.assertEqual(list(read_messages(output_reader, timeout=DELAY)), [
            '[Command: {}] -- Starting...'.format(command),
        ])

    def test_exit_code_filter(self):
        shell_command = ShellCommand(exit_code_filter=lambda x: None if x != 0 else x, delay=DELAY)
        output_reader, output_writer = Pipe(duplex=False)
        filename = 'foo_bar_baz'
        command = 'ls {}'.format(filename)

        shell_command(output_writer, command)

        stdout = list(read_messages(output_reader, timeout=DELAY))
        self.assertEqual(stdout[0], '[Command: {}] -- Starting...'.format(command))
        self.assertIn('[STDERR] -- ', stdout[1])
        self.assertIn('No such file or directory', stdout[1])

    def test_stdin(self):
        shell_command = ShellCommand(delay=DELAY)
        output_reader, output_writer = Pipe(duplex=False)
        input_reader, input_writer = Pipe(duplex=False)
        input_writer.send('foo\n')
        shell_command(output_writer, 'read a; echo $a', input_reader=input_reader, shell=True)
        self.assertEqual(list(read_messages(output_reader, timeout=DELAY)), [
            '[Command: read a; echo $a] -- Starting...',
            '[STDOUT] -- foo'])


class ShellCommandStepTests(unittest.TestCase):
    def test_shell_command_step(self):
        shell_command = ShellCommand(delay=DELAY)
        output_reader, output_writer = Pipe(duplex=False)
        step = Step(shell_command)
        step.start(output_writer, 'echo "Hello"')
        step.join()

        self.assertFalse(step.is_alive())
        self.assertEqual(str(step), str(shell_command))
        self.assertEqual(list(read_messages(output_reader, timeout=DELAY)), ['[Command: echo "Hello"] -- Starting...',
                                                                             '[STDOUT] -- Hello'])
