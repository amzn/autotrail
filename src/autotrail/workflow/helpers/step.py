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
import logging
import shlex

from multiprocessing import Process, Pipe
from subprocess import Popen, PIPE
from time import sleep

from autotrail.core.api.management import read_message, read_messages, send_messages
from autotrail.workflow.helpers.execution import ExceptionSafeSubProcessFunction


logger = logging.getLogger(__name__)


def is_dict_subset_of(sub_dict, super_dict):
    """Checks if sub_dict is contained in the super_dict.

    :param sub_dict:    A mapping. This is looked for in the super_dict.
    :param super_dict:  A mapping. May or may not contain the sub_dict.
    :return:            True, if all the key-value pairs of sub_dict are present in the super_dict. False otherwise.
    """
    return all(key in super_dict and super_dict[key] == value for key, value in sub_dict.items())


class Step(ExceptionSafeSubProcessFunction):
    """An ExceptionSafeSubProcessFunction that supports tags.

    Tags are arbitrary key-value pairs that can be associated with a step. These can be used to arbitrarily group
    steps together. For example, consider the following 4 steps:
        step_1 = Step(function_1, key1='value1', key2='value2')
        step_2 = Step(function_2, key1='value1', key3='value3')
        step_3 = Step(function_3, key2='value2', key3='value3')

    step_1 and step_2 can be collectively referred to with the tag {'key1': 'value1'}
    step_2 and step_3 can be collectively referred to with the tag {'key3': 'value3'}
    step_1 and step_3 can be collectively referred to with the tag {'key2': 'value2'}

    Such grouping is useful in workflows where one might want to perform API operations on a collection of steps
    that serve some common purpose.

    There are 2 tags that are automatically present for every Step:
        'n'     -- This is a unique number associated with a Step and is automatically incremented (starting with 0)
                   with the number of instances of Step. For example, step_1 will have n=0, step_2 will have n=1 and
                   so on.
                   The 'n' tag is stored as the 'id' instance attribute.
        'name'  -- A meaningful "name" for the step. If none is provided, it is automatically determined from the
                   contained callable's name.

    A step can be checked for a matching tag with the 'in' operator. E.g.,
        {'key1': 'value1'} in step_1 -> True
        {'key3': 'value3'} in step_2 -> True
    """
    unique_id = 0

    def __init__(self, function, **tags):
        """Setup the step by associating tags and the unique ID.

        Creation of Step objects is not thread-safe.

        :param function:    A callable.
        :param tags:        Arbitrary key-value pairs to be associated with this step. See class documentation.
        """
        super(Step, self).__init__(function)

        self.id = Step.unique_id
        Step.unique_id += 1

        self.tags = tags
        self.tags['n'] = self.id
        if 'name' not in tags:
            self.tags['name'] = self._make_name()

    def __str__(self):
        return self.tags['name']

    def __contains__(self, tags):
        return is_dict_subset_of(tags, self.tags)

    def _make_name(self):
        if hasattr(self._function, '__name__'):
            return self._function.__name__
        else:
            return str(self._function)


class ContextWrapper:
    """Wraps a Step-like object that can be called only with a context parameter.

    This wrapper imitates the behaviour of a Step except that the 'start' method accepts only the context parameter.
    Since a Step contains a callable that may accept *args, **kwargs; this wrapper uses a pre_processor to extract them.

    A Step runs the contained callable as a sub-process, therefore, it cannot make persistent changes to the context.
    The value returned, or the exception raised by the Step's callable is used by the post_processor to update the
    context.
    This is possible since the calls to the pre_processor and post_processor happen in the same process that keeps the
    shared context.

    If either the pre_processor or post_processor raise an exception, it will become the exception of the step.
    """
    def __init__(self, step, pre_processor=None, post_processor=None):
        """Setup the pre and post processors.

        :param step:                A Step like object.
        :param pre_processor:       A callable that accepts (step, context) as parameters and returns the tuple of
                                    *args and **kwargs that will be used when invoking the function contained in the
                                    step.
                                    If no pre_processor is provided, the step will be called with one parameter viz.,
                                    context.
        :param post_processor:      A callable that accepts:
                                        (step, context, <Step's return value>, <exception raised by the Step>)
                                    And returns a tuple consisting of:
                                        (<return_value>, <exception>).
                                    This <return_value> will be the return value of the step (since this class is a
                                    wrapper). Same goes for the <exception>.
                                    If no post_processor is provided:
                                    1. The step's return_value will be passed as-is.
                                    2. The step's exception will be passed as-is.
        """
        self._step = step
        self._pre_processor = pre_processor
        self._post_processor = post_processor

        self.tags = self._step.tags
        self.id = self._step.id

        self._context = None
        self._exception = None

    def __str__(self):
        return str(self._step)

    def __contains__(self, tags):
        return tags in self._step

    def start(self, context):
        """Run the contained function as a subprocess by passing the given context.

        :param context: The only argument to pass to the contained function.
        :return:        None
        """
        self._context = context
        if self._pre_processor:
            try:
                args, kwargs = self._pre_processor(self._step, self._context)
            except Exception as e:
                logger.exception('Failed to run pre-processor for step: {} due to error: {}'.format(
                    str(self._step), e))
                self._exception = e
                return

            try:
                self._step.start(*args, **kwargs)
            except Exception as e:
                logger.exception('Failed to start step: {} due to error: {}'.format(str(self._step), e))
                self._exception = e
        else:
            try:
                self._step.start(self._context)
            except Exception as e:
                logger.exception('Failed to start step: {} due to error: {}'.format(str(self._step), e))
                self._exception = e

    def join(self):
        """Wait for the subprocess to finish."""
        self._step.join()

    def is_alive(self):
        """Check if the subprocess is running.

        :return: True if the process is running. False otherwise.
        """
        return self._step.is_alive()

    def get_result(self):
        """Obtain the result from the function run.

        :return: If the function has completed, this will return a tuple of the form:
                    (<return_value>, <exception>)
                    where:
                    <return_value> is the value returned by the function.
                    <exception> is the exception raised by the function. If no exception is raised, this will be
                                None.
                 If the function has not completed, this will return None.
                 The post-processors and exception handlers will be called (if applicable) before producing the
                 above tuple.
        """
        if self._exception:
            return_value = None
            exception = self._exception
        else:
            result = self._step.get_result()
            if result:
                try:
                    return_value, exception = result
                except Exception as e:
                    logger.exception('Failed to extract result for step {} due to error: {}'.format(
                        str(self._step), e))
                    return_value, exception = None, e
            else:
                return result

        if self._post_processor:
            try:
                return_value, exception = self._post_processor(self._step, self._context, return_value, exception)
            except Exception as e:
                logger.exception('Failed to run post-processor for step {} due to error: {}'.format(
                    str(self._step), e))
                return_value, exception = None, e
        return return_value, exception

    def terminate(self):
        """Terminate the subprocess."""
        self._step.terminate()


class ChainPreProcessors:
    """Combiner for pre-processors.

    To understand what pre-processors are, please see the ContextWrapper class.
    """
    def __init__(self, pre_processors):
        """Combine the given pre_processors.

        :param pre_processors: An iterable of pre-processor callables each of which accept two parameters and return a
        tuple of *args and **kwargs.
        """
        self._pre_processors = pre_processors

    def __call__(self, *args, **kwargs):
        """Call the pre-defined pre-processor callables and return a tuple of *args and **kwargs.

        :param args:    These arguments must be accepted by each of the pre-processors.
        :param kwargs:  Keyword arugments that must be accepted by each of the pre-processors.
        :return:        A tuple of (args, kwargs) where args is a tuple and kwargs is a dictionary.
        """
        accumulated_args = []
        accumulated_kwargs = {}
        for pre_processor in self._pre_processors:
            try:
                processed_args, processed_kwargs = pre_processor(*args, **kwargs)
            except Exception as e:
                logger.exception('Failed to call pre-processor {} due to error {}'.format(pre_processor, e))
                raise
            accumulated_args.extend(list(processed_args))
            accumulated_kwargs.update(processed_kwargs)
        return tuple(accumulated_args), accumulated_kwargs


def make_contextless_step(function, **tags):
    """Factory to make a step for a function that doesn't accept a context. No parameters will be passed to the given
    function when it is called.

    :param function:    A callable that doesn't accept any parameters.
    :param tags:        Key-value pairs to be associated with the step.
    :return:            A Step-like object that:
                        1. Has the given tags associated with it.
                        2. Accepts (step, context) parameters to run.
                        3. Upon being called, calls the given function and returns its returned value or raised
                           exception.
    """
    return ContextWrapper(Step(function, **tags), pre_processor=lambda step, context: (tuple(), dict()))


def instruction(connection, message, timeout=None):
    """Send a message using the given connection and block until timeout for a response.

    :param connection:  A multiiprocessing.Connection like object that supports poll(<timeout>) and recv() methods.
    :param message:     Boolean. False means the instruction could not be completed.
    :param timeout:     The number of seconds to wait for a response to the instruction message.
    :return:            None if the boolean True is received via the connection.
    :raises:            ValueError if the boolean False is received via the connection.
    """
    connection.send(message)
    if connection.poll(timeout):
        answer = connection.recv()
        if answer is False:
            raise ValueError('The instruction could not be completed based on the response received.')
    else:
        raise RuntimeError('No response received.')


def output(connection, message):
    """Send message through the given connection.

    :param connection:  Must support the send(<message>) method. The return value is ignored.
    :param message:     The message that needs to be sent using the given connection.
    :return:            None
    """
    connection.send(message)


def io_preprocessor(step, context):
    """A pre-processor for a step that accepts a r/w connection to send and receive messages.

    :param step:    A Step like object that has an 'id' attribute.
    :param context: The context dictionary. Must have a 'step_data' key.
    :return:        A tuple to form *args, **kwargs as follows:
                        (<An r/w multiprocessing.connection object>, ),
                        {}
                    Side-effect: An r/w multiprocessing.connection object will be stored in the context under
                    'step_data' -> step.id -> 'rw_connection'.
    """
    connection_1, connection_2 = Pipe(duplex=True)
    context['step_data'].setdefault(step.id, {})['rw_connection'] = connection_2
    return (connection_1,), {}


def output_preprocessor(step, context):
    """A pre-processor for a step that accepts a write-only connection to send out messages.

    :param step:    A Step like object that has an 'id' attribute.
    :param context: The context dictionary. Must have a 'step_data' key.
    :return:        A tuple to form *args, **kwargs as follows:
                        (<A write-only multiprocessing.connection object>, ),
                        {}
                    Side-effect: An read-only multiprocessing.connection object will be stored in the context under
                    'step_data' -> step.id -> 'ro_connection'.
    """
    reader, writer = Pipe(duplex=False)
    context['step_data'].setdefault(step.id, {})['ro_connection'] = reader
    return (writer,), {}


def make_instruction_step(instruction_preprocessor, **tags):
    """Factory that makes a step that sends an instruction and succeeds/fails based on the response received.

    :param instruction_preprocessor:    A callable that accepts (step, context) and returns a tuple of *args and
                                        **kwargs where:
                                            args must be (<instruction as a string>,)
                                            kwargs may include {'timeout': <integer>}, which by default is None.
                                        It means the step will wait indefinitely for a response.
                                        The step will fail with RuntimeError if a response is not received within the
                                        timeout period.
    :param tags:                        Key-value pairs to be associated with the step.
    :return:                            A Step-like object that:
                                        1. Has the given tags associated with it.
                                        2. Accepts a context as the only parameter to run.
                                        3. Upon being called:
                                            3.1. Updates the context with an r/w multiprocessing.Connection under:
                                                 context['step_data'][<step.id>]['rw_connection']
                                            3.2 Upon being called, calls the instruction_preprocessor to generate an
                                                instruction.
                                            3.3. Sends the instruction using an r/w multiprocessing.Connection.
                                            3.4. Returns None if a boolean True message is received at the connection.
                                                 Raises ValueError if a boolean False message is received.
    """
    return ContextWrapper(Step(instruction, **tags),
                          pre_processor=ChainPreProcessors([io_preprocessor, instruction_preprocessor]))


def make_output_step(message_preprocessor, **tags):
    """Factory that makes a step that sends an output message built at run-time.

    :param message_preprocessor:    A callable that accepts (step, context) and returns a tuple of *args and **kwargs
                                    where:
                                        args must be (<output message as a string>,)
                                        kwargs must be empty {}
    :param tags:                    Key-value pairs to be associated with the step.
    :return:                        A Step-like object that:
                                        1. Has the given tags associated with it.
                                        2. Accepts a context as the only parameter to run.
                                        3. Upon being called:
                                            3.1. Updates the context with an read-only multiprocessing.Connection under:
                                                 context['step_data'][<step.id>]['ro_connection']
                                            3.2 Upon being called, calls the message_preprocessor to generate an
                                                output message.
                                            3.3. Sends the message using a write-only multiprocessing.Connection.
                                            3.4. Returns None.
    """
    return ContextWrapper(Step(output, **tags),
                          pre_processor=ChainPreProcessors([output_preprocessor, message_preprocessor]))


def make_simple_preprocessor(obj):
    """Factory that makes a function that accepts (step, context) and returns the given obj as *args and no kwargs.

    :param obj: Any object.
    :return:    A function that accepts (step, context) paramteres and returns a tuple (*args, **kwargs) in the
                following form:
                (
                    (obj,),
                    {}
                )
    """
    return lambda step, context: ((obj,), {})


class ShellCommandFailedError(RuntimeError):
    """A run-time exception raised by the ShellCommand class when the execution of the shell command fails."""
    pass


class ShellCommand:
    """A callable that runs a system command with connections to STDOUT and STDIN sanitized by given filters.

    Raises ShellCommandFailedError when either the error_filter or the exit_code_filter returns a True value.
    """
    def __init__(self, stdout_filter=lambda x: x, stderr_filter=lambda x: x, error_filter=lambda x: None,
                 exit_code_filter=lambda x: x, delay=1):
        """Setup the filters and delay to be used when running the system command.

        :param stdout_filter:       Filter function for STDOUT messages. This function should accept a string and
                                    return either a string or None. None means the message will be filtered out.
        :param stderr_filter:       Filter function for STDERR messages. This function should accept a string and
                                    return either a string or None. None means the message will be filtered out.
        :param error_filter:        Filter function to detect errors in STDOUT and STDERR combined. This function
                                    should accept a string and return either a string or None. None means the message \
                                    will be filtered out.
        :param exit_code_filter:    Filter function to detect errors in the system exit code. This function should
                                    accept a string and return either a string or None. None means the message will be
                                    filtered out.
        :param delay:               The delay with which to read/check for STDOUT/STDERR messages.
        """
        self.delay = delay

        self.stdout_filter = stdout_filter
        self.stderr_filter = stderr_filter
        self.error_filter = error_filter
        self.exit_code_filter = exit_code_filter
        self.__name__ = str(self)

    def _filter_messages(self, messages, message_filter):
        for message in messages:
            error = self.error_filter(message)
            if error:
                raise ShellCommandFailedError(error)
            message = message_filter(message)
            if message:
                yield message

    def __call__(self, output_writer, command, input_reader=None, shell=False):
        """Runs the system command, send STDOUT and STDERR messages to the output_writer and relay
        messages from the input_reader to STDIN.

        This callable does the following:
        1. Runs the system command as a sub-process.
        2. Reads the messages from STDOUT and STDERR.
        3. Check for errors. (See __init__). Raises ShellCommandFailedError if any errors are detected.
        4. Filter out STDOUT and STDERR messages (See __init__).
        5. Send the messages using the output_writer's send() method.
        6. Read any messages received at the input_reader and relay them to STDIN.
        7. Once the command completes, checks the exit code. Raises ShellCommandFailedError based on the exit code.

        :param output_writer:   A multiprocessing.Connection like object that supports a send(<utf-8 string>) method.
                                All output message (STDOUT and STDERR) will be written to this connection. They will be
                                formatted and prefixed with "STDOUT" and "STDERR" respectively.
        :param command:         The system command to run as a string. E.g., 'ls -l'
        :param input_reader:    A multiprocessing.Connection like object that supports the poll(<timeout>) and recv()
                                methods.
                                Any messages received here will be written to the STDIN of the command being run.
        :param shell:           The shell argument (which defaults to False) specifies whether to use the shell as the
                                program to execute.
        :return:                None.
                                Side-effect:
        """
        output_writer.send('[Command: {}] -- Starting...'.format(command))

        # If shell is True, it is recommended to pass args as a string rather than as a sequence. (Python docs)
        command = shlex.split(command) if shell is False else command
        command_process, stdin, stdout, stderr = run_shell_command(command, shell=shell)

        processes = [command_process]
        stdout_reader, stdout_writer = Pipe(duplex=False)
        stderr_reader, stderr_writer = Pipe(duplex=False)
        processes.append(create_stream_listener(stdout, stdout_writer))
        processes.append(create_stream_listener(stderr, stderr_writer))
        if input_reader:
            processes.append(create_stream_writer(input_reader, stdin))

        while True:
            # The process may be running at the start of the loop and may finish when the loop body is handling the
            # messages (e.g., the process may write messages to STDOUT when the messages from STDERR are being handled)
            # To avoid such messages being lost, we collect the process state before handling messages and decide to
            # continue the loop if the process was running when we started reading messages. Doing this later may cause
            # messages to be lost.
            exit_code = command_process.poll()

            try:
                send_messages(output_writer, ('[STDOUT] -- {}'.format(message) for message in self._filter_messages(
                    read_messages(stdout_reader, timeout=self.delay), self.stdout_filter)))
                send_messages(output_writer, ('[STDERR] -- {}'.format(message) for message in self._filter_messages(
                    read_messages(stderr_reader), self.stderr_filter)))
            except ShellCommandFailedError:
                terminate(processes)
                raise

            if exit_code is not None:
                break

        terminate(processes)

        if self.exit_code_filter(exit_code):
            raise ShellCommandFailedError('[Command: {command}] -- Failed with exit code: {exit_code}'.format(
                command=command, exit_code=exit_code))


def run_shell_command(command, shell=False):
    """Run the given system command in a separate process and provide handles to STDIN, STDOUT and STDERR.

    :param command: A string containing the shell command.
    :param shell:   The shell argument (which defaults to False) specifies whether to use the shell as the
                     program to execute.
    :return:        A tuple of the form: (command_process, stdin, stdout, stderr) Where:
                    command_process -- A subprocess.Popen object referring to the subprocess that is running the shell
                                       command.
                    stdin           -- A subprocess.PIPE object into which the STDIN messages can be written.
                    stdout          -- A subprocess.PIPE object into which the STDOUT is being redirected.
                    stderr          -- A subprocess.PIPE object into which the STDERR is being redirected.
    """
    command_process = Popen(command, stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=shell)

    stdin = command_process.stdin
    stdout = command_process.stdout
    stderr = command_process.stderr

    return command_process, stdin, stdout, stderr


def stream_reader(stream, connection, delay=0.1):
    """Read from the given stream (supporting the readline() method) and send the read messages through the given
    connection (supporting the send(<utf-8 string>) method).

    :param stream:      Must support the readline() method that returns bytes. Trailing newlines will be stripped.
    :param connection:  Must support the send(<utf-8 string>) method.
    :param delay:       Amount of time to wait before attempting to read the next line.
    :return:            None
    """
    while True:
        line = stream.readline().strip().decode("utf-8")
        if line:
            connection.send(line)
        sleep(delay)


def stream_writer(connection, stream):
    """Wait for messages in the given connection and write them to the stream (supporting write(<utf-8 string>) method.

    :param connection:  Must support the send(<utf-8 string>) method.
                        The read blocks until a message arrives. No sleep required.
    :param stream:      Must support the write(<utf-8 string>) and flush() methods.
    :return:            None
    """
    while True:
        message = read_message(connection, None)
        stream.write(message.encode("utf-8"))
        stream.flush()


def create_stream_writer(connection, stream):
    """Runs stream_writer as a multiprocessing.Process that waits for messages in the given connection and writes them
    to the stream (supporting write(<utf-8 string>) method.

    :param connection:  Must support the send(<utf-8 string>) method.
                        The read blocks until a message arrives. No sleep required.
    :param stream:      Must support the write(<utf-8 string>) and flush() methods.
    :return:            A multiprocessing.Process object referring to the writer subprocess.
    """
    writer_process = Process(target=stream_writer, args=(connection, stream))
    writer_process.start()
    return writer_process


def create_stream_listener(stream, connection, delay=0.1):
    """Runs stream_reader as a multiprocessing.Process that reads messages from the given stream (supporting the
    readline() method) and sends them through the given connection (supporting the send(<utf-8 string>) method).

    :param stream:      Must support the readline() method that returns bytes. Trailing newlines will be stripped.
    :param connection:  Must support the send(<utf-8 string>) method.
    :param delay:       Amount of time to wait before attempting to read the next line.
    :return:            A multiprocessing.Process object referring to the writer subprocess.
    """
    listener_process = Process(target=stream_reader, args=(stream, connection), kwargs=dict(delay=delay))
    listener_process.start()
    return listener_process


def terminate(processes):
    """Terminates the given processes by calling their terminate() method.

    :param processes:   An iterable over multiprocessing.Process like objects that support the terminate() method.
    :return:            None
    """
    for process in processes:
        try:
            process.terminate()
        except OSError:
            pass
