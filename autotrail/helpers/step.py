"""Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.

Step and action function helpers

This module defines some helpers that make writing action functions and defining steps very easy.
"""


import shlex
import string

from functools import wraps
from multiprocessing import Process
from multiprocessing import Queue as ProcessQueue
from Queue import Empty as QueueEmpty
from subprocess import Popen, PIPE
from time import sleep

from autotrail.core.dag import Step


def create_conditional_step(action_function, **tags):
    """Factory to create a Step that acts like a guard to a branch.
    If this step fails, all subsequent steps in that branch, will be skipped.

    Consider the following trail where 'b' is the "conditional" step.

                   +--> d -->+
         +--> b -->|         |-->+
         |         +--> e -->+   |
    a -->|                       |--> f
         +--> c --------------- >+

    Lets say we want the branch "b" to run only if some condition is matched.
    We can consider the step 'b' to be the 'conditional' for this branch, i.e., it should succeed only if the condition
    is satisfied. If the condition is not satisfied, it will fail.
    Failure of 'b' will have the effect of skipping the progeny i.e., if 'b' fails, steps d and e will be "skipped".

    Progeny here is strict i.e., progeny of 'b' are 'd' and 'e' but not 'f' (since 'f' has a parent that is not related
    to 'b').

    This is done by setting two of the step's attributes:
    Step.pause_on_fail = True           -- So that the step fails instead of being moved to Step.PAUSE.
    Step.skip_progeny_on_failure = True -- So that the progeny are skipped on failure.

    Returns:
    Step object whose pause_on_fail is False and skip_progeny_on_failure is True.
    """
    step = Step(action_function, **tags)
    step.pause_on_fail = False
    step.skip_progeny_on_failure = True
    return step


def _create_arguments_dictionary(parameter_extractors, environment, context):
    """Creates a dictionary of arguments (to pass to an action function) using the parameters and the
    parameter_extractor_mapping as a guide and extracts the values from environment and context.

    Arguments:
    parameter_extractors        -- A list of functions used to extract and prepare the parameters.
                                   Each of these functions should:
                                   1. Accept 2 arguments: the TrailEnvironment and context. If there is no context,
                                      None will be passed as an argument.
                                   2. They must return a dictionary whose key is one of the parameters.
                                   The dictionary created from all of the parameter_extractors is then used as kwargs
                                   to the function_or_method. See example usage for more clarity.
    environment                 -- The TrailEnvironment object passed to an action function.
    context                     -- The user-defined context object passed to the action function.

    Returns:
    A dictionary containing the values retrieved from environment and context using each of the functions from
    parameter_extractors.
    The dictionary is of the form:
    {
        '<Parameter>': <Value>,
    }
    """
    arguments = {}
    for parameter_extractor in parameter_extractors:
        arguments.update(parameter_extractor(environment, context))
    return arguments


def _run_return_handlers(returned, context, return_handlers):
    """Call each function in the return_handlers list by passsing them the returned and context objects.

    Arguments:
    returned        -- The object returned by the function being wrapped using extraction_wrapper.
                       The type of this object doesn't matter but should be compatible with the behaviour expected by
                       each of the return_handlers.
    context         -- The context object.
    return_handlers -- A list of return handler functions:
                       [<Return handler>, ...]
                       Each of these functions should:
                       1. Accept 2 arguments: the returned value and the context object.
                          E.g., lambda retval, context: #code
                       2. Their return values are not used. However, they can produce side-effects by updating the
                          context objects.
                       Each return handler function is called one after the other so that each function can extract one
                       value from the returned object and update the context object. This allows each of these to be
                       lambdas in most cases and doesn't require them to be complicated functions.
    """
    for return_handler in return_handlers:
        return_handler(returned, context)


def extraction_wrapper(function_or_method, parameter_extractors, function=True, return_handlers=None,
                       return_value_extractor=lambda rval, context: rval,
                       exception_handlers=None):
    """Action function decorator that replaces the action function with a function that accepts TrailEnvironment and
    Context and extracts the values using the provided parameter_extractor_mapping before calling the action function
    it decorates.

    In reality, this is a function that returns a decorator, which in-turn handles the call to the action function.

    Arguments:
    function_or_method          -- A free-standing function or the method of an object that needs to be wrapped.
                                   Unlike the requirement of a Step, this doesn't have to be the __call__ method but
                                   any method.
    function                    -- Boolean - True represents the object passed as function_or_method is a free-standing
                                   function.
                                   False means the object passed as function_or_method is a method of an object.
                                   Defaults to True. Methods are handled differently because they have a 'self'
                                   argument.
    parameter_extractors        -- A list of functions used to extract and prepare the parameters for the given
                                   function_or_method.
                                   Each of these functions should:
                                   1. Accept 2 arguments: the TrailEnvironment and context. If there is no context,
                                      None will be passed as an argument.
                                   2. They must return a dictionary whose key is one of the parameters of the given
                                      function_or_method.
                                   The dictionary created from all of the parameter_extractors is then used as kwargs
                                   to the function_or_method. See example usage for more clarity.
    return_handlers             -- A list of return handler functions:
                                   [<Return handler>, ...]
                                   Each of these functions should:
                                   1. Accept 2 arguments: the returned value and the context object.
                                      E.g., lambda retval, context: #code
                                   2. Their return values are not used. However, they can produce side-effects by
                                      updating the context objects with any method calls.
                                   Each return handler function is called one after the other so that each function
                                   can extract one value from the returned object and update the context object. This
                                   allows each of these to be lambdas in most cases and doesn't require them to be
                                   complicated functions.
    return_value_extractor      -- A function that accepts returned value and context and returns a value that will be
                                   the final return value of the action function. This return value is returned as a
                                   result of the step running the action function.
                                   Defaults to returning the value returned as-is by using: lambda rval, context: rval
    exception_handlers          -- A list of tuples containing the exception class and the function that will be called
                                   to handle an exception of that class.
                                   'isinstance' is used to check if the exception class matches and ordering is
                                   respected, so, these need to be ordered in the same "layered" manner as is done in
                                   Python code i.e., a narrow exception hander needs to come before the broader handler
                                   if they are subclassed.
                                   Once a match is found, no subsequent handlers will be checked.
                                   Example form:
                                       [(<Exception class 1>, <Handler function 1>), ...]
                                   The functions associated with the exception class should:
                                   1. Accept 2 arguments: the exception object and the context object.
                                      E.g., lambda e, context: #code
                                   2. Their return values are considered the return value of the step.
                                   If an exception is not found in list, it goes unhandled and it will bubble up,
                                   causing the step to fail.

    Returns:
    An action function that can be used to construct a Step.
    The returned function accepts 2 parameters: TrailEnvironment and context and therefore can be used to create a Step.

    Usage:
    def example_usage(value, trail_output, default='No'):
        # Do something.
        return some_value

    step_example_usage = Step(extraction_wrapper(
        example_usage,

        # The parameter extraction functions eventually produce the following dictionary:
        # {
        #     'value': context.get_value(),
        #     'trail_output': environment.output,
        #     'default': 'Yes',
        # }
        # Which are then passed as kwargs to example_usage having the same effect as:
        #     example_usage(value=context.get_value(), trail_output=environment.output, default='Yes')
        parameter_extractors=[
            # The 'value' is obtained by calling context.get_value()
            lambda env, context: dict(value=context.get_value()),

            # The 'trail_output' object is obtained from the TrailEnvironment object passed by AutoTrail.
            lambda env, context: dict(trail_output=env.output),

            # Supply the keyword argument value='yes' to example_usage function without using either context or
            # TrailEnvironment.
            lambda env, context: dict(default='Yes'),
        ],

        # Infact, the above can be simplified to the following:
        # parameter_extractors=[
        #    lambda env, context: dict(value=context.get_value(), trail_output=env.output, default='Yes'))
        # ],

        # Here, cleanup_func will be called if SomeException is raised.
        exception_handlers=[(SomeException, lambda e, context: cleanup_func(context.resource))],

        # The returned value is stored in the context object using the set_value method.
        return_handlers=[lambda rval, context: context.set_value(rval)],

        # The final return value is the value returned by example_usage but converted to string.
        return_value_extractor=lambda rval, context: str(rval)))

    This is helpful when writing action functions and retain a readable signature.
        def example_usage(value, trail_output, default='No')
    Is more readable than:
        def example_usage(environment, context):
    Because in the latter case, it is not immediately clear what values are being used.
    """
    if return_handlers is None:
        return_handlers = []

    if exception_handlers is None:
        exception_handlers = []

    @wraps(function_or_method)
    def args_replacing_func(*args):
        if function:
            # Case where function_or_method is a free-standing function.
            environment, context = args
            func_call = function_or_method
        else:
            # Case where function_or_method is bound to an instance of a class.
            # 'self' will be passed by Python, and it needs to be passed to the function_or_method.
            self, environment, context = args
            func_call = lambda **kwargs: function_or_method(self, **kwargs)

        arguments = _create_arguments_dictionary(parameter_extractors, environment, context)

        try:
            returned = func_call(**arguments)
        except Exception as e:
            for exception_class, exception_handler in exception_handlers:
                if isinstance(e, exception_class):
                    return exception_handler(e, context)
            raise

        _run_return_handlers(returned, context, return_handlers)
        return return_value_extractor(returned, context)
    return args_replacing_func


def accepts_context(func):
    """Allows defining an action function that accepts only a context and doesn't accept the TrailEnvironment.

    Example:
    @accepts_context
    def action_function_taking_only_context(context):
        # Do something with context
        pass
    """
    @wraps(func)
    def func_accepts_context(trail_env, context):
        return func(context)
    return func_accepts_context


def accepts_environment(func):
    """Allows defining an action function that accepts only theTrailEnvironment and doesn't accept a context.

    Example:
    @accepts_environment
    def action_function_taking_only_environment(trail_env):
        # Do something with trail_env
        pass
    """
    @wraps(func)
    def func_accepts_environment(trail_env, context):
        return func(trail_env)
    return func_accepts_environment


def accepts_nothing(func):
    """Allows defining an action function that accepts nothing.
    It ignores both TrailEnvironment and context.

    Example:
    @accepts_nothing
    def action_function_takes_nothing():
        # Do something
        pass
    """
    @wraps(func)
    def func_accepts_nothing(trail_env, context):
        return func()
    return func_accepts_nothing


class InstructionNotCompletedError(RuntimeError):
    """A run-time exception raised by the Instruction class to indicate that the instruction sent to the user could not
    be completed.

    When an instruction is sent to the user as a message, (with reply_needed as True), the user is expected to reply to
    the message.
    A True value means the instruction was completed by the user.
    A False value means the instruction could not be completed for whatever reason.
    This is raised when the user returns a False value.
    """
    pass


class Instruction(object):
    """Provides the skeleton for a Runbook instruction-like action_function.

    The class creates an action function that ensures the given instruction is sent to the user.

    Arguments:
    name                -- This is a string that represents the name of the action function.
    instruction_function -- This is a function which will be called with the environment and context objects and
                           should return the instruction (string) that will be sent to the user or None.
                           Example:
                              lambda env, context: 'Run the command: /tmp/command_to_run {}'.format(context.value)
                           If a string is returned by the instruction_function, the string is sent to the user as
                           an instruction.
                           If None is returned by the instruction_function, then no instruction is sent. This is useful
                           for conditional instructions ie., an instruction should be sent based on some context
                           attribute's value.
    reply_needed        -- boolean - Changes behaviour as follows:
                           If True : The action function returns *only* when the user has responded to the message
                                     (marking it done). The reply message needed by this action function is the boolean
                                     True.
                                     False indicates that the instruction could not be completed and the action
                                     function will be considered failed.
                           If False: The action function doesn't wait for the user's response.

    Raises:
    AttributeError      -- If the context object does not have an attribute required by the template (if any).

    Example #1:
        # An instruction that needs a reply from the user.
        instruction_step = Step(Instruction('instruction_1', 'This is instruction #1.'))

    Example #2:
        # An instruction that does not need any reply from the user.
        instruction_step = Step(Instruction('instruction_1', 'This is instruction #1.', reply_needed=False))

    Example #3:
        # An instruction that uses templates
        instruction_step = Step(Instruction('instruction_1', 'This is a templated instruction: {value}'))
        # The attribute 'value' will be extracted from context and applied to the above template.
    """
    def __init__(self, name, instruction_function, reply_needed=True):
        self.name = name
        self.instruction_function = instruction_function
        self.reply_needed = reply_needed

    def __str__(self):
        return str(self.name)

    def __call__(self, trail_env, context):
        """This method is the one called when AutoTrail runs this action function (an instance of this class).

        It accepts TrailEnvironment and context for compliance and makes no use of context.

        Arguments:
        trail_env -- An object of type TrailEnvironment. (passed by AutoTrail)
        context   -- A user defined object passed to AutoTrail during runtime. (passed by AutoTrail)

        Returns:
        String -- Message expressing success.
        Raises InstructionNotCompletedError expresing failure.
        """
        message = self.instruction_function(trail_env, context)

        if message is None:
            return 'No instruction to send.'

        if self.reply_needed:
            done = trail_env.input(message)
            if done is not True:
                raise InstructionNotCompletedError('This step could not be completed based on response received.')
            else:
                return 'Step completed successfully based on response received.'
        else:
            trail_env.output(message)
            return 'Output sent.'


class ShellCommandFailedError(RuntimeError):
    """A run-time exception raised by the ShellCommand class to indicate that the execution of the shell command failed.

    When a shell command is run, its failure is identified by:
    1. The exit code of the command (as identified by the predicate function provided by the user).
    2. The presence of a specific error in STDERR (as identified by the predicate function).

    This exception is raised when the command fails due to either of the above reasons.
    """
    pass


class ShellCommand(object):
    """Provides the skeleton to run a shell command.

    The class creates an action function that runs a shell command and tails its output.
    Any output sent to STDOUT or STDERR by the shell command is then comunicated to the user using messages.

    Arguments:
    name                   -- This is a string that represents the name of the action function.
    command_function       -- This is a function which will be called with the environment and context objects and
                              should return the command (string) that will be run or None.
                              Example:
                                lambda env, context: '/tmp/command_to_run --value {}'.format(context.value)
                              When a string is returned by the command_function, the command is executed.
                              If None is returned by the command_function, then nothing is executed. This is useful
                              for conditional execution ie., a command should be run based on some context attribute's
                              value.

    Keyword Arguments:
    delay                  -- This is the delay with which the STDOUT is checked for new output.
                              Defaults to 1.
    is_error_ignorable     -- Predicate function that returns True when an error in STDERR can be ignored (not shown to
                              the user).
                              Defaults to: Ignore only false values (like blank lines).
                              Signature of the predicate function:
                                Return Type: Boolean (True or False)
                                Arguments  : String -- error message sent to STDERR.
    is_error_fatal         -- Predicate function that returns True when an error is considered FATAL ie., the action
                              function should FAIL if this error is encountered in STDERR.
                              Defaults to: Ignore all. This default option ensures that the action function does not
                              fail due to the presence of messages in STDERR but due to the exit code of the shell
                              command since it is quite likely that a command might print any warnings to STDERR
                              without failing.
                              Signature of the predicate function:
                                Return Type: Boolean (True or False)
                                Arguments  : String -- error message sent to STDERR.
    is_exit_code_ignorable -- Predicate function that returns True when the exit code of the shell command can be
                              ignored and not treated as failure.
                              Defaults to: Ignore nothing. Any non-zero exit code will cause the action function to
                              fail.
                              Signature of the predicate function:
                               Return Type: Boolean (True or False)
                               Arguments  : Int -- Exit code of the shell command.
    is_output_ignorable    -- Predicate function that returns True when a message in STDOUT can be ignored (not shown
                              to the user). This is useful when the shell command is verbose and not all output needs
                              to be sent to the user.
                              Defaults to: Ignore only false values (like blank lines).
                              Signature of the predicate function:
                                Return Type: Boolean (True or False)
                                Arguments  : String -- message sent to STDOUT.

    Example:
    shell_step = Step(ShellCommand('list_files', 'ls -l'))
    """
    def __init__(self, name, command_function, delay=1,
                 is_error_ignorable=lambda x: True if not x else False,
                 is_error_fatal=lambda x: False,
                 is_exit_code_ignorable=lambda x: x == 0,
                 is_output_ignorable=lambda x: True if not x else False):
        self.name = name
        self.command_function = command_function
        self.delay = delay

        # Predicate functions
        self.is_error_ignorable = is_error_ignorable
        self.is_error_fatal = is_error_fatal
        self.is_exit_code_ignorable = is_exit_code_ignorable
        self.is_output_ignorable = is_output_ignorable

    def __str__(self):
        return str(self.name)

    def send_messages(self, trail_env, prefix, messages):
        """Send messages to the user using TrailEnvironment.output.

        Prefixes the messages with the provided prefix and formats them as follows:
            [<prefix>] -- <message>
        Each message from messages is sent in the above format.

        Arguments:
        trail_env -- An object of type TrailEnvironment.
        prefix    -- String that will be prefixed before each message.
                     Used when messages need to be distinguised between STDOUT and STDERR.
        messages  -- List of strings.
        """
        for message in messages:
            trail_env.output('[{0}] -- {1}'.format(prefix, message))

    def get_valid_stdout_messages(self, stdout_messages):
        """Reads messages from self.stdout_queue and filters them.

        Uses the is_output_ignorable predicate function to filter out messages in STDOUT.

        Pre-condition:
        self.is_output_ignorable should refer to the predicate function.

        Arguments:
        stdout_messages -- List of messages read from STDOUT.

        Returns:
        List of strings -- List of filtered STDOUT messages.
        """
        return [message for message in stdout_messages if not self.is_output_ignorable(message)]

    def get_valid_stderr_messages(self, stderr_messages):
        """Reads messages from self.stderr_queue and filters them.

        Uses the is_error_ignorable predicate function to filter out messages in STDERR.

        Pre-condition:
        self.is_error_ignorable should refer to the predicate function.

        Arguments:
        stderr_messages -- List of messages read from STDERR.

        Returns:
        List of strings -- List of filtered STDERR messages.
        """
        return [message for message in stderr_messages if not self.is_error_ignorable(message)]

    def get_latest_fatal_error(self, stderr_messages):
        """Returns the last fatal error encountered in stderr_messages.

        Fatal errors are identified using the is_error_fatal predicate function.
        This method iterates over stderr_messages and returns the last identified fatal error.

        Pre-condition:
        self.is_error_fatal should refer to the predicate function.

        Arguments:
        stderr_messages -- List of strings - messages collected from STDERR.

        Returns:
        string          -- Containing the last encountered fatal error.
        None            -- If no errors were encountered.
        """
        fatal_message = None
        for error_message in stderr_messages:
            if self.is_error_fatal(error_message):
                fatal_message = error_message
        return fatal_message

    def generate_failure_message(self, command, command_process, fatal_error):
        """Generates a user-friendly message in case of failure of the
        shell command.

        Arguments:
        command         -- String - The command to be run.
        command_process -- A Popen object that refers to the process running the shell command.
        fatal_error     -- This is either None or a string containing the last known fatal error.

        Returns:
        string -- A message of the format:
            [Command: <command>] -- Failed with exit code: <exit code> STDERR (Last line): <fatal error>
        """
        return '[Command: {command}] -- Failed with exit code: {exit_code}, STDERR (Last line): {error}'.format(
            command=command, exit_code=command_process.poll(), error=fatal_error)

    def generate_success_message(self, command, command_process):
        """Generates a user-friendly message in case of successs of the shell command.

        Arguments:
        command         -- String - The command to be run.
        command_process -- A Popen object that refers to the process running the shell command.

        Returns:
        string -- A message of the format:
            [Command: <command>] -- Succeeded with exit code: <exit code>
        """
        return '[Command: {command}] -- Succeeded with exit code: {exit_code}.'.format(
            command=command, exit_code=command_process.poll())

    def was_run_successful(self, command_process, fatal_error):
        """This method returns true if the run of the shell command was successful.

        Success or Failure depends on two criteria:
        1. If the exit code can be ignored (Default: exit code 0 is ignored.)
        2. If no fatal error was encountered (Default: All errors are non-fatal)
        This means that by default, if a shell command exits with an exit code of 0,
        it is considered successful irrespective of the presence of any messages in STDERR.

        Pre-condition:
        self.is_exit_code_ignorable should refer to the predicate function.

        Arguments:
        command_process -- A Popen object that refers to the process running the shell command.
        fatal_error     -- This is either None or a string containing the last known fatal error.

        Returns:
        boolean         -- True when the run was successful. False otherwise.
        """
        exit_code = command_process.poll()
        return (self.is_exit_code_ignorable(exit_code) and fatal_error is None)

    def __call__(self, trail_env, context):
        """This method is the one called when AutoTrail runs this action function (an instance of this class).

        It accepts TrailEnvironment and context for compliance and makes no use of context.

        This method contains the business logic and makes use of all the other methods and helper functions to achieve
        the following:
        1) Notify the user about staring.
        2) Run the shell command in a subprocess.
        3) Tail the STDOUT and STDERR and filter them as defined by the predicate functions and send them to the user.
        4) Based on the predicate functions, determine if the run was successful or failed and notify the user of the
           same.
        5) Notify AutoTrail about success and failure by either returning or raising AssertionError.

        Arguments:
        trail_env -- An object of type TrailEnvironment. (passed by AutoTrail)
        context   -- A user defined object passed to AutoTrail during runtime. (passed by AutoTrail)

        Returns:
        String    -- Message expressing success.
        Raises    -- ShellCommandFailedError expresing failure.
        """
        # Make a command using the provided command template and the values provided in the context (if any).
        command = self.command_function(trail_env, context)
        if command is None:
            return 'No command to run.'

        # This contains the last fatal error seen in STDERR. Will remain None if all errors encountered are non fatal.
        # See class documentation to understand what fatal errors are.
        latest_fatal_error = None

        trail_env.output('[Command: {}] -- Starting.'.format(command))

        command_process, stdin, stdout, stderr = run_shell_command(command)

        # Start listeners, which will asynchronously collect any messages from STDOUT and STDERR and put them into
        # queues.
        stdout_listener, stdout_queue = create_stream_listener(stdout)
        stderr_listener, stderr_queue = create_stream_listener(stderr)

        # Start the writer, which will asynchronously collect any messages from the user and write then into STDIN.
        # All messages sent by the user are available in trail_env.input_queue.
        stdin_writer = create_stream_writer(trail_env.input_queue, stdin)

        # Tail the command and asynchronously collect messages in STDOUT and STDERR
        for stdout_messages, stderr_messages in tail_command(command_process, stdout_queue, stderr_queue,
                                                             delay=self.delay):
            # Filter the messages based on user-provided predicate functions.
            valid_stdout_messages = self.get_valid_stdout_messages(stdout_messages)
            valid_stderr_messages = self.get_valid_stderr_messages(stderr_messages)

            # Notify user.
            self.send_messages(trail_env, 'STDOUT', valid_stdout_messages)
            self.send_messages(trail_env, 'STDERR', valid_stderr_messages)

            latest_fatal_error = self.get_latest_fatal_error(valid_stderr_messages)

        # Terminate the listeners and writers because they don't have any termination logic in them and need to be
        # terminated explicitly.
        stdout_listener.terminate()
        stderr_listener.terminate()
        stdin_writer.terminate()

        if self.was_run_successful(command_process, latest_fatal_error):
            message = self.generate_success_message(command, command_process)
            trail_env.output(message)
            return message
        else:
            message = self.generate_failure_message(command, command_process, latest_fatal_error)
            trail_env.output(message)
            raise ShellCommandFailedError(message)


def make_simple_templating_function(template):
    """Factory for making a templating function that applies the context object to the given template to create a
    string.

    Arguments:
    template -- Templated string, whose values will be filled at run-time from the passed context.
                For example, the templated string:
                'ls -l {filename}'
                Will result in a string where the {filename} value will be populated by using "context.filename"
                attribute of context.
                If no context is passed, templating will have no effect.
                If no templating is done, context values will have no effect.

    Returns:
    An templating function that accepts TrailEnvironment and context objects, returning a string.

    Usage:
        make_simple_templating_function('ls -l {filename}')
    Will return a function that accepts TrailEnvironment and context. When called, it will return the string:
        'ls -l <filename obtained using getattr(context, "filename")>'
    """
    return lambda trail_env, context: apply_object_attributes_to_template(template, context)


def make_context_attribute_based_templating_function(attribute_name, template, expected_value=True, notify_user=True):
    """Factory for making a conditional templating function that:
    1. Obtains the attribute_name from the context object by calling getattr(<context>, attribute_name).
    2. Compares this value with the expected_value.
    3. If the values match, apply the context object to the template and return the string.
    4. If the values do not match, do nothing, return None. (Sends a message to the user using TrailEnvironment.output
       if notify_user is True)

    Arguments:
    attribute_name -- string - The attribute's name in the context object, whose value will be used to decide
                      whether and instruction will be sent to the user or not.
    expected_value -- Any comparable object. This value will be compared with the value obtained from context.
                      Defaults to True.
    template       -- Templated string, whose values will be filled at run-time from the passed context.
                      Example:
                        'ls -l {filename}'
                      The {filename} value will be populated by using "context.filename" attribute of context.
                      If no context is passed, templating will have no effect.
                      If no templating is done, context values will have no effect.

    Returns:
    An templating function that accepts TrailEnvironment and context objects, returning a string.

    Usage:
        make_context_attribute_based_templating_function('list_file', 'ls -l {filename}')
    Will return a function that accepts TrailEnvironment and context. When called, it will:
    1. Extract the 'list_file' attribute from the context.
    2. If this value == True, it will return the string:
        'ls -l <filename obtained using getattr(context, "filename")>'
       Otherwise, it will return None. (And notify the user using TrailEnvironment.output)
    """
    def templating_function(trail_env, context):
        attribute_value = getattr(context, attribute_name)
        if attribute_value == expected_value:
            return apply_object_attributes_to_template(template, context)
        elif notify_user:
            trail_env.output(('Nothing to do since "{attribute_name}" in context has the value: "{value}" and the '
                              'expected value is: "{expected_value}".').format(
                                attribute_name=attribute_name, value=attribute_value, expected_value=expected_value))
    return templating_function


def apply_object_attributes_to_template(template, value_object):
    """Generate a string from the template by applying values from the given object.

    If the template provided is not a template (not have any placeholders), this will not have any effect and template
    will be returned unchanged.
    If value_object is None, this will not have any effect.

    Arguments:
    template        -- A string that may or may not be templated.
                       If templated, the placeholders will be populated with values from the attributes of the
                       value_object.
    value_object    -- Any object that supports the __dict__ method. Most classes have a __dict__ method that return a
                       mapping of all the attributes and the associated values.

    Returns:
    string          -- This will be the template, with the values from value_object applied.
    """
    # Parse the template and extract the field names.
    # We'll use the field names to explicitly look-up attributes in the value_object.
    # The reason for this is that it works for @property attributes as well as normal attributes.
    field_names = [field_name for _, field_name, _, _ in string.Formatter().parse(template) if field_name is not None]

    template_values = {}
    for field_name in field_names:
        try:
            template_values[field_name] = getattr(value_object, field_name)
        except AttributeError as e:
            raise AttributeError(('Unable to apply object to template. Could not look-up attribute \'{}\' in the '
                                  'object \'{}\'. Error: {}').format(field_name, str(value_object), str(e)))
    return template.format(**template_values)


def stream_reader(stream, queue):
    """This is a worker that is used by ShellCommand to tail STDOUT and STDERR of a running command.
    This worker doesn't have any termination logic and needs to be terminated by the caller.

    Arguments:
    stream -- A stream to read from. Like subprocess.PIPE. Must support readline() method.
    queue  -- A multiprocessing.Queue object into which this function will be writing messages from the stream to.
    """
    while True:
        line = stream.readline().strip()
        if line:
            queue.put(line)


def stream_writer(queue, stream):
    """This is a worker that constantly reads from the given queue and writes to the given stream the moment any
    messages arrive. This is used by ShellCommand to tail the an input queue and write the arriving messages to STDIN.
    This worker doesn't have any termination logic and needs to be terminated by the caller.

    Arguments:
    queue  -- A multiprocessing.Queue object from which this function will be reading messages and writing into the
              stream.
    stream -- A stream to write to. Like subprocess.PIPE. Must support write() method.
    """
    while True:
        message = queue.get()
        stream.write(message)


def create_stream_writer(queue, stream):
    """Runs a writer to tail a queue and write to a stream.

    When the shell command is run with Popen, we need a way to asynchronously and non-blockingly receive messages sent
    by the user and write to the STDIN of the running process. This is achieved by running the stream_writer function
    as a subprocess. Each such instance is called a writer.

    The writer reads from the given queue and as soon as it gets a message, it writes it to the given stream.
    Typically this is used to read from a multiprocessing.Queue like object and write to a stream like STDIN.

    This function creates and runs one instance of a writer.

    Arguments:
    queue  -- A multiprocessing.Queue object from which the writer will be reading messages and writing into the stream.
    stream -- A stream to write to. Like subprocess.PIPE. Must support write() method.

    Returns:
    writer_process -- A multiprocessing.Process object referring to the writer subprocess. This is needed to terminate
                      the writer since the writer worker contains no termination logic.
    """
    writer_process = Process(target=stream_writer, args=(queue, stream))
    writer_process.start()
    return writer_process


def create_stream_listener(stream):
    """Runs listeners to tail STDOUT and STDERR.

    When the shell command is run with Popen, we need a way to asynchronously and non-blockingly read STDOUT and STDERR.

    This is achieved by running the stream_reader function as a subprocess.
    Each such instance is called a listener. This function creates and runs such listeners:

    Arguments:
    stream -- A stream to read from. Like subprocess.PIPE. Must support readline() method.

    Returns:
    A tuple of the form: (listener_process, queue)
    Where:
    listener_process -- A multiprocessing.Process object referring to the listener subprocess. This is needed to
                        terminate the listener since the listener contains no termination logic.
    queue            -- A multiprocessing.Queue object into which the listener will be writing messages from the stream
                        to. This conversion from a stream like object to a queue like object allows one to read in a
                        non-blocking manner.
    """
    queue = ProcessQueue()
    listener_process = Process(target=stream_reader, args=(stream, queue))
    listener_process.start()
    return (listener_process, queue)


def run_shell_command(shell_command):
    """Run the shell command associated with this class.

    Uses Popen to run the command in a separate process.

    Arguments:
    shell_command -- A string containing the shell command.

    Returns:
    A tuple of the form: (command_process, stdin, stdout, stderr) Where:
        command_process -- A subprocess.Popen object referring to the subprocess that is running the shell command.
        stdin           -- A subprocess.PIPE object into which the STDIN messages can be written.
        stdout          -- A subprocess.PIPE object into which the STDOUT is being redirected.
        stderr          -- A subprocess.PIPE object into which the STDERR is being redirected.
    """
    command_as_list = shlex.split(shell_command)

    # Run the command as a subprocess
    command_process = Popen(command_as_list, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    stdin = command_process.stdin
    stdout = command_process.stdout
    stderr = command_process.stderr

    return (command_process, stdin, stdout, stderr)


def tail_command(command_process, stdout_queue, stderr_queue, delay=1):
    """Tail a running command and collect messages from STDOUT and STDERR queues and yield the collected messages with
    each iteration.

    Arguments:
    command_process -- A subprocess.Popen object referring to the subprocess that is running the shell command.
    stdout_queue    -- A multiprocessing.Queue object into which an STDOUT listener would be writing messages.
    stderr_queue    -- A multiprocessing.Queue object into which an STDERR listener would be writing messages.

    Keyword Arguments:
    delay           -- The delay in seconds between collecting messages from the STDOUT and STDERR queues.

    Returns:
    A generator that yields tuples of the form: (stdout_messages, stderr_messages) Where:
        stdout_messages -- A list of messages collected from STDOUT stream.
        stderr_messages -- A list of messages collected from STDERR stream.
    """
    while command_process.poll() is None:
        # Sleep has to go at the beginning of the loop because the subsequent statements outside of the loop assume
        # that the process has finished and all STDOUT and STDERR messsages have been collected. If sleep is at the end
        # of the loop and the process sends some messages to STDOUT or STDERR and exits within this delay, the next
        # iteration of the loop will not execute and the messages will be lost.
        sleep(delay)

        stdout_messages = get_messages_from_queue(stdout_queue)
        stderr_messages = get_messages_from_queue(stderr_queue)

        yield (stdout_messages, stderr_messages)


def get_messages_from_queue(queue):
    """Read messages from the given queue until it is empty.

    Arguments:
    queue -- A multiprocessing.Queue like object that supports get_nowait() method and raises Queue.Empty exception
             when there are no more messages to be read.

    Returns:
    list  -- A list of messages read from the queue.
    """
    messages = []
    while True:
        try:
            messages.append(queue.get_nowait())
        except QueueEmpty:
            break

    return messages
