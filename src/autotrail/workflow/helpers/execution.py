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

from functools import wraps
from multiprocessing import Process, Queue
from queue import Empty


logger = logging.getLogger(__name__)


def exception_safe_call(function, *args, **kwargs):
    """Run a function by catching any exceptions it may raise and return the return value and exception as a tuple.

    :param function:    Callable to be called with the args and kwargs given.
    :param args:        The *args for the callable.
    :param kwargs:      The **kwargs for the callable.
    :return:            A tuple of the form (<return_value>, <exception>) where:
                        <return_value> is the value returned by the callable.
                        <exception> is the exception raised by the callable. If no exception is raised, this will be
                                    None.
    """
    return_value = None
    exception = None
    try:
        return_value = function(*args, **kwargs)
    except Exception as e:
        logger.exception(e)
        exception = e

    return (return_value, exception)


def make_exception_safe(function):
    """Transparently wrap the given function by catching any exceptions it may raise and return the return value and
    exception as a tuple.

    :param function:    Callable to wrap.
    :return:            A callable which has the name and docstring of the wrapped function but it returns a tuple of
                        the form (<return_value>, <exception>) where:
                        <return_value> is the value returned by the callable.
                        <exception> is the exception raised by the callable. If no exception is raised, this will be
                                    None.
    """
    @wraps(function)
    def safe_function(*args, **kwargs):
        return exception_safe_call(function, *args, **kwargs)
    return safe_function


def call_with_result_callback(function, callback, args=None, kwargs=None):
    """Call a function with the given args and kwargs and then call the callback with the return value.

    :param function:    The function to be called with args and kwargs.
    :param callback:    The callable that will be called with the return value of the function.
    :param args:        Args for function.
    :param kwargs:      Kwargs for function.
    :return:            None.
    """
    args = args or tuple()
    kwargs = kwargs or {}
    callback(function(*args, **kwargs))


def run_function_as_execption_safe_subprocess(function, *args, **kwargs):
    """Run a function in a sub process, and send the return value and exceptions, using a multiprocessing.Connection.

    :param function: The function to be called.
    :param args:     Args for function.
    :param kwargs:   Kwargs for function.
    :return:         A tuple consisting of the following:
                     process:       The multiprocessing.Process object that is running the function.
                     result queue:  A read-only multiprocessing.Queue that will contain only one message; a tuple
                                    of the form:
                                    (<return_value>, <exception>)
                                    where:
                                        <return_value> is the value returned by the callable.
                                        <exception> is the exception raised by the callable.
                                                    If no exception is raised, this will be None.
    """
    result_queue = Queue()
    process = Process(
        target=call_with_result_callback,
        args=(make_exception_safe(function), result_queue.put),
        kwargs=dict(
            # Args and kwargs for the function go here.
            args=args,
            kwargs=kwargs))
    process.start()
    return process, result_queue


class ExceptionSafeSubProcessFunction:
    """Run a function in a sub process, provide mechanisms to manage the process and collect the result.

    When a function is run as a sub-process, it may have a return value on successful completion or might raise an
    exception. This class provides a way to collect this from the sub-process and perform other management tasks like
    starting, terminating etc.
    """
    def __init__(self, function):
        """Setup the function to run in a subprocess.

        :param function: The callable to run. This should accept the *args and **kwargs that will be passed during
                         the 'start' method call.
        """
        self._function = function
        self._process = None
        self._result_queue = None
        self._result = None

    def __str__(self):
        return str(self._function)

    def start(self, *args, **kwargs):
        """Run the function as a subprocess by passing the given args and kwargs.

        :param args:    The args compatible with the function this is initialized with.
        :param kwargs:  Keyword arguments compatible with the function this is initialized with.
        :return:        None
        """
        self._process, self._result_queue = run_function_as_execption_safe_subprocess(self._function, *args, **kwargs)

    def join(self):
        """Wait for the subprocess to finish."""
        self._process.join()

    def is_alive(self):
        """Check if the subprocess is running.

        This check is a transparent call to the sub-process object's 'is_alive' method. Do not use this to check if
        a function has returned since the communication with the sub-process takes place using multiprocessing.Queue
        objects; in some cases, when large objects are written to the queue, the process remains alive until the
        data from the queue is read.

        :return: True if the process is running. False otherwise.
        """
        return self._process is not None and self._process.is_alive()

    def get_result(self):
        """Obtain the result from running the function.

        :return: If the function has completed, this will return a tuple of the form:
                    (<return_value>, <exception>)
                    where:
                    <return_value> is the value returned by the function.
                    <exception> is the exception raised by the function. If no exception is raised, this will be
                                None.
                 If the function has not completed, this will return None.
        """
        if self._result is None and self._result_queue is not None:
            try:
                self._result = self._result_queue.get_nowait()
            except Empty:
                self._result = None
        return self._result

    def terminate(self):
        """Terminate the subprocess."""
        self._process.terminate()
