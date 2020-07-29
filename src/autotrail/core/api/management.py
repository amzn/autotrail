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

from multiprocessing.connection import Listener, Client
from time import sleep


logger = logging.getLogger(__name__)


def read_message(connection, timeout=0.1):
    """Attempt to read a single message from the given multiprocessing.Connection object.

    :param connection: A multiprocessing.Connection like object that supports poll(<timeout>) and recv() methods.
    :param timeout:    The timeout (in seconds) while waiting for messages.
    :return:           The message received from the connection.
    """
    if connection.poll(timeout):
        try:
            return connection.recv()
        except EOFError:
            pass


def read_messages(connection, timeout=0.1):
    """Generator to read and iterate over all messages from the given multiprocessing.Connection object.

    :param connection: A multiprocessing.Connection like object that supports poll(<timeout>) and recv() methods.
    :param timeout:    The timeout (in seconds) while waiting for messages.
    :return:           The message received from the connection.
    """
    while True:
        message = read_message(connection, timeout)
        if message:
            yield message
        else:
            break


def send_messages(connection, messages):
    """Send messages to the given connection.

    :param messages:    An iterable of strings.
    :param connection:  A multiprocessing.Connection like object that supports the 'send(<message>)' method.
    :return:            None.
    """
    for message in messages:
        connection.send(message)


class APIRequest:
    """API request object containing the API method name, and the args and kwargs for it."""
    def __init__(self, method, args, kwargs):
        """Define the API method to be called and the args and kwargs for it.

        :param method:  API method to be called. This must be a public method of the API handler class.
        :param args:    *args for the API method.
        :param kwargs:  **kwargs for the API method.

        The args and kwargs needs to match the signature of the method in the API handler class.
        """
        self.method = method
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        return 'APIRequest({method}, {args}, {kwargs})'.format(
            method=self.method, args=self.args, kwargs=self.kwargs)

    def __str__(self):
        return str(repr(self))


class APIResponse:
    """Response to an API call. This is the response object sent to the requester of the API call.

    Objects of this class have the following attributes:
        return_value:   The value to be returned as a result of the API call.
        exception:      Any exception encountered as a result of the API call. Incorrect call etc.
    """
    def __init__(self, return_value, exception=None):
        """Define the API response.

        :param return_value:    The value to be returned as a result of the API call.
        :param exception:       Any exception encountered as a result of the API call. Incorrect call etc.
        """
        self.return_value = return_value
        self.exception = exception

    def __repr__(self):
        return 'APIResponse({return_value}, exception={exception})'.format(
            return_value=self.return_value, exception=self.exception)

    def __str__(self):
        return str(repr(self))


class APIHandlerResponse:
    """API handler's response to an API call.

    The API handler is called by the server with the API request received. The handler needs a way to communicate
    with the requester and the handler. Hence, objects of this class have the following attributes:
        return_value:   The value to be returned as a result of the API call.
        exception:      Any exception encountered as a result of the API call. Incorrect call etc.
        relay_value:    To be relayed to the server and not sent to the requester.

    This object is NOT sent to the requester.
    """
    def __init__(self, return_value, exception=None, relay_value=None):
        """Define the API response.

        :param return_value:    The value to be returned as a result of the API call.
        :param exception:       Any exception encountered as a result of the API call. Incorrect call etc.
        :param relay_value:     To be relayed to the server and not sent to the requester.
        """
        self.return_value = return_value
        self.exception = exception
        self.relay_value = relay_value

    def __repr__(self):
        return 'APIHandlerResponse({return_value}, exception={exception}, relay_value={relay_value})'.format(
            return_value=self.return_value, exception=self.exception, relay_value=self.relay_value)

    def __str__(self):
        return str(repr(self))


class ConnectionServer:
    """A single request API server callable that communicates using the given multiprocessing.Connection object by
    invoking the given handler.
    """
    def __init__(self, handler, connection, timeout=0.1):
        """Define the connection over which requests will be served and the handler used to process the requests.

        :param handler:     A callable that must:
                            1. Accept the API request to be the first parameter.
                            2. Accept the rest of the parameters passed to this callable (args and kwargs).
                            3. Return APIHandlerResponse or similar object.
                            If the handler is not compatible for any of the calls, the details will be logged but the
                            exception will not propagate.
        :param connection:  A multiprocessing.Connection like object that supports poll(<timeout>) and recv() methods.
                            An APIResponse object will be instantiated with the 'return_value' and 'exception'
                            attributes from the received APIHandlerResponse object. This APIResponse object will be
                            sent using the given connection.
        :param timeout:     The timeout (in seconds) while waiting for requests.
        """
        self._handler = handler
        self._connection = connection
        self._timeout = timeout

    def __call__(self, *args, **kwargs):
        """This callable will serve a single request by calling the handler, sending the response and returning the
        relay value.

        A request is received by attempting to read a message from the connection (with the given timeout).
        The handler is called with the following signature:
            APIHandlerResponse = handler(<API request received from the connection>, *args, **kwargs)
        An APIResponse object will be instantiated with the 'return_value' and 'exception' attributes from the received
            APIHandlerResponse object. This APIResponse object will be sent using the given connection.
        If the handler raises an exception, it will be logged and the call will return None.
        If the handler doesn't return a tuple, it will be logged and the call will return None.
        If the connection is closed before the API result is returned, it will be ignored and the relay_value will be
            returned.

        :param args:    Passed along with the request to the handler. The request will be the first parameter.
        :param kwargs:  Passed as-is to the handler.
        :return:        None, if no request is received.
                        The relay_value attribute from the APIHandlerResponse object returned by the handler.
        """
        request = read_message(self._connection, self._timeout)
        logger.debug('Received request: {}'.format(request))
        if not request:
            return

        try:
            handler_response = self._handler(request, *args, **kwargs)
        except Exception as e:
            logger.exception(('Handler: {} failed to handle request: {}, with args={}, kwargs={} '
                              'due to error={}').format(self._handler, request, args, kwargs, e))
            return

        api_response = APIResponse(handler_response.return_value, handler_response.exception)
        try:
            logger.debug('Sending response: {}'.format(api_response))
            self._connection.send(api_response)
        except IOError:
            pass

        logger.debug('Returning response: {}'.format(handler_response.relay_value))
        return handler_response.relay_value

    def __del__(self):
        try:
            self._connection.close()
        except OSError:
            pass


class ConnectionClient:
    """Client to send requests and receive responses using a multiprocessing.Connection object."""
    def __init__(self, connection):
        """Define the connection to be used for API communication.

        :param connection:  A multiprocessing.Connection like object that supports poll(<timeout>) and recv() methods.
        """
        self._connection = connection

    def __call__(self, request, timeout=1):
        """Sends the passed request using the given connection object and returns the response.

        :param request: The request to send. Any object that can be serialized into a multiprocessing.Connection.
        :param timeout: The timeout (in seconds) while waiting for requests.
        :return:        The response received.
        """
        self._connection.send(request)
        response = read_message(self._connection, timeout)
        return response


class SocketServer:
    """Serve API calls through a Unix socket file using ConnectionServer until signalled to shutdown.

    When the handler returns an APIHandlerResponse (or similar object) with a relay_value of SocketServer.SHUTDOWN, it
    signals the server to stop serving any more requests.
    """

    SHUTDOWN = 'Shutdown Server'    # Signal to shutdown server.

    def __init__(self, socket_file, handler, delay=1, timeout=1):
        """

        :param socket_file: A Unix socket file that will be used to communicate.
        :param handler:     A callable that is called with requests read from the socket. It is called with the request
                            as the only parameter. The handler must return a APIHandlerResponse or similar object.
                            If the APIHandlerResponse object's relay_value is SocketServer.SHUTDOWN, the server will
                            stop serving any more requests and close communications at the socket.
        :param delay:       The delay between serving API requests in seconds.
        :param timeout:     The amount of time in seconds to wait for a request once a connection has been accepted.
        """
        self._socket_file = socket_file
        self._handler = handler
        self._delay = delay
        self._timeout = timeout

    def __call__(self, *args, **kwargs):
        """Start the server loop. Serve requests until signalled to stop.

        :param args:    Passed along with the request to the handler. The request will be the first parameter.
        :param kwargs:  Passed as-is to the handler.
        :return:        None. Returns only when the handler's APIHandlerResponse object's relay_value is
                        SocketServer.SHUTDOWN.
        """
        try:
            listener = Listener(address=self._socket_file, family='AF_UNIX')
            logger.debug('Created listener for socket file: {}'.format(self._socket_file))
        except Exception as e:
            logger.exception('Unable to start the listener. Error: {}'.format(e))
            raise

        while True:
            connection = listener.accept()
            server = ConnectionServer(self._handler, connection, timeout=self._timeout)
            relay_value = server(*args, **kwargs)
            if relay_value == self.SHUTDOWN:
                logger.info('Received signal to shutdown server. Shutting down.')
                break
            sleep(self._delay)


class SocketClient:
    """A ConnectionClient like class that communicates over a socket file.

    This is a callable that sends the passed request over the given socket file and returns the response.
    """
    def __init__(self, address):
        """Initialise the SocketClient to use a given socket file.

        :param address:     A Unix socket file that will be used to communicate.
        """
        self._address = address

    def __call__(self, request, timeout=1):
        """Make a request over the socket, wait for a response and return it.

        :param request: Any object that can be sent over a Unix socket.
        :param timeout: The amount of time in seconds to wait for a response after a requeset has been sent.
        :return:        The response received over the socket.
        """
        connection = Client(address=self._address, family='AF_UNIX')
        client = ConnectionClient(connection)
        response = client(request, timeout=timeout)
        connection.close()
        return response


class MethodAPIHandlerWrapper:
    """Wrap a given handler object as an API handler that accepts APIRequest objects as requests, invokes handler
    methods and returns an APIHandlerResponse object."""
    def __init__(self, handler):
        """Define the handler whose methods will be called to serve API requests.

        :param handler: The handler is an object whose methods will be invoked based on the request received.
                        The handler must return a APIHandlerResponse or similar object.
                        The call to the handler's method can be symbolically shown as:
                            handler.<'method' from request>(*<passed args to this callable>, *<'args' from request>,
                                                            **<kwargs combined from request ('kwargs') and passed to
                                                            this callable>)
                        If the handler raises an exception, the following APIHandlerResponse object will be returned:
                            APIHandlerResponse(None, <exception raised>, None)
        """
        self._handler = handler

    def __call__(self, request, *args, **kwargs):
        """Serve a single request by calling handler methods and returning the APIHandlerResponse they return.

        Any exception raised by the handler method will be logged with full context and traceback.
        The APIHandlerResponse object will be logged for each call (at DEBUG level).

        :param request: An APIRequest or similar object containing supplemental *args and **kwargs for the handler
                        method. The "supplemental" is explained below.
        :param args:    These are the first arguments passed to the handler. These are supplemented (suffixed)
                        by the 'args' provided in the request (above).
        :param kwargs:  These are passed to the handler method called. These are supplemented (collated) with
                        the 'kwargs' provided in the request (above).
        :return:        An APIHandlerResponse object.
                        If the handler raises an exception, the following APIHandlerResponse object will be returned:
                            APIHandlerResponse(None, <exception raised>, None)
        """
        try:
            method = getattr(self._handler, request.method)
            args = list(args) + list(request.args)
            kwargs.update(request.kwargs)
            response = method(*args, **kwargs)
        except Exception as e:
            logger.exception('APIHandler got the exception {} with the message: {}'.format(type(e), e))
            response = APIHandlerResponse(None, e)

        logger.debug('Received request: {request} -- Response: {response}'.format(request=request, response=response))
        return response


class MethodAPIClientWrapper:
    """Wraps a ConnectionClient like callable with an object like interface such that method calls result in APIRequest
    objects being sent and responses being returned.

    This is best illustrated with an example as follows:
        method_client = MethodAPIClientWrapper(client)
        result = method_client.foo(42, bar='baz')

    The 'foo' method call is equivalent to:
        result = client(APIRequest('foo', (42,), {'bar': 'baz'})
    """
    def __init__(self, client, timeout=1):
        """Define the client to be used for communication.

        :param client:  A ConnectionClient like callable.
        :param timeout: The timeout (in seconds) while waiting for a response.
        """
        self._client = client
        self._timeout = timeout

    def __getattr__(self, method):
        """Return a function that will convert the method call into a request, send it using the client and
        return the remote call's return value or raise the exception received.

        See the class docstring for an example.

        :param method:  The method name (str).
        :return:        A function that:
                        1. Accepts *args and **kwargs.
                        2. Produces an APIRequest object with the method name, args and kwargs.
                        3. Calls the pre-defined client with the request and waits for a response.
                        4. Receives an APIResponse object.
                        5. Returns the APIResponse's 'return_value' attribute if there its 'exception' attribute is
                           None.
                           Raises the exception if the value of 'exception' is not None.
        """
        def method_callable(*args, **kwargs):
            response = self._client(APIRequest(method, args, kwargs), timeout=self._timeout)

            if not response:
                return None

            if response.exception is not None:
                raise response.exception
            return response.return_value

        return method_callable
