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

from collections import namedtuple
from socket import AF_UNIX, SOCK_STREAM, socket, timeout
from urllib import quote, unquote


def socket_write(socket, message):
    """Write the given message as a quoted string to the socket ending with a newline character.

    Arguments:
    socket  -- A connection socket as returned by socket.accept().
    message -- A string or string like object that can be written into the socket.

    Post-condition:
    The message is quoted and then written as a string to the socket appended with a newline character.
    """
    wfile = socket.makefile('wb')
    wfile.write('{}\n'.format(quote(message)))
    wfile.close()


def socket_write_json(socket, message_object):
    """Write the given dictionary as a quoted JSON to the socket ending with a newline character.

    Arguments:
    socket         -- A connection socket as returned by socket.accept().
    message_object -- An object that can be converted to a string using json.dumps.

    Post-condition:
    The message is converted to JSON and quoted, then written as a string to the socket appended with a newline
    character.
    """
    socket_write(socket, json.dumps(message_object))


def socket_read(socket):
    """Read a quoted request (ending with a newline character) from the given socket.

    Arguments:
    socket         -- A connection socket as returned by socket.accept().

    Returns:
    String         -- after stripping and unquoting the request received.
    """
    rfile = socket.makefile('rb')
    request = unquote(rfile.readline().strip())
    rfile.close()
    return request


def socket_read_json(socket):
    """Read a quoted JSON request (ending with a newline character) from the given socket.

    Arguments:
    socket         -- A connection socket as returned by socket.accept().

    Returns:
    The object representation of the JSON request. The request is stripped and unquoted, then converted from JSON and
    returned.
    """
    return json.loads(socket_read(socket))


def socket_server_setup(socket_file, backlog_count=1, timeout=0.00001, family=AF_UNIX, socket_type=SOCK_STREAM):
    """Setup a socket for a server by doing the following:
    1. Create a socket of the given family and socket_type.
    2. Bind it to the given socket_file.
    3. Set the backlog limit for the socket.
    4. Set the timeout on blocking socket operations.

    Arguments:
    socket_file   -- The socket file to be used.

    Keyword Arguments:
    backlog_count -- This specifies the maximum number of queued connections and should be at least 0; the maximum
                     value is system-dependent (usually 5), the minimum value is forced to 0.
    timeout       -- A non-negative float expressing seconds, or None.
    family        -- The socket family.
    socket_type   -- The socket type.
    """
    server = socket(family, socket_type)
    server.bind(socket_file)
    server.listen(backlog_count)
    server.settimeout(timeout)
    return server


def send_request(socket_file, request, family=AF_UNIX, socket_type=SOCK_STREAM, socket_reader=socket_read_json,
                  socket_writer=socket_write_json):
    """Send the given request using the given socket and return the response received.

    Arguments:
    socket_file   -- The socket file to be used.
    request       -- The request to send. This needs to be consumable by the given socket_writer.
    family        -- The socket family.
    socket_type   -- The socket type.
    socket_reader -- The function used to read a response from the socket.
    socket_writer -- The function used to write the request to the socket.
    """
    connection_socket = socket(family, socket_type)
    connection_socket.connect(socket_file)
    socket_writer(connection_socket, request)
    response = socket_reader(connection_socket)
    connection_socket.close()
    return response


HandlerResult = namedtuple('HandlerResult', ['response', 'return_value'])


def serve_socket(socket, request_handler, socket_reader=socket_read_json, socket_writer=socket_write_json):
    """Serve a single request from the given socket. This is done as follows:
    1. A connection is accepted. If no connection request is received (timeout), returns None.
    2. Read the request using the given socket_reader.
    3. Invoke the given request_handler with the request.
    4. Extract the response and return value from the object returned by the request_handler.
    5. Write the response to the connection socket using the socket_writer.
    6. Return the return value extracted from the object returned by the request_handler.
    7. Close the socket in every case.

    Arguments:
    socket          -- A socket.socket like object. Must support the accept() method that should return a tuple
                       with the first object being a connection socket.
    request_handler -- A function that will handle the request received. This function should:
                       1. Accept one argument, the request object returned by the socket_reader.
                       2. Must return a HandlerResult or similar object that contains:
                        response     -- This is the response that is sent to the requester using the socket_writer.
                        return_value -- This is returned to the caller of this function.
                       The request_handler is called only when a request is received.

    Keyword Arguments:
    socket_reader   -- The function used to read a response from the socket.
    socket_writer   -- The function used to write the request to the socket.
    """
    connection_socket = None
    return_value = None

    try:
        logging.debug('Waiting for a connection...')
        connection_socket, _ = socket.accept()
        request = socket_reader(connection_socket)
        if request:
            logging.debug('Got the following request: {}'.format(request))
            handler_result = request_handler(request)
            socket_writer(connection_socket, handler_result.response)
            logging.debug('Sending the following response: {}'.format(handler_result.response))
            return_value = handler_result.return_value
    except timeout:
        logging.debug('Timed out waiting for connection.')
        pass
    finally:
        if connection_socket:
            connection_socket.close()

    return return_value
