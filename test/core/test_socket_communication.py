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
from socket import socket, AF_UNIX, SOCK_STREAM, timeout

import six

from six.moves.urllib.parse import quote, unquote

from autotrail.core.socket_communication import (socket_write, socket_write_json, socket_read, socket_read_json,
                                                 socket_server_setup, send_request, serve_socket)


class TestSocketCommunication(unittest.TestCase):
    def setUp(self):
        self.json_patcher = patch('autotrail.core.socket_communication.json')
        self.mock_json = self.json_patcher.start()
        self.mock_json_message = MagicMock()
        self.mock_json.dumps = MagicMock(return_value=self.mock_json_message)
        self.mock_json.loads = MagicMock(return_value=self.mock_json_message)

    def tearDown(self):
        self.json_patcher.stop()

    def test_socket_write(self):
        mock_socket = MagicMock()
        mock_wfile = MagicMock()
        mock_socket.makefile = MagicMock(return_value=mock_wfile)

        socket_write(mock_socket, 'mock_message')

        if six.PY2:
            mock_socket.makefile.assert_called_once_with('wb')
        else:
            mock_socket.makefile.assert_called_once_with('wb', buffering=None)
        mock_wfile.write.assert_called_once_with(b'mock_message\n')

    def test_socket_write_json(self):
        socket_write_patcher = patch('autotrail.core.socket_communication.socket_write')
        mock_socket_write = socket_write_patcher.start()
        mock_socket = MagicMock()
        message_dict = dict(foo='bar')

        socket_write_json(mock_socket, message_dict)

        mock_socket_write.assert_called_once_with(mock_socket, self.mock_json_message)
        self.mock_json.dumps.assert_called_once_with(message_dict)

        socket_write_patcher.stop()

    def test_socket_read(self):
        mock_socket = MagicMock()
        mock_rfile = MagicMock()
        mock_rfile.readline = MagicMock(return_value=b' mock_message\n')
        mock_rfile.close = MagicMock()
        mock_socket.makefile = MagicMock(return_value=mock_rfile)

        request = socket_read(mock_socket)

        if six.PY2:
            mock_socket.makefile.assert_called_once_with('rb')
        else:
            mock_socket.makefile.assert_called_once_with('rb', buffering=None, newline='\n')
        mock_rfile.readline.assert_called_once_with()
        mock_rfile.close.assert_called_once_with()
        self.assertEqual(request, 'mock_message')

    def test_socket_read_json(self):
        socket_read_patcher = patch('autotrail.core.socket_communication.socket_read', return_value='mock_request')
        mock_socket_read = socket_read_patcher.start()
        mock_socket = MagicMock()

        request = socket_read_json(mock_socket)

        mock_socket_read.assert_called_once_with(mock_socket)
        self.mock_json.loads.assert_called_once_with('mock_request')
        self.assertEqual(request, self.mock_json_message)

        socket_read_patcher.stop()

    def test_socket_server_setup(self):
        mock_socket_file = MagicMock()
        mock_backlog_count = MagicMock()
        mock_timeout = MagicMock()
        mock_family = MagicMock()
        mock_socket_type = MagicMock()

        mock_server = MagicMock()
        socket_patcher = patch('autotrail.core.socket_communication.socket', return_value=mock_server)
        mock_socket = socket_patcher.start()

        returned_server = socket_server_setup(mock_socket_file, backlog_count=mock_backlog_count, timeout=mock_timeout,
                                              family=mock_family, socket_type=mock_socket_type)

        mock_socket.assert_called_once_with(mock_family, mock_socket_type)
        mock_server.bind.assert_called_once_with(mock_socket_file)
        mock_server.listen.assert_called_once_with(mock_backlog_count)
        mock_server.settimeout.assert_called_once_with(mock_timeout)
        self.assertEqual(returned_server, mock_server)

        socket_patcher.stop()

    def test_send_request(self):
        mock_socket_file = MagicMock()
        mock_family = MagicMock()
        mock_socket_type = MagicMock()
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_socket_reader = MagicMock(return_value=mock_response)
        mock_socket_writer = MagicMock()

        mock_connection_socket = MagicMock()
        socket_patcher = patch('autotrail.core.socket_communication.socket', return_value=mock_connection_socket)
        mock_socket = socket_patcher.start()

        response = send_request(mock_socket_file, mock_request, family=mock_family, socket_type=mock_socket_type,
                                 socket_reader=mock_socket_reader, socket_writer=mock_socket_writer)

        mock_socket.assert_called_once_with(mock_family, mock_socket_type)
        mock_connection_socket.connect.assert_called_once_with(mock_socket_file)
        mock_socket_writer.assert_called_once_with(mock_connection_socket, mock_request)
        mock_socket_reader.assert_called_once_with(mock_connection_socket)
        mock_connection_socket.close.assert_called_once_with()
        self.assertEqual(response, mock_response)

        socket_patcher.stop()

    def test_serve_socket(self):
        mock_connection_socket = MagicMock()
        mock_address = MagicMock()
        mock_socket = MagicMock()
        mock_socket.accept = MagicMock(return_value=(mock_connection_socket, mock_address))
        mock_handler_result = MagicMock()
        mock_request_handler = MagicMock(return_value=mock_handler_result)
        mock_request = MagicMock()
        mock_socket_reader = MagicMock(return_value=mock_request)
        mock_socket_writer = MagicMock()

        return_value = serve_socket(mock_socket, mock_request_handler, socket_reader=mock_socket_reader,
                                    socket_writer=mock_socket_writer)

        mock_socket.accept.assert_called_once_with()
        mock_socket_reader.assert_called_once_with(mock_connection_socket)
        mock_request_handler.assert_called_once_with(mock_request)
        mock_socket_writer.assert_called_once_with(mock_connection_socket, mock_handler_result.response)
        mock_connection_socket.close.assert_called_once_with()
        self.assertEqual(return_value, mock_handler_result.return_value)

    def test_serve_socket_with_timeout(self):
        mock_connection_socket = MagicMock()
        mock_address = MagicMock()
        mock_socket = MagicMock()
        mock_socket.accept = MagicMock(side_effect=timeout)
        mock_handler_result = MagicMock()
        mock_request_handler = MagicMock(return_value=mock_handler_result)
        mock_request = MagicMock()
        mock_socket_reader = MagicMock(return_value=mock_request)
        mock_socket_writer = MagicMock()

        return_value = serve_socket(mock_socket, mock_request_handler, socket_reader=mock_socket_reader,
                                    socket_writer=mock_socket_writer)

        mock_socket.accept.assert_called_once_with()
        self.assertEqual(mock_socket_reader.call_count, 0)
        self.assertEqual(mock_request_handler.call_count, 0)
        self.assertEqual(mock_socket_writer.call_count, 0)
        self.assertEqual(mock_connection_socket.call_count, 0)
        self.assertEqual(mock_connection_socket.close.call_count, 0)
        self.assertEqual(return_value, None)

    def test_serve_socket_with_timeout_raised_by_socket_reader(self):
        mock_connection_socket = MagicMock()
        mock_address = MagicMock()
        mock_socket = MagicMock()
        mock_socket.accept = MagicMock(return_value=(mock_connection_socket, mock_address))
        mock_handler_result = MagicMock()
        mock_request_handler = MagicMock(return_value=mock_handler_result)
        mock_request = MagicMock()
        mock_socket_reader = MagicMock(side_effect=timeout)
        mock_socket_writer = MagicMock()

        return_value = serve_socket(mock_socket, mock_request_handler, socket_reader=mock_socket_reader,
                                    socket_writer=mock_socket_writer)

        mock_socket.accept.assert_called_once_with()
        mock_socket_reader.assert_called_once_with(mock_connection_socket)
        self.assertEqual(mock_request_handler.call_count, 0)
        self.assertEqual(mock_socket_writer.call_count, 0)
        self.assertEqual(mock_connection_socket.call_count, 0)
        mock_connection_socket.close.assert_called_once_with()
        self.assertEqual(return_value, None)

    def test_serve_socket_with_no_request_received(self):
        mock_connection_socket = MagicMock()
        mock_address = MagicMock()
        mock_socket = MagicMock()
        mock_socket.accept = MagicMock(return_value=(mock_connection_socket, mock_address))
        mock_handler_result = MagicMock()
        mock_request_handler = MagicMock(return_value=mock_handler_result)
        mock_socket_reader = MagicMock(return_value=None)
        mock_socket_writer = MagicMock()

        return_value = serve_socket(mock_socket, mock_request_handler, socket_reader=mock_socket_reader,
                                    socket_writer=mock_socket_writer)

        mock_socket.accept.assert_called_once_with()
        mock_socket_reader.assert_called_once_with(mock_connection_socket)
        self.assertEqual(mock_request_handler.call_count, 0)
        self.assertEqual(mock_socket_writer.call_count, 0)
        mock_connection_socket.close.assert_called_once_with()
        self.assertEqual(return_value, None)
