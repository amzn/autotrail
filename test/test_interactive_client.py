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

Tests the InteractiveClientWrapper.

Strategy: The InteractiveClientWrapper class accepts a MethodAPIClientWrapper like object and makes calls to it.
In this suite of tests, we define a mock object to pass instead of the MethodAPIClientWrapper such that it returns
values just like a real workflow. We then assert whether the output is as expected.

"""
import pytest
import unittest

from io import StringIO
from unittest.mock import MagicMock

from autotrail.workflow.default_workflow.io import InteractiveClientWrapper


API_CALL_RESPONSE_FOR_START = [0, 1, 2]
API_CALL_RESPONSE_FOR_SHUTDOWN = []
API_CALL_RESPONSE_FOR_SEND_MESSAGE = [1]
API_CALL_RESPONSE_FOR_LIST = [
    {'n': 0, 'name': 'output'},
    {'n': 1, 'name': 'instruction'},
    {'n': 2, 'name': 'other'}
]
API_CALL_RESPONSE_FOR_STATUS_SINGLE = {
    0: {'Name': 'output',
        'Tags': {'n': 0, 'name': 'output'},
        'State': 'Successful',
        'Actions': [],
        'I/O': [],
        'Output': ['Test output message.'],
        'Return value': None,
        'Exception': None
        }
}
API_CALL_RESPONSE_FOR_STATUS_MULTIPLE = {
    0: {'Name': 'output',
        'Tags': {'n': 0, 'name': 'output'},
        'State': 'Running',
        'Actions': ['Succeed', 'Fail', 'Interrupt', 'Error'],
        'I/O': [],
        'Output': ['Test output message.'],
        'Return value': None,
        'Exception': None
        },
    1: {'Name': 'instruction',
        'Tags': {'n': 1, 'name': 'instruction'},
        'State': 'Waiting',
        'Actions': ['Pause', 'Mark to skip'],
        'I/O': None,
        'Output': None,
        'Return value': None,
        'Exception': None
        },
    2: {'Name': 'other',
        'Tags': {'n': 2, 'name': 'other'},
        'State': 'Paused',
        'Actions': ['Resume', 'Mark to skip'],
        'I/O': None,
        'Output': None,
        'Return value': None,
        'Exception': None
        },
}
API_CALL_RESPONSE_FOR_STEPS_WAITING = {
    1: {'Name': 'instruction',
        'I/O': ['Test instruction.']
        },
}
API_CALL_RESPONSE_FOR_PAUSE = [2]
API_CALL_RESPONSE_FOR_INTERRUPT = [2]
API_CALL_RESPONSE_FOR_RESUME = [2]
API_CALL_RESPONSE_FOR_RERUN = [2]
API_CALL_RESPONSE_FOR_SKIP = [2]
API_CALL_RESPONSE_FOR_UNSKIP = [2]


API_CALL_OUTPUT_FOR_START = """
- API Call: start
  Dry Run: True
  List of step names that are affected:
    # 0 - output
    # 1 - instruction
    # 2 - other
"""
API_CALL_OUTPUT_FOR_SHUTDOWN = """- API Call: shutdown
  Dry Run: True
  List of step names that are affected:
"""
API_CALL_OUTPUT_FOR_SEND_MESSAGE = """
- API Call: send_message_to_steps
  Dry Run: False
  List of step names that are affected:
    # 1 - instruction
"""
API_CALL_OUTPUT_FOR_LIST = """
List of steps and their tags:
- Name: output
  n: 0
- Name: instruction
  n: 1
- Name: other
  n: 2
"""
API_CALL_OUTPUT_FOR_STATUS_SINGLE = """
Status of steps:
- Name        : output
  n           : 0
  Tags        : n = 0
              : name = output
  State       : Successful
  Actions     : []
  I/O         : []
  Output      : Test output message.
  Return value: None
  Exception   : None
"""
API_CALL_OUTPUT_FOR_STATUS_MULTIPLE = """
Status of steps:
- Name        : output
  n           : 0
  Tags        : n = 0
              : name = output
  State       : Running
  Actions     : Succeed
              : Fail
              : Interrupt
              : Error
  I/O         : []
  Output      : Test output message.
  Return value: None
  Exception   : None

- Name        : instruction
  n           : 1
  Tags        : n = 1
              : name = instruction
  State       : Waiting
  Actions     : Pause
              : Mark to skip
  I/O         : None
  Output      : None
  Return value: None
  Exception   : None

- Name        : other
  n           : 2
  Tags        : n = 2
              : name = other
  State       : Paused
  Actions     : Resume
              : Mark to skip
  I/O         : None
  Output      : None
  Return value: None
  Exception   : None
"""
API_CALL_OUTPUT_FOR_STEPS_WAITING = """
Status of steps:
- Name        : instruction
  n           : 1
  I/O         : Test instruction.
"""
API_CALL_OUTPUT_FOR_PAUSE = """
- API Call: pause
  Dry Run: True
  List of step names that are affected:
    # 2 - other
"""
API_CALL_OUTPUT_FOR_INTERRUPT = """
- API Call: interrupt
  Dry Run: True
  List of step names that are affected:
    # 2 - other
"""
API_CALL_OUTPUT_FOR_RESUME = """
- API Call: resume
  Dry Run: True
  List of step names that are affected:
    # 2 - other
"""
API_CALL_OUTPUT_FOR_RERUN = """
- API Call: rerun
  Dry Run: True
  List of step names that are affected:
    # 2 - other
"""
API_CALL_OUTPUT_FOR_SKIP = """
- API Call: skip
  Dry Run: True
  List of step names that are affected:
    # 2 - other
"""
API_CALL_OUTPUT_FOR_UNSKIP = """
- API Call: unskip
  Dry Run: True
  List of step names that are affected:
    # 2 - other
"""


def reset_streamio_object(streamio_object):
    streamio_object.truncate(0)
    streamio_object.seek(0)


@pytest.mark.validationtest
class WorkflowValidationTests(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.mock_client = MagicMock()
        self.mock_client.start = MagicMock(return_value=API_CALL_RESPONSE_FOR_START)
        self.mock_client.shutdown = MagicMock(return_value=API_CALL_RESPONSE_FOR_SHUTDOWN)
        self.mock_client.send_message_to_steps = MagicMock(return_value=API_CALL_RESPONSE_FOR_SEND_MESSAGE)
        self.mock_client.list = MagicMock(return_value=API_CALL_RESPONSE_FOR_LIST)
        self.mock_client.status = MagicMock(side_effect=[API_CALL_RESPONSE_FOR_STATUS_SINGLE,
                                                         API_CALL_RESPONSE_FOR_STATUS_MULTIPLE])
        self.mock_client.steps_waiting_for_user_input = MagicMock(return_value=API_CALL_RESPONSE_FOR_STEPS_WAITING)
        self.mock_client.pause = MagicMock(return_value=API_CALL_RESPONSE_FOR_PAUSE)
        self.mock_client.interrupt = MagicMock(return_value=API_CALL_RESPONSE_FOR_INTERRUPT)
        self.mock_client.resume = MagicMock(return_value=API_CALL_RESPONSE_FOR_RESUME)
        self.mock_client.rerun = MagicMock(return_value=API_CALL_RESPONSE_FOR_RERUN)
        self.mock_client.skip = MagicMock(return_value=API_CALL_RESPONSE_FOR_SKIP)
        self.mock_client.unskip = MagicMock(return_value=API_CALL_RESPONSE_FOR_UNSKIP)

        self.stdout_stream = StringIO()
        self.stderr_stream = StringIO()
        self.interactive_client = InteractiveClientWrapper(
            self.mock_client, stdout=self.stdout_stream, stderr=self.stderr_stream)

        # The 'list' API called on the class instantiation to fetch all the step tags. This is a one-time call.
        self.mock_client.list.assert_called_once_with()
        self.mock_client.list.reset_mock()

    def tearDown(self):
        pass

    def _check_output(self, expected_output):
        self.assertEqual(self.stdout_stream.getvalue().strip(), expected_output.strip())
        reset_streamio_object(self.stdout_stream)

    def test_workflow(self):
        self.interactive_client.start()
        self._check_output(API_CALL_OUTPUT_FOR_START)
        self.mock_client.start.assert_called_once_with(dry_run=True)

        self.interactive_client.shutdown()
        self.mock_client.shutdown.assert_called_once_with(dry_run=True)
        self._check_output(API_CALL_OUTPUT_FOR_SHUTDOWN)

        self.interactive_client.send_message_to_steps(True, n=1, dry_run=False)
        self.mock_client.send_message_to_steps.assert_called_once_with(True, n=1, dry_run=False)
        self._check_output(API_CALL_OUTPUT_FOR_SEND_MESSAGE)

        self.interactive_client.list()
        self.mock_client.list.assert_called_once_with()
        self._check_output(API_CALL_OUTPUT_FOR_LIST)

        self.interactive_client.status()
        self.mock_client.status.assert_called_once_with(fields=None, states=None)
        self._check_output(API_CALL_OUTPUT_FOR_STATUS_SINGLE)

        self.mock_client.status.reset_mock()
        self.interactive_client.status()
        self.mock_client.status.assert_called_once_with(fields=None, states=None)
        self._check_output(API_CALL_OUTPUT_FOR_STATUS_MULTIPLE)

        self.interactive_client.steps_waiting_for_user_input()
        self.mock_client.steps_waiting_for_user_input.assert_called_once_with()
        self._check_output(API_CALL_OUTPUT_FOR_STEPS_WAITING)

        self.interactive_client.pause()
        self.mock_client.pause.assert_called_once_with(dry_run=True)
        self._check_output(API_CALL_OUTPUT_FOR_PAUSE)

        self.interactive_client.interrupt()
        self.mock_client.interrupt.assert_called_once_with(dry_run=True)
        self._check_output(API_CALL_OUTPUT_FOR_INTERRUPT)

        self.interactive_client.resume()
        self.mock_client.resume.assert_called_once_with(dry_run=True)
        self._check_output(API_CALL_OUTPUT_FOR_RESUME)

        self.interactive_client.rerun()
        self.mock_client.rerun.assert_called_once_with(dry_run=True)
        self._check_output(API_CALL_OUTPUT_FOR_RERUN)

        self.interactive_client.skip()
        self.mock_client.skip.assert_called_once_with(dry_run=True)
        self._check_output(API_CALL_OUTPUT_FOR_SKIP)

        self.interactive_client.unskip()
        self.mock_client.unskip.assert_called_once_with(dry_run=True)
        self._check_output(API_CALL_OUTPUT_FOR_UNSKIP)
