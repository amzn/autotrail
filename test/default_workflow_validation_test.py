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

Validate various workflow API calls and controls.

In this test, a non-trivial workflow will be defined and executed. The various workflow controls will be exercised
on the steps involved while running the workflow below:

                                            +--> interrupt_resume ----------->+
                                            |                                 |--->+
                                            +--> error_rerun_and_skip ------->+    |
 output --> instruction --> pause_resume -->|                                      +--> unskip
                                            |            +------------------->+    |
                                            +--> fail -->|                    |--->+
                                                         +-----> on_fail ---->+

To facilitate this, the various workflow step's functions are defined as follows:
- 'interrupt_resume' has a sleep to allow the running step sub-process to be interrupted.
- 'error_rerun_and_skip' is set to fail with a specific exception that is considered temporary and re-runnable.
  Because it even re-runs will fail with the same exception, this step will be skipped.
- 'fail' is set to fail with an exception that is not considered temporary and cannot be re-run.

Test plan:
1. Start the workflow.
2. The "input_output" will test the I/O mechanisms by sending output messages and waiting for input messages.
3. Pause the "pause_resume" step and verify it has taken effect (subsequent steps should not be running).
4. Skip the "unskip" step as we will test unskipping it later.
5. Resume the "pause_resume" step. Verify the subsequent steps run.
6. Wait for the "interrupt_resume" step to start running and interrupt it.
7. Ensure the "error_rerun_and_skip" and "fail" steps have failed.
8. Ensure the "on_fail" step is running/succeeded.
9. Rerun the "error_rerun_and_skip" step and verify it goes back into "Running".
10. Unskip the "unskip" step.
11. Skip the "error_rerun_and_skip" step.
12. Verify that the "unskip" step completed successfully.

"""
import os
import pytest
import unittest

from time import sleep

from autotrail.workflow.default_workflow.api import StatusField, make_api_client
from autotrail.workflow.default_workflow.state_machine import StepFailed, Action, State
from autotrail.workflow.default_workflow.management import WorkflowManager
from autotrail.workflow.helpers.context import make_context, make_context_serializer
from autotrail.workflow.helpers.step import (make_contextless_step, make_instruction_step, make_output_step,
                                             make_simple_preprocessor)


DELAY = 0.5


def pause_resume():
    return 'Return value of pause_resume.'


def interrupt_resume():
    sleep(DELAY * 7)
    return 'Return value of interrupt_resume.'


def error_rerun_and_skip():
    sleep(DELAY * 5)
    raise RuntimeError('Exception from error_rerun_and_skip.')


def fail():
    raise StepFailed('Exception from fail.')


def on_fail():
    return 'Return value of on_fail.'


def unskip():
    return 'Return value of unskip.'


@pytest.mark.validationtest
class WorkflowValidationTests(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

        self.step_output = make_output_step(make_simple_preprocessor('Test output message.'))
        self.step_instruction = make_instruction_step(make_simple_preprocessor('Test instruction.'))
        self.step_pause_resume = make_contextless_step(pause_resume)
        self.step_interrupt_resume = make_contextless_step(interrupt_resume)
        self.step_error_rerun_and_skip = make_contextless_step(error_rerun_and_skip)
        self.step_fail = make_contextless_step(fail)
        self.step_on_fail = make_contextless_step(on_fail)
        self.step_unskip = make_contextless_step(unskip)

        # Ordering -- See the docstring at the top of this test suite for details.
        success_pairs = [
            (self.step_output,                   self.step_instruction),
            (self.step_instruction,              self.step_pause_resume),
            (self.step_pause_resume,             self.step_interrupt_resume),
            (self.step_pause_resume,             self.step_error_rerun_and_skip),
            (self.step_pause_resume,             self.step_fail),
            (self.step_interrupt_resume,         self.step_unskip),
            (self.step_error_rerun_and_skip,     self.step_unskip),
            (self.step_on_fail,                  self.step_unskip)]
        failure_pairs = [
            (self.step_fail, self.step_on_fail)]

        self._socket_file = '/tmp/validation_test.socket'
        try:
            os.remove(self._socket_file)
        except OSError:
            pass

        context = make_context()
        context_serializer = make_context_serializer(context)
        self.workflow_manager = WorkflowManager(success_pairs, failure_pairs, context, context_serializer,
                                                self._socket_file, workflow_delay=DELAY, api_delay=DELAY)
        self.client = make_api_client(self._socket_file, timeout=5)
        self.workflow_manager.start()
        sleep(DELAY * 5)

    def tearDown(self):
        self.workflow_manager.terminate()
        self.workflow_manager.cleanup()

    def _assert_status_attribute(self, step, status, attribute, expected_value, normalizer=None):
        found_value = status[attribute] if normalizer is None else normalizer(status[attribute])
        expected_value = expected_value if normalizer is None else normalizer(expected_value)

        error_message = ('Error validating status for step "{step}" -- Expected "{attribute}" to be '
                         '"{expected_value}" (type: {expected_value_type}) but found it to be '
                         '"{found_value}" (type: {found_value_type}). Status: {status}').format(
            step=step, attribute=attribute, expected_value=expected_value, expected_value_type=type(expected_value),
            found_value=found_value, found_value_type=type(found_value), status=status)
        self.assertEqual(found_value, expected_value, error_message)

    def _check_status(self, expected_states):
        status = self.client.status()

        state_actions = {
            State.READY:        [Action.START, Action.PAUSE, Action.MARKSKIP],
            State.WAITING:      [Action.MARKSKIP, Action.PAUSE],
            State.RUNNING:      [Action.INTERRUPT, Action.ERROR, Action.SUCCEED, Action.FAIL],
            State.PAUSED:       [Action.RESUME, Action.MARKSKIP],
            State.TOSKIP:       [Action.UNSKIP],
            State.ERROR:        [Action.MARKSKIP, Action.RERUN],
            State.INTERRUPTED:  [Action.RESUME, Action.MARKSKIP],
            State.SUCCEEDED:    [],
            State.SKIPPED:      [],
            State.FAILED:       [],
        }

        result_states = {State.ERROR, State.FAILED, State.SUCCEEDED, State.SKIPPED}

        expected_step_results = {
            self.step_output.id:               (None, None),
            self.step_instruction.id:          (None, None),
            self.step_pause_resume.id:         ('Return value of pause_resume.', None),
            self.step_error_rerun_and_skip.id: (None, 'Exception from error_rerun_and_skip.'),
            self.step_interrupt_resume.id:     ('Return value of interrupt_resume.', None),
            self.step_fail.id:                 (None, 'Exception from fail.'),
            self.step_on_fail.id:              ('Return value of on_fail.', None),
            self.step_unskip.id:               ('Return value of unskip.', None),
        }

        for step, state in expected_states:
            self.assertIn(step.id, status)
            step_status = status[step.id]
            self._assert_status_attribute(step, step_status, StatusField.NAME, str(step))
            self._assert_status_attribute(step, step_status, StatusField.STATE, state)
            self._assert_status_attribute(step, step_status, StatusField.TAGS, step.tags)
            self._assert_status_attribute(step, step_status, StatusField.ACTIONS, state_actions[state],
                                          normalizer=sorted)

            if state in result_states:
                # Checking for "return_value" and "exception" makes sense only when an end state has been reached.
                expected_return_value, expected_exception = expected_step_results[step.id]
                self._assert_status_attribute(step, step_status, StatusField.RETURN_VALUE, expected_return_value)
                self._assert_status_attribute(step, step_status, StatusField.EXCEPTION, expected_exception,
                                              normalizer=str)

    def _wait_until(self, step, state, delay=DELAY, max_tries=70):
        count = 0
        while True:
            status = self.client.status()
            if status[step.id][StatusField.STATE] == state:
                break

            if count < max_tries:
                sleep(delay)
                count += 1
            else:
                raise RuntimeError(
                    'Timed out waiting for step "{}" to change state to "{}". Last known state is "{}".'.format(
                        str(step), state, status[step.id][StatusField.STATE]))

    def test_workflow(self):
        result = self.client.start()
        self.assertEqual(sorted(result), sorted([self.step_output.id, self.step_instruction.id,
                                                 self.step_pause_resume.id, self.step_error_rerun_and_skip.id,
                                                 self.step_interrupt_resume.id, self.step_fail.id,
                                                 self.step_on_fail.id, self.step_unskip.id]))

        self._check_status([
            (self.step_output,               State.READY),
            (self.step_instruction,          State.READY),
            (self.step_pause_resume,         State.READY),
            (self.step_error_rerun_and_skip, State.READY),
            (self.step_interrupt_resume,     State.READY),
            (self.step_fail,                 State.READY),
            (self.step_on_fail,              State.READY),
            (self.step_unskip,               State.READY),
        ])

        result = self.client.start(dry_run=False)
        self.assertEqual(sorted(result), sorted([self.step_output.id, self.step_instruction.id,
                                                 self.step_pause_resume.id, self.step_error_rerun_and_skip.id,
                                                 self.step_interrupt_resume.id, self.step_fail.id,
                                                 self.step_on_fail.id, self.step_unskip.id]))

        result = self.client.pause(name=str(self.step_pause_resume), dry_run=False)
        self.assertEqual(result, [self.step_pause_resume.id])

        result = self.client.skip(name=str(self.step_unskip), dry_run=False)
        self.assertEqual(result, [self.step_unskip.id])

        self._wait_until(self.step_output, State.SUCCEEDED)
        status = self.client.status(name=str(self.step_output))
        self.assertIn(self.step_output.id, status)
        self.assertEqual(status[self.step_output.id][StatusField.OUTPUT], ['Test output message.'])

        # Once "input_output" runs, it will send an output message and prompt for an input. Verify that happens.
        self._wait_until(self.step_instruction, State.RUNNING)

        status = self.client.status(name=str(self.step_instruction))
        self.assertIn(self.step_instruction.id, status)
        self.assertEqual(status[self.step_instruction.id][StatusField.IO], ['Test instruction.'])
        self.assertEqual(self.client.steps_waiting_for_user_input(),
                         {1: {'Name': 'instruction', 'I/O': ['Test instruction.']}})
        result = self.client.send_message_to_steps(True, name=str(self.step_instruction), dry_run=False)
        self.assertEqual(result, [self.step_instruction.id])

        self._wait_until(self.step_instruction, State.SUCCEEDED)

        self._check_status([
            (self.step_output,               State.SUCCEEDED),
            (self.step_instruction,          State.SUCCEEDED),
            (self.step_pause_resume,         State.PAUSED),
            (self.step_error_rerun_and_skip, State.WAITING),
            (self.step_interrupt_resume,     State.WAITING),
            (self.step_fail,                 State.WAITING),
            (self.step_on_fail,              State.WAITING),
            (self.step_unskip,               State.TOSKIP),
        ])

        # Since we paused "step_pause_resume", sleep for 1 second (which results in about 5-7 iterations of the
        # state step and verify that the states haven't changed.
        sleep(DELAY * 5)
        self._check_status([
            (self.step_output,               State.SUCCEEDED),
            (self.step_instruction,          State.SUCCEEDED),
            (self.step_pause_resume,         State.PAUSED),
            (self.step_error_rerun_and_skip, State.WAITING),
            (self.step_interrupt_resume,     State.WAITING),
            (self.step_fail,                 State.WAITING),
            (self.step_on_fail,              State.WAITING),
            (self.step_unskip,               State.TOSKIP),
        ])

        result = self.client.resume(name=str(self.step_pause_resume), dry_run=False)
        self.assertEqual(result, [self.step_pause_resume.id])

        self._wait_until(self.step_interrupt_resume, State.RUNNING)

        result = self.client.interrupt(name=str(self.step_interrupt_resume), dry_run=False)
        self.assertEqual(result, [self.step_interrupt_resume.id])

        self._wait_until(self.step_interrupt_resume, State.INTERRUPTED)
        self._wait_until(self.step_error_rerun_and_skip, State.ERROR)
        self._wait_until(self.step_fail, State.FAILED)
        self._wait_until(self.step_on_fail, State.SUCCEEDED)

        self._check_status([
            (self.step_output,               State.SUCCEEDED),
            (self.step_instruction,          State.SUCCEEDED),
            (self.step_pause_resume,         State.SUCCEEDED),
            (self.step_error_rerun_and_skip, State.ERROR),
            (self.step_interrupt_resume,     State.INTERRUPTED),
            (self.step_fail,                 State.FAILED),
            (self.step_on_fail,              State.SUCCEEDED),
            (self.step_unskip,               State.TOSKIP),
        ])

        result = self.client.resume(name=str(self.step_interrupt_resume), dry_run=False)
        self.assertEqual(result, [self.step_interrupt_resume.id])

        self._wait_until(self.step_interrupt_resume, State.SUCCEEDED)

        self._check_status([
            (self.step_output,               State.SUCCEEDED),
            (self.step_instruction,          State.SUCCEEDED),
            (self.step_pause_resume,         State.SUCCEEDED),
            (self.step_error_rerun_and_skip, State.ERROR),
            (self.step_interrupt_resume,     State.SUCCEEDED),
            (self.step_fail,                 State.FAILED),
            (self.step_on_fail,              State.SUCCEEDED),
            (self.step_unskip,               State.TOSKIP),
        ])

        result = self.client.rerun(name=str(self.step_error_rerun_and_skip), dry_run=False)
        self.assertEqual(result, [self.step_error_rerun_and_skip.id])

        self._wait_until(self.step_error_rerun_and_skip, State.RUNNING)

        self._check_status([
            (self.step_output,               State.SUCCEEDED),
            (self.step_instruction,          State.SUCCEEDED),
            (self.step_pause_resume,         State.SUCCEEDED),
            (self.step_error_rerun_and_skip, State.RUNNING),
            (self.step_interrupt_resume,     State.SUCCEEDED),
            (self.step_fail,                 State.FAILED),
            (self.step_on_fail,              State.SUCCEEDED),
            (self.step_unskip,               State.TOSKIP),
        ])

        self._wait_until(self.step_error_rerun_and_skip, State.ERROR)

        result = self.client.unskip(name=str(self.step_unskip), dry_run=False)
        self.assertEqual(result, [self.step_unskip.id])

        result = self.client.skip(name=str(self.step_error_rerun_and_skip), dry_run=False)
        self.assertEqual(result, [self.step_error_rerun_and_skip.id])

        self._wait_until(self.step_unskip, State.SUCCEEDED)
        self._check_status([
            (self.step_output,               State.SUCCEEDED),
            (self.step_instruction,          State.SUCCEEDED),
            (self.step_pause_resume,         State.SUCCEEDED),
            (self.step_error_rerun_and_skip, State.SKIPPED),
            (self.step_interrupt_resume,     State.SUCCEEDED),
            (self.step_fail,                 State.FAILED),
            (self.step_on_fail,              State.SUCCEEDED),
            (self.step_unskip,               State.SUCCEEDED),
        ])

        result = self.client.get_serialized_context()
        # The Exception objects need to be converted to strings for a normalized comparision.
        result['step_data'][4]['exception'] = str(result['step_data'][4]['exception'])
        result['step_data'][5]['exception'] = str(result['step_data'][5]['exception'])
        self.assertEqual(result['step_data'], {
                0: {'output': ['Test output message.'],
                    'exception': None,
                    'return_value': None,
                    'io': []},
                1: {'output': [],
                    'exception': None,
                    'return_value': None,
                    'io': ['Test instruction.']},
                2: {'output': [],
                    'exception': None,
                    'return_value': 'Return value of pause_resume.',
                    'io': []},
                3: {'output': [],
                    'exception': None,
                    'return_value': 'Return value of interrupt_resume.',
                    'io': []},
                4: {'output': [],
                    'exception': str(RuntimeError('Exception from error_rerun_and_skip.',)),
                    'return_value': None,
                    'io': []},
                5: {'output': [],
                    'exception': str(StepFailed('Exception from fail.',)),
                    'return_value': None,
                    'io': []},
                6: {'output': [],
                    'exception': None,
                    'return_value': 'Return value of on_fail.',
                    'io': []},
                7: {'output': [],
                    'exception': None,
                    'return_value': 'Return value of unskip.',
                    'io': []}})

        result = self.client.shutdown(dry_run=False)
        self.assertEqual(result, [])

        self.workflow_manager.join(DELAY)
        self.assertFalse(self.workflow_manager.is_workflow_alive())
        self.assertFalse(self.workflow_manager.is_api_server_alive())
