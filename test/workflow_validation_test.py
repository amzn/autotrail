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
 first --> pause_resume -->|                                      +--> unskip --> last
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
2. Pause the "pause_resume" step and verify it has taken effect (subsequent steps should not be running).
3. Skip the "unskip" step as we will test unskipping it later.
4. Resume the "pause_resume" step. Verify the subsequent steps run.
5. Wait for the "interrupt_resume" step to start running and interrupt it.
6. Ensure the "error_rerun_and_skip" and "fail" steps have failed.
7. Ensure the "on_fail" step is running/succeeded.
8. Rerun the "error_rerun_and_skip" step and verify it goes back into "Running".
9. Unskip the "unskip" step.
10. Skip the "error_rerun_and_skip" step.
11. Verify that the "unskip" and "last" steps completed successfully.

"""
import pytest
import unittest

from operator import methodcaller
from time import sleep

from autotrail.core.state_machine import run_state_machine_evaluator
from autotrail.core.api.management import ConnectionClient, APIRequest, APIHandlerResponse
from autotrail.core.api.callbacks import ManagedCallback
from autotrail.core.api.serializers import SerializerCallable, Serializer
from autotrail.workflow.default_workflow.state_machine import State, Action, make_state_machine_definitions
from autotrail.workflow.helpers.step import Step


DELAY = 0.5


class RetryableError(Exception):
    pass


def first():
    return 'Return value of first step.'


def pause_resume():
    return 'Return value of pause_resume.'


def interrupt_resume():
    sleep(DELAY * 2)
    return 'Return value of interrupt_resume.'


def error_rerun_and_skip():
    sleep(DELAY * 2)
    raise RetryableError('Exception from error_rerun_and_skip.')


def fail():
    raise RuntimeError('Exception from fail.')


def on_fail():
    return 'Return value of on_fail.'


def unskip():
    return 'Return value of unskip.'


def last():
    return 'Return value of last.'


class AutomaticActions:
    @staticmethod
    def check_step(step, context):
        if step.is_alive():
            return 'running'

        return_value, exception = step.get_result()
        context[step.id] = dict(return_value=return_value, exception=exception)

        if exception is None:
            return 'success'
        elif isinstance(exception, RetryableError):
            return 'tempfail'
        else:
            return 'failure'

    @staticmethod
    def start(step, context):
        step.start()

    @staticmethod
    def noop(step, context):
        pass


class APIHandlers:
    def __init__(self, step_id_to_object_mapping, context):
        self._step_id_to_object_mapping = step_id_to_object_mapping
        self._context = context

    def interrupt(self, states, transitions, machine_name):
        if Action.INTERRUPT in transitions[machine_name]:
            self._step_id_to_object_mapping[machine_name].terminate()
            return APIHandlerResponse(None, relay_value={machine_name: Action.INTERRUPT})
        else:
            return APIHandlerResponse(None, relay_value={})


@pytest.mark.validationtest
class WorkflowValidationTests(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

        self.step_first = Step(first)
        self.step_pause_resume = Step(pause_resume)
        self.step_interrupt_resume = Step(interrupt_resume)
        self.step_error_rerun_and_skip = Step(error_rerun_and_skip)
        self.step_fail = Step(fail)
        self.step_on_fail = Step(on_fail)
        self.step_unskip = Step(unskip)
        self.step_last = Step(last)

        self.step_id_to_object_mapping = {
            self.step_first.id: self.step_first,
            self.step_pause_resume.id: self.step_pause_resume,
            self.step_interrupt_resume.id: self.step_interrupt_resume,
            self.step_error_rerun_and_skip.id: self.step_error_rerun_and_skip,
            self.step_fail.id: self.step_fail,
            self.step_on_fail.id: self.step_on_fail,
            self.step_unskip.id: self.step_unskip,
            self.step_last.id: self.step_last,
        }
        machine_serializers = [SerializerCallable(step_object, key=step_object.id,
                                                  serializer_function=methodcaller('get_result'))
                               for step_object in self.step_id_to_object_mapping.values()]

        # Ordering -- See the docstring at the top of this test suite for details.
        success_pairs = [
            (self.step_first, self.step_pause_resume),
            (self.step_pause_resume, self.step_interrupt_resume),
            (self.step_pause_resume, self.step_error_rerun_and_skip),
            (self.step_pause_resume, self.step_fail),
            (self.step_interrupt_resume, self.step_unskip),
            (self.step_error_rerun_and_skip, self.step_unskip),
            (self.step_on_fail, self.step_unskip),
            (self.step_unskip, self.step_last)
        ]

        failure_pairs = [
            (self.step_fail, self.step_on_fail),
        ]

        generated_state_machine_definitions = make_state_machine_definitions(success_pairs, failure_pairs)

        declared_state_machine_definitions = {
            self.step_first.id: (State.READY, {
                State.READY: {
                    Action.START:       (State.WAITING,      []),
                    Action.PAUSE:       (State.PAUSED,       []),
                    Action.MARKSKIP:    (State.TOSKIP,       [])},
                State.WAITING: {
                    Action.RUN:          (State.RUNNING,     []),
                    Action.PAUSE:        (State.PAUSED,      []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.TOSKIP: {
                    Action.UNSKIP:       (State.WAITING,     []),
                    Action.SKIP:         (State.SKIPPED,     [])},
                State.PAUSED: {
                    Action.RESUME:       (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.ERROR: {
                    Action.RERUN:        (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.RUNNING: {
                    Action.SUCCEED:      (State.SUCCEEDED,   []),
                    Action.FAIL:         (State.FAILED,      []),
                    Action.INTERRUPT:    (State.INTERRUPTED, []),
                    Action.ERROR:        (State.ERROR,       [])},
                State.INTERRUPTED: {
                    Action.RESUME:       (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])}
            }),
            self.step_pause_resume.id: (State.READY, {
                State.READY: {
                    Action.START:       (State.WAITING,      []),
                    Action.PAUSE:       (State.PAUSED,       []),
                    Action.MARKSKIP:    (State.TOSKIP,       [])},
                State.WAITING: {
                    Action.RUN:          (State.RUNNING,     [
                        {self.step_first.id:                  [State.SUCCEEDED, State.SKIPPED]}
                    ]),
                    Action.PAUSE:        (State.PAUSED,      []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.TOSKIP: {
                    Action.UNSKIP:       (State.WAITING,     []),
                    Action.SKIP:         (State.SKIPPED,     [
                        {self.step_first.id:                  [State.SUCCEEDED, State.SKIPPED]}
                    ])},
                State.PAUSED: {
                    Action.RESUME:       (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.ERROR: {
                    Action.RERUN:        (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.RUNNING: {
                    Action.SUCCEED:      (State.SUCCEEDED,   []),
                    Action.FAIL:         (State.FAILED,      []),
                    Action.INTERRUPT:    (State.INTERRUPTED, []),
                    Action.ERROR:        (State.ERROR,       [])},
                State.INTERRUPTED: {
                    Action.RESUME:       (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])}
            }),
            self.step_interrupt_resume.id: (State.READY, {
                State.READY: {
                    Action.START:       (State.WAITING,      []),
                    Action.PAUSE:       (State.PAUSED,       []),
                    Action.MARKSKIP:    (State.TOSKIP,       [])},
                State.WAITING: {
                    Action.RUN:          (State.RUNNING,     [
                        {self.step_pause_resume.id:           [State.SUCCEEDED, State.SKIPPED],
                         self.step_first.id:                  [State.SUCCEEDED, State.SKIPPED]}
                    ]),
                    Action.PAUSE:        (State.PAUSED,      []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.TOSKIP: {
                    Action.UNSKIP:       (State.WAITING,     []),
                    Action.SKIP:         (State.SKIPPED,     [
                        {self.step_pause_resume.id:           [State.SUCCEEDED, State.SKIPPED],
                         self.step_first.id:                  [State.SUCCEEDED, State.SKIPPED]}
                    ])},
                State.PAUSED: {
                    Action.RESUME:       (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.ERROR: {
                    Action.RERUN:        (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.RUNNING: {
                    Action.SUCCEED:      (State.SUCCEEDED,   []),
                    Action.FAIL:         (State.FAILED,      []),
                    Action.INTERRUPT:    (State.INTERRUPTED, []),
                    Action.ERROR:        (State.ERROR,       [])},
                State.INTERRUPTED: {
                    Action.RESUME:       (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])}
            }),
            self.step_error_rerun_and_skip.id: (State.READY, {
                State.READY: {
                    Action.START:       (State.WAITING,      []),
                    Action.PAUSE:       (State.PAUSED,       []),
                    Action.MARKSKIP:    (State.TOSKIP,       [])},
                State.WAITING: {
                    Action.RUN:          (State.RUNNING,     [
                        {self.step_pause_resume.id:           [State.SUCCEEDED, State.SKIPPED],
                         self.step_first.id:                  [State.SUCCEEDED, State.SKIPPED]}
                    ]),
                    Action.PAUSE:        (State.PAUSED,      []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.TOSKIP: {
                    Action.UNSKIP:       (State.WAITING,     []),
                    Action.SKIP:         (State.SKIPPED,     [
                        {self.step_pause_resume.id:           [State.SUCCEEDED, State.SKIPPED],
                         self.step_first.id:                  [State.SUCCEEDED, State.SKIPPED]}
                    ])},
                State.PAUSED: {
                    Action.RESUME:       (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.ERROR: {
                    Action.RERUN:        (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.RUNNING: {
                    Action.SUCCEED:      (State.SUCCEEDED,   []),
                    Action.FAIL:         (State.FAILED,      []),
                    Action.INTERRUPT:    (State.INTERRUPTED, []),
                    Action.ERROR:        (State.ERROR,       [])},
                State.INTERRUPTED: {
                    Action.RESUME:       (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])}
            }),
            self.step_fail.id: (State.READY, {
                State.READY: {
                    Action.START:       (State.WAITING,      []),
                    Action.PAUSE:       (State.PAUSED,       []),
                    Action.MARKSKIP:    (State.TOSKIP,       [])},
                State.WAITING: {
                    Action.RUN:          (State.RUNNING,     [
                        {self.step_pause_resume.id:           [State.SUCCEEDED, State.SKIPPED],
                         self.step_first.id:                  [State.SUCCEEDED, State.SKIPPED]}
                    ]),
                    Action.PAUSE:        (State.PAUSED,      []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.TOSKIP: {
                    Action.UNSKIP:       (State.WAITING,     []),
                    Action.SKIP:         (State.SKIPPED,     [
                        {self.step_pause_resume.id:           [State.SUCCEEDED, State.SKIPPED],
                         self.step_first.id:                  [State.SUCCEEDED, State.SKIPPED]}
                    ])},
                State.PAUSED: {
                    Action.RESUME:       (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.ERROR: {
                    Action.RERUN:        (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.RUNNING: {
                    Action.SUCCEED:      (State.SUCCEEDED,   []),
                    Action.FAIL:         (State.FAILED,      []),
                    Action.INTERRUPT:    (State.INTERRUPTED, []),
                    Action.ERROR:        (State.ERROR,       [])},
                State.INTERRUPTED: {
                    Action.RESUME:       (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])}
            }),
            self.step_on_fail.id: (State.READY, {
                State.READY: {
                    Action.START:       (State.WAITING,      []),
                    Action.PAUSE:       (State.PAUSED,       []),
                    Action.MARKSKIP:    (State.TOSKIP,       [])},
                State.WAITING: {
                    Action.RUN:          (State.RUNNING,     [
                        {self.step_fail.id: [State.FAILED]}
                    ]),
                    Action.PAUSE:        (State.PAUSED,      []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.TOSKIP: {
                    Action.UNSKIP:       (State.WAITING,     []),
                    Action.SKIP:         (State.SKIPPED,     [
                        {self.step_fail.id: [State.FAILED]}
                    ])},
                State.PAUSED: {
                    Action.RESUME:       (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.ERROR: {
                    Action.RERUN:        (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.RUNNING: {
                    Action.SUCCEED:      (State.SUCCEEDED,   []),
                    Action.FAIL:         (State.FAILED,      []),
                    Action.INTERRUPT:    (State.INTERRUPTED, []),
                    Action.ERROR:        (State.ERROR,       [])},
                State.INTERRUPTED: {
                    Action.RESUME:       (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])}
            }),
            self.step_unskip.id: (State.READY, {
                State.READY: {
                    Action.START:       (State.WAITING,      []),
                    Action.PAUSE:       (State.PAUSED,       []),
                    Action.MARKSKIP:    (State.TOSKIP,       [])},
                State.WAITING: {
                    Action.RUN:          (State.RUNNING,     [
                        {self.step_interrupt_resume.id:       [State.SUCCEEDED, State.SKIPPED],
                         self.step_error_rerun_and_skip.id:   [State.SUCCEEDED, State.SKIPPED],
                         self.step_on_fail.id:                [State.SUCCEEDED, State.SKIPPED],
                         self.step_pause_resume.id:           [State.SUCCEEDED, State.SKIPPED],
                         self.step_first.id:                  [State.SUCCEEDED, State.SKIPPED]}
                    ]),
                    Action.PAUSE:        (State.PAUSED,      []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.TOSKIP: {
                    Action.UNSKIP:       (State.WAITING,     []),
                    Action.SKIP:         (State.SKIPPED,     [
                        {self.step_interrupt_resume.id:       [State.SUCCEEDED, State.SKIPPED],
                         self.step_error_rerun_and_skip.id:   [State.SUCCEEDED, State.SKIPPED],
                         self.step_on_fail.id:                [State.SUCCEEDED, State.SKIPPED],
                         self.step_pause_resume.id:           [State.SUCCEEDED, State.SKIPPED],
                         self.step_first.id:                  [State.SUCCEEDED, State.SKIPPED]}
                    ])},
                State.PAUSED: {
                    Action.RESUME:       (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.ERROR: {
                    Action.RERUN:        (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.RUNNING: {
                    Action.SUCCEED:      (State.SUCCEEDED,   []),
                    Action.FAIL:         (State.FAILED,      []),
                    Action.INTERRUPT:    (State.INTERRUPTED, []),
                    Action.ERROR:        (State.ERROR,       [])},
                State.INTERRUPTED: {
                    Action.RESUME:       (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])}
            }),
            self.step_last.id: (State.READY, {
                State.READY: {
                    Action.START:       (State.WAITING,      []),
                    Action.PAUSE:       (State.PAUSED,       []),
                    Action.MARKSKIP:    (State.TOSKIP,       [])},
                State.WAITING: {
                    Action.RUN:          (State.RUNNING,     [
                        {self.step_unskip.id:                 [State.SUCCEEDED, State.SKIPPED],
                         self.step_interrupt_resume.id:       [State.SUCCEEDED, State.SKIPPED],
                         self.step_error_rerun_and_skip.id:   [State.SUCCEEDED, State.SKIPPED],
                         self.step_on_fail.id:                [State.SUCCEEDED, State.SKIPPED],
                         self.step_pause_resume.id:           [State.SUCCEEDED, State.SKIPPED],
                         self.step_first.id:                  [State.SUCCEEDED, State.SKIPPED]}
                    ]),
                    Action.PAUSE:        (State.PAUSED,      []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.TOSKIP: {
                    Action.UNSKIP:       (State.WAITING,     []),
                    Action.SKIP:         (State.SKIPPED,     [
                        {self.step_unskip.id:                 [State.SUCCEEDED, State.SKIPPED],
                         self.step_interrupt_resume.id:       [State.SUCCEEDED, State.SKIPPED],
                         self.step_error_rerun_and_skip.id:   [State.SUCCEEDED, State.SKIPPED],
                         self.step_on_fail.id:                [State.SUCCEEDED, State.SKIPPED],
                         self.step_pause_resume.id:           [State.SUCCEEDED, State.SKIPPED],
                         self.step_first.id:                  [State.SUCCEEDED, State.SKIPPED]}
                    ])},
                State.PAUSED: {
                    Action.RESUME:       (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.ERROR: {
                    Action.RERUN:        (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])},
                State.RUNNING: {
                    Action.SUCCEED:      (State.SUCCEEDED,   []),
                    Action.FAIL:         (State.FAILED,      []),
                    Action.INTERRUPT:    (State.INTERRUPTED, []),
                    Action.ERROR:        (State.ERROR,       [])},
                State.INTERRUPTED: {
                    Action.RESUME:       (State.WAITING,     []),
                    Action.MARKSKIP:     (State.TOSKIP,      [])}
            }),
        }

        self.assertEqual(generated_state_machine_definitions, declared_state_machine_definitions)

        action_evaluations = {
            # Action            Function/worker                 Successful action
            Action.RUN:         (AutomaticActions.start,        None),
            Action.SUCCEED:     (AutomaticActions.check_step,   'success'),
            Action.FAIL:        (AutomaticActions.check_step,   'failure'),
            Action.ERROR:       (AutomaticActions.check_step,   'tempfail'),
            Action.SKIP:        (AutomaticActions.noop,         None)}
        machine_action_definition = {m: action_evaluations for m in self.step_id_to_object_mapping}

        self.context = {}
        self.callback_manager = ManagedCallback(APIHandlers(self.step_id_to_object_mapping, self.context),
                                                machine_action_definition,
                                                self.step_id_to_object_mapping,
                                                context=self.context,
                                                machine_serializer=Serializer(machine_serializers),
                                                context_serializer=Serializer([
                                                    SerializerCallable(self.context, key='context')]),
                                                delay=DELAY)
        self.process = run_state_machine_evaluator(generated_state_machine_definitions, self.callback_manager)
        self.machine_api_client = ConnectionClient(self.callback_manager.api_client_connection)
        self.process.start()
        sleep(DELAY * 2)

    @property
    def states(self):
        return dict(self.callback_manager.states)

    @property
    def transitions(self):
        return dict(self.callback_manager.transitions)

    @property
    def context_serialized(self):
        return dict(self.callback_manager.context_serialized['context'])

    @property
    def machines_serialized(self):
        return dict(self.callback_manager.machines_serialized)

    def check_states(self, expected_states_and_actions):
        for machine_name, (expected_state, expected_actions) in expected_states_and_actions.items():
            self.assertEqual(expected_state, self.states[machine_name])
            self.assertEqual(sorted(expected_actions), sorted(self.transitions[machine_name].keys()))

    def check_return_value(self, step, return_value):
        self.assertEqual(self.context_serialized[step.id], {'exception': None, 'return_value': return_value})
        self.assertEqual(self.machines_serialized[step.id], (return_value, None))

    def check_exception(self, step, exception, message):
        return_value_from_context = self.context_serialized[step.id]['return_value']
        exception_from_context = self.context_serialized[step.id]['exception']
        return_value_from_step_object, exception_from_step_object = self.machines_serialized[step.id]

        self.assertIsNone(return_value_from_context)
        self.assertIsNone(return_value_from_step_object)

        self.assertIsInstance(exception_from_context, exception)
        self.assertIsInstance(exception_from_step_object, exception)

        self.assertEqual(str(exception_from_context), message)
        self.assertEqual(str(exception_from_step_object), message)

    def wait_until(self, step_name, state, delay=DELAY, max_tries=50):
        count = 0
        while self.states[step_name] != state:
            if count < max_tries:
                sleep(delay)
                count += 1
            else:
                raise RuntimeError(
                    'Timed out waiting for step "{}" to change state to "{}". Last known state is "{}".'.format(
                        step_name, state, self.states[step_name]))

    def test_workflow(self):
        self.assertEqual(self.context_serialized, {})
        self.assertEqual(self.machines_serialized, {self.step_first.id: None,
                                                    self.step_pause_resume.id: None,
                                                    self.step_interrupt_resume.id: None,
                                                    self.step_error_rerun_and_skip.id: None,
                                                    self.step_fail.id: None,
                                                    self.step_on_fail.id: None,
                                                    self.step_unskip.id: None,
                                                    self.step_last.id: None})
        self.check_states({
            self.step_first.id:                (State.READY,       [Action.START, Action.PAUSE, Action.MARKSKIP]),
            self.step_pause_resume.id:         (State.READY,       [Action.START, Action.PAUSE, Action.MARKSKIP]),
            self.step_interrupt_resume.id:     (State.READY,       [Action.START, Action.PAUSE, Action.MARKSKIP]),
            self.step_error_rerun_and_skip.id: (State.READY,       [Action.START, Action.PAUSE, Action.MARKSKIP]),
            self.step_fail.id:                 (State.READY,       [Action.START, Action.PAUSE, Action.MARKSKIP]),
            self.step_on_fail.id:              (State.READY,       [Action.START, Action.PAUSE, Action.MARKSKIP]),
            self.step_unskip.id:               (State.READY,       [Action.START, Action.PAUSE, Action.MARKSKIP]),
            self.step_last.id:                 (State.READY,       [Action.START, Action.PAUSE, Action.MARKSKIP])})

        self.callback_manager.actions_writer.send({step_id: Action.START for step_id in self.step_id_to_object_mapping})
        self.callback_manager.actions_writer.send({self.step_pause_resume.id: Action.PAUSE})
        self.callback_manager.actions_writer.send({self.step_unskip.id: Action.MARKSKIP})

        self.wait_until(self.step_first.id, State.SUCCEEDED)

        self.check_states({
            self.step_first.id:                (State.SUCCEEDED,   []),
            self.step_pause_resume.id:         (State.PAUSED,      [Action.MARKSKIP, Action.RESUME]),
            self.step_interrupt_resume.id:     (State.WAITING,     [Action.MARKSKIP, Action.PAUSE]),
            self.step_error_rerun_and_skip.id: (State.WAITING,     [Action.MARKSKIP, Action.PAUSE]),
            self.step_fail.id:                 (State.WAITING,     [Action.MARKSKIP, Action.PAUSE]),
            self.step_on_fail.id:              (State.WAITING,     [Action.MARKSKIP, Action.PAUSE]),
            self.step_unskip.id:               (State.TOSKIP,      [Action.UNSKIP]),
            self.step_last.id:                 (State.WAITING,     [Action.MARKSKIP, Action.PAUSE])})

        sleep(DELAY * 2)

        self.check_states({
            self.step_first.id:                (State.SUCCEEDED,   []),
            self.step_pause_resume.id:         (State.PAUSED,      [Action.MARKSKIP, Action.RESUME]),
            self.step_interrupt_resume.id:     (State.WAITING,     [Action.MARKSKIP, Action.PAUSE]),
            self.step_error_rerun_and_skip.id: (State.WAITING,     [Action.MARKSKIP, Action.PAUSE]),
            self.step_fail.id:                 (State.WAITING,     [Action.MARKSKIP, Action.PAUSE]),
            self.step_on_fail.id:              (State.WAITING,     [Action.MARKSKIP, Action.PAUSE]),
            self.step_unskip.id:               (State.TOSKIP,      [Action.UNSKIP]),
            self.step_last.id:                 (State.WAITING,     [Action.MARKSKIP, Action.PAUSE])})

        self.callback_manager.actions_writer.send({self.step_pause_resume.id: Action.RESUME})

        self.wait_until(self.step_interrupt_resume.id, State.RUNNING)

        self.machine_api_client(APIRequest('interrupt', (self.step_interrupt_resume.id,), {}))

        self.wait_until(self.step_interrupt_resume.id, State.INTERRUPTED)
        self.wait_until(self.step_error_rerun_and_skip.id, State.ERROR)
        self.wait_until(self.step_fail.id, State.FAILED)
        self.wait_until(self.step_on_fail.id, State.SUCCEEDED)

        self.check_states({
            self.step_first.id:                (State.SUCCEEDED,   []),
            self.step_pause_resume.id:         (State.SUCCEEDED,   []),
            self.step_interrupt_resume.id:     (State.INTERRUPTED, [Action.MARKSKIP, Action.RESUME]),
            self.step_error_rerun_and_skip.id: (State.ERROR,       [Action.MARKSKIP, Action.RERUN]),
            self.step_fail.id:                 (State.FAILED,      []),
            self.step_on_fail.id:              (State.SUCCEEDED,   []),
            self.step_unskip.id:               (State.TOSKIP,      [Action.UNSKIP]),
            self.step_last.id:                 (State.WAITING,     [Action.MARKSKIP, Action.PAUSE])})

        self.callback_manager.actions_writer.send({self.step_interrupt_resume.id: Action.RESUME})

        self.wait_until(self.step_interrupt_resume.id, State.SUCCEEDED)

        self.check_states({
            self.step_first.id:                (State.SUCCEEDED,   []),
            self.step_pause_resume.id:         (State.SUCCEEDED,   []),
            self.step_interrupt_resume.id:     (State.SUCCEEDED,   []),
            self.step_error_rerun_and_skip.id: (State.ERROR,       [Action.MARKSKIP, Action.RERUN]),
            self.step_fail.id:                 (State.FAILED,      []),
            self.step_on_fail.id:              (State.SUCCEEDED,   []),
            self.step_unskip.id:               (State.TOSKIP,      [Action.UNSKIP]),
            self.step_last.id:                 (State.WAITING,     [Action.MARKSKIP, Action.PAUSE])})

        self.callback_manager.actions_writer.send({self.step_error_rerun_and_skip.id: Action.RERUN})

        self.wait_until(self.step_error_rerun_and_skip.id, State.RUNNING)

        self.check_states({
            self.step_first.id:                (State.SUCCEEDED,   []),
            self.step_pause_resume.id:         (State.SUCCEEDED,   []),
            self.step_interrupt_resume.id:     (State.SUCCEEDED,   []),
            self.step_error_rerun_and_skip.id: (State.RUNNING,     [Action.INTERRUPT, Action.FAIL, Action.SUCCEED,
                                                                      Action.ERROR]),
            self.step_fail.id:                 (State.FAILED,      []),
            self.step_on_fail.id:              (State.SUCCEEDED,   []),
            self.step_unskip.id:               (State.TOSKIP,      [Action.UNSKIP]),
            self.step_last.id:                 (State.WAITING,     [Action.MARKSKIP, Action.PAUSE])})

        self.wait_until(self.step_error_rerun_and_skip.id, State.ERROR)

        self.callback_manager.actions_writer.send({self.step_unskip.id: Action.UNSKIP})
        self.callback_manager.actions_writer.send({self.step_error_rerun_and_skip.id: Action.MARKSKIP})

        self.wait_until(self.step_last.id, State.SUCCEEDED)
        self.check_states({
            self.step_first.id:                (State.SUCCEEDED,   []),
            self.step_pause_resume.id:         (State.SUCCEEDED,   []),
            self.step_interrupt_resume.id:     (State.SUCCEEDED,   []),
            self.step_error_rerun_and_skip.id: (State.SKIPPED,     []),
            self.step_fail.id:                 (State.FAILED,      []),
            self.step_on_fail.id:              (State.SUCCEEDED,   []),
            self.step_unskip.id:               (State.SUCCEEDED,   []),
            self.step_last.id:                 (State.SUCCEEDED,   [])})

        self.check_return_value(self.step_first,             'Return value of first step.')
        self.check_return_value(self.step_pause_resume,      'Return value of pause_resume.')
        self.check_return_value(self.step_interrupt_resume,  'Return value of interrupt_resume.')
        self.check_return_value(self.step_on_fail,           'Return value of on_fail.')
        self.check_return_value(self.step_unskip,            'Return value of unskip.')
        self.check_return_value(self.step_last,              'Return value of last.')

        self.check_exception(self.step_error_rerun_and_skip, RetryableError,  'Exception from error_rerun_and_skip.')
        self.check_exception(self.step_fail,                 RuntimeError,    'Exception from fail.')

        for _ in range(5):
            if not self.process.is_alive():
                break
            sleep(DELAY)

        self.assertFalse(self.process.is_alive())
