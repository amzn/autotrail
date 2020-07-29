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

Ordering
                                (on success)
                            +--> choice_a_1 -->+
           +--> branch_a -->|                  |--> branch_a_end
           |                +--> choice_a_2 -->+
           |                    (on failure)
  first -->+
           |                    (on success)
           |                +--> choice_b_1 -->+
           +--> branch_b -->|                  |--> branch_b_end
                            +--> choice_b_2 -->+
                                (on failure)

"""
import pytest
import unittest

from multiprocessing import Manager
from time import sleep

from autotrail.core.api.callbacks import (ChainActionCallbacks, AutomatedActionCallback, InjectedActionCallback,
                                          StatesCallback, TransitionsCallback)
from autotrail.core.state_machine import state_machine_evaluator
from autotrail.workflow.helpers.execution import run_function_as_execption_safe_subprocess


DELAY = 0.5

class State:
    READY = 'Ready'
    WAIT = 'Waiting'
    RUN = 'Running'
    SUCCESS = 'Successful'
    FAILURE = 'Failed'


class Action:
    START = 'Start'
    RUN = 'Run'
    SUCCEED = 'Success check'
    FAIL = 'Failure check'


class Step:
    def __init__(self, name, action_function):
        self.name = name
        self.action_function = action_function
        self.result = None
        self.error = None

    def __call__(self, context):
        try:
            self.result = self.action_function(context)
        except Exception as e:
            self.error = e

    def __str__(self):
        return self.name


def first(context):
    context['first'] = True
    return 'Return value of first'


def branch_a(context):
    context['branch_a'] = False
    raise RuntimeError('Exception from branch_a')


def branch_b(context):
    context['branch_b'] = True
    return 'Return value of branch_b'


def choice_a_1(context):
    context['choice_a_1'] = True
    return 'Return value of choice_a_1'


def choice_a_2(context):
    context['choice_a_2'] = False
    return 'Return value of choice_a_2'


def choice_b_1(context):
    context['choice_b_1'] = True
    return 'Return value of choice_b_1'


def choice_b_2(context):
    context['choice_b_2'] = False
    return 'Return value of choice_b_2'


def branch_a_end(context):
    context['branch_a_end'] = True
    return 'Return value of branch_a_end'


def branch_b_end(context):
    context['branch_b_end'] = True
    return 'Return value of branch_b_end'


@pytest.mark.validationtest
class BasicValidationTests(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

        self.step_first = Step('first', first)
        self.step_branch_a = Step('branch_a', branch_a)
        self.step_branch_b = Step('branch_b', branch_b)
        self.step_choice_a_1 = Step('choice_a_1', choice_a_1)
        self.step_choice_a_2 = Step('choice_a_2', choice_a_2)
        self.step_choice_b_1 = Step('choice_b_1', choice_b_1)
        self.step_choice_b_2 = Step('choice_b_2', choice_b_2)
        self.step_branch_a_end = Step('branch_a_end', branch_a_end)
        self.step_branch_b_end = Step('branch_b_end', branch_b_end)

        self.machine_name_to_object_mapping = {
            'first': self.step_first,
            'branch_a': self.step_branch_a,
            'branch_b': self.step_branch_b,
            'choice_a_1': self.step_choice_a_1,
            'choice_a_2': self.step_choice_a_2,
            'choice_b_1': self.step_choice_b_1,
            'choice_b_2': self.step_choice_b_2,
            'branch_a_end': self.step_branch_a_end,
            'branch_b_end': self.step_branch_b_end,
        }

        self.state_machine_definitions = {
            'first': (State.READY,   {
                State.READY:   {Action.START:      (State.WAIT,        [])},
                State.WAIT:    {Action.RUN:        (State.RUN,         [])},
                State.RUN:     {Action.SUCCEED:    (State.SUCCESS,     []),
                                Action.FAIL:       (State.FAILURE,     [])}}),
            'branch_a': (State.READY,   {
                State.READY:   {Action.START:      (State.WAIT,        [])},
                State.WAIT:    {Action.RUN:        (State.RUN,         [{'first': [State.SUCCESS]}])},
                State.RUN:     {Action.SUCCEED:    (State.SUCCESS,     []),
                                Action.FAIL:       (State.FAILURE,     [])}}),
            'branch_b': (State.READY,   {
                State.READY:   {Action.START:      (State.WAIT,        [])},
                State.WAIT:    {Action.RUN:        (State.RUN,         [{'first': [State.SUCCESS]}])},
                State.RUN:     {Action.SUCCEED:    (State.SUCCESS,     []),
                                Action.FAIL:       (State.FAILURE,     [])}}),
            'choice_a_1': (State.READY,   {
                State.READY:   {Action.START:      (State.WAIT,        [])},
                State.WAIT:    {Action.RUN:        (State.RUN,         [{'branch_a': [State.SUCCESS]}])},
                State.RUN:     {Action.SUCCEED:    (State.SUCCESS,     []),
                                Action.FAIL:       (State.FAILURE,     [])}}),
            'choice_a_2': (State.READY,   {
                State.READY:   {Action.START:      (State.WAIT,        [])},
                State.WAIT:    {Action.RUN:        (State.RUN,         [{'branch_a': [State.FAILURE]}])},
                State.RUN:     {Action.SUCCEED:    (State.SUCCESS,     []),
                                Action.FAIL:       (State.FAILURE,     [])}}),
            'choice_b_1': (State.READY,   {
                State.READY:   {Action.START:      (State.WAIT,        [])},
                State.WAIT:    {Action.RUN:        (State.RUN,         [{'branch_b': [State.SUCCESS]}])},
                State.RUN:     {Action.SUCCEED:    (State.SUCCESS,     []),
                                Action.FAIL:       (State.FAILURE,     [])}}),
            'choice_b_2': (State.READY,   {
                State.READY:   {Action.START:      (State.WAIT,        [])},
                State.WAIT:    {Action.RUN:        (State.RUN,         [{'branch_b': [State.FAILURE]}])},
                State.RUN:     {Action.SUCCEED:    (State.SUCCESS,     []),
                                Action.FAIL:       (State.FAILURE,     [])}}),
            'branch_a_end': (State.READY,   {
                State.READY:   {Action.START:      (State.WAIT,        [])},
                State.WAIT:    {Action.RUN:        (State.RUN,         [{'choice_a_1': [State.SUCCESS]},
                                                                        {'choice_a_2': [State.SUCCESS]}])},
                State.RUN:     {Action.SUCCEED:    (State.SUCCESS,     []),
                                Action.FAIL:       (State.FAILURE,     [])}}),
            'branch_b_end': (State.READY,   {
                State.READY:   {Action.START:      (State.WAIT,        [])},
                State.WAIT:    {Action.RUN:        (State.RUN,         [{'choice_b_1': [State.SUCCESS]},
                                                                        {'choice_b_2': [State.SUCCESS]}])},
                State.RUN:     {Action.SUCCEED:    (State.SUCCESS,     []),
                                Action.FAIL:       (State.FAILURE,     [])}}),
        }

        self.check_step = lambda machine, context: machine.result is not None

        # There is no automated action for starting the workflow.
        # This workflow will require the Action.START to be injected to start running.
        action_evaluations = {
            # Action             Function/worker                            Successful action
            Action.RUN:         (lambda machine, context: machine(context), None),
            Action.SUCCEED:     (self.check_step,                           True),
            Action.FAIL:        (self.check_step,                           False),
        }
        machine_action_definition = {m: action_evaluations for m in self.machine_name_to_object_mapping}
        self._context = Manager().dict()

        automated_action_callback = AutomatedActionCallback(self.machine_name_to_object_mapping, self._context,
                                                            machine_action_definition)
        self._states_callback = StatesCallback()
        self._transitions_callback = TransitionsCallback()
        self.injected_actions_callback = InjectedActionCallback()
        self.action_callback = ChainActionCallbacks([self._states_callback, self._transitions_callback,
                                                     automated_action_callback, self.injected_actions_callback])

    @property
    def states(self):
        return dict(self._states_callback.states)

    @property
    def transitions(self):
        return dict(self._transitions_callback.transitions)

    @property
    def context(self):
        return dict(self._context)

    def _wait_until(self, expected_states, delay=DELAY, max_retries=50):
        retry_count = 0
        while retry_count < max_retries:
            if any(self.states[machine_name] != expected_state
                   for machine_name, expected_state in expected_states.items()):
                retry_count += 1
                sleep(delay)
            else:
                return

        raise RuntimeError('The expected states {} was not reached after {} retries. Last state reached: {}'.format(
            expected_states, max_retries, self.states))

    def _check_states_and_actions(self, states_and_actions):
        for step_name, (expected_state, expected_actions) in states_and_actions.items():
            self.assertEqual(expected_state, self.states.get(step_name))
            self.assertEqual(expected_actions, list(self.transitions.get(step_name, {})))

    def test(self):
        process, result_queue = run_function_as_execption_safe_subprocess(state_machine_evaluator,
                                                                          self.state_machine_definitions,
                                                                          self.action_callback)
        self.run_tests(process, result_queue)

    def run_tests(self, process, result_queue):
        sleep(DELAY)

        self._check_states_and_actions({
            str(self.step_first):           (State.READY,   [Action.START]),
            str(self.step_branch_a):        (State.READY,   [Action.START]),
            str(self.step_branch_b):        (State.READY,   [Action.START]),
            str(self.step_choice_a_1):      (State.READY,   [Action.START]),
            str(self.step_choice_a_2):      (State.READY,   [Action.START]),
            str(self.step_choice_b_1):      (State.READY,   [Action.START]),
            str(self.step_choice_b_2):      (State.READY,   [Action.START]),
            str(self.step_branch_a_end):    (State.READY,   [Action.START]),
            str(self.step_branch_b_end):    (State.READY,   [Action.START])})

        sleep(DELAY)

        # The machines should not start, i.e., be in the same state as before
        self._check_states_and_actions({
            str(self.step_first):           (State.READY,   [Action.START]),
            str(self.step_branch_a):        (State.READY,   [Action.START]),
            str(self.step_branch_b):        (State.READY,   [Action.START]),
            str(self.step_choice_a_1):      (State.READY,   [Action.START]),
            str(self.step_choice_a_2):      (State.READY,   [Action.START]),
            str(self.step_choice_b_1):      (State.READY,   [Action.START]),
            str(self.step_choice_b_2):      (State.READY,   [Action.START]),
            str(self.step_branch_a_end):    (State.READY,   [Action.START]),
            str(self.step_branch_b_end):    (State.READY,   [Action.START])})

        # Start only the first 3 steps.
        self.injected_actions_callback.actions_writer.send({
            str(self.step_first):           Action.START,
            str(self.step_branch_a):        Action.START,
            str(self.step_branch_b):        Action.START})

        self._wait_until({str(self.step_branch_a): State.FAILURE,
                          str(self.step_branch_b): State.SUCCESS})

        self._check_states_and_actions({
            str(self.step_first):           (State.SUCCESS, []),
            str(self.step_branch_a):        (State.FAILURE, []),
            str(self.step_branch_b):        (State.SUCCESS, []),
            str(self.step_choice_a_1):      (State.READY,   [Action.START]),
            str(self.step_choice_a_2):      (State.READY,   [Action.START]),
            str(self.step_choice_b_1):      (State.READY,   [Action.START]),
            str(self.step_choice_b_2):      (State.READY,   [Action.START]),
            str(self.step_branch_a_end):    (State.READY,   [Action.START]),
            str(self.step_branch_b_end):    (State.READY,   [Action.START])})

        sleep(DELAY)

        # The rest of the machines should not have started, i.e., be in the same state as before
        self._check_states_and_actions({
            str(self.step_first):           (State.SUCCESS, []),
            str(self.step_branch_a):        (State.FAILURE, []),
            str(self.step_branch_b):        (State.SUCCESS, []),
            str(self.step_choice_a_1):      (State.READY,   [Action.START]),
            str(self.step_choice_a_2):      (State.READY,   [Action.START]),
            str(self.step_choice_b_1):      (State.READY,   [Action.START]),
            str(self.step_choice_b_2):      (State.READY,   [Action.START]),
            str(self.step_branch_a_end):    (State.READY,   [Action.START]),
            str(self.step_branch_b_end):    (State.READY,   [Action.START])})

        # Start the rest of the steps.
        self.injected_actions_callback.actions_writer.send({
            str(self.step_choice_a_1):      Action.START,
            str(self.step_choice_a_2):      Action.START,
            str(self.step_choice_b_1):      Action.START,
            str(self.step_choice_b_2):      Action.START,
            str(self.step_branch_a_end):    Action.START,
            str(self.step_branch_b_end):    Action.START})

        for _ in range(3):
            if process.is_alive():
                sleep(DELAY)

        self.assertFalse(process.is_alive())

        result, error = result_queue.get_nowait()

        self.assertIsNone(error)

        self.assertEqual(self.states, {
            str(self.step_first):           State.SUCCESS,
            str(self.step_branch_a):        State.FAILURE,
            str(self.step_branch_b):        State.SUCCESS,
            str(self.step_choice_a_1):      State.WAIT,
            str(self.step_choice_a_2):      State.SUCCESS,
            str(self.step_choice_b_1):      State.SUCCESS,
            str(self.step_choice_b_2):      State.WAIT,
            str(self.step_branch_a_end):    State.SUCCESS,
            str(self.step_branch_b_end):    State.SUCCESS})

        self.assertEqual(self.transitions, {
            str(self.step_first):           {},
            str(self.step_branch_a):        {},
            str(self.step_branch_b):        {},
            str(self.step_choice_a_1):      {},
            str(self.step_choice_a_2):      {},
            str(self.step_choice_b_1):      {},
            str(self.step_choice_b_2):      {},
            str(self.step_branch_a_end):    {},
            str(self.step_branch_b_end):    {}})

        self.assertEqual(self.context, {
            str(self.step_first):           True,
            str(self.step_branch_a):        False,
            str(self.step_branch_b):        True,
            str(self.step_choice_a_2):      False,
            str(self.step_choice_b_1):      True,
            str(self.step_branch_a_end):    True,
            str(self.step_branch_b_end):    True})
