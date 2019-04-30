"""Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.

This is a validation test of AutoTrail.

It tests all the features end-to-end by doing an automatic trail run and generating API calls.
It inspects the trail run by verifying the responses received from the API calls.

This is not an intrusive test at all and therefore doesn't use any mocks or internal mechanisms not available to the
user.
"""

import logging
import os
import pytest
import unittest

from autotrail import TrailClient, TrailServer, Step, StatusField

from time import sleep


# Context definitions
class Context(object):
    def __init__(self, value):
        self.value = value


# Action Functions
def action_function_a(trail_env, context):
    sleep(0.2)
    return "This is action A. Value from context: {}".format(context.value)


def action_function_b(trail_env, context):
    sleep(0.2)
    return "This is action B. Value from context: {}".format(context.value)


def action_function_c(trail_env, context):
    sleep(0.2)
    return "This is action C. Value from context: {}".format(context.value)


def action_function_d(trail_env, context):
    sleep(0.2)
    return "This is action D. Value from context: {}".format(context.value)


def action_function_e(trail_env, context):
    sleep(0.2)
    return "This is action E. Value from context: {}".format(context.value)


def action_function_f(trail_env, context):
    sleep(0.2)
    return "This is action F. Value from context: {}".format(context.value)


def action_function_g(trail_env, context):
    sleep(0.2)
    return "This is action G. Value from context: {}".format(context.value)


@pytest.mark.validationtest
class AutoTrailValidationTests(unittest.TestCase):
    def setUp(self):
        self.step_a = Step(action_function_a)
        self.step_b = Step(action_function_b, foo='bar')
        self.step_c = Step(action_function_c)
        self.step_d = Step(action_function_d)
        self.step_e = Step(action_function_e)
        self.step_f = Step(action_function_f, foo='bar')
        self.step_g = Step(action_function_g)

        # Trail definition
        # The following example trail represents the following DAG:
        # The run-time (in seconds) of each step is mentioned below the step name.
        #         +----> step_b ----> step_c ----> step_d ----+
        #         |        1           1             1        |
        # step_a -|                                           +----> step_g
        #    1    |         1                         1       |        1
        #         +----> step_e -----------------> step_f ----+
        #
        test_trail_definition = [
            (self.step_a, self.step_b),
            (self.step_a, self.step_e),

            # First branch
            (self.step_b, self.step_c),
            (self.step_c, self.step_d),
            (self.step_d, self.step_g),

            # Second branch
            (self.step_e, self.step_f),
            (self.step_f, self.step_g),
        ]

        # Adding this for convenience in tests.
        self.step_list = [
            self.step_a,
            self.step_b,
            self.step_c,
            self.step_d,
            self.step_e,
            self.step_f,
            self.step_g,
        ]

        # Setup trail to run automatically (non-interactively)
        context = Context(42)
        self.trail_server = TrailServer(test_trail_definition, delay=0.1, context=context)
        self.trail_server.serve(threaded=True)
        self.trail_client = TrailClient(self.trail_server.socket_file)

    def tearDown(self):
        self.trail_client.stop(dry_run=False)
        self.trail_client.shutdown(dry_run=False)

    def check_step_attributes(self, attribute, expected_values, **status_kwargs):
        statuses = self.trail_client.status(**status_kwargs)
        self.assertNotEqual(statuses, None, 'Status returned was None.')
        self.assertNotEqual(statuses, [], 'Status returned was empty.')
        for status in statuses:
            step_name = status[StatusField.NAME]
            value = status[attribute]
            expected_value = expected_values[step_name]
            self.assertEqual(value, expected_value, (
                'The status of step {step_name} was expected to be "{expected}" but was found to be "{value}".'
            ).format(step_name=step_name, expected=expected_value,
                     value=value))

    def check_affected_steps(self, affected_steps, affected_step_names_expected):
        affected_step_names = [str(step['name']) for step in affected_steps]
        self.assertEqual(len(affected_step_names), len(affected_step_names_expected))
        for expected_step_name in affected_step_names_expected:
            self.assertIn(expected_step_name, affected_step_names, (
                'Expected {expected} to be affected but it was not. List of affected step names: {affected}'
            ).format(expected=expected_step_name, affected=affected_step_names))

    def wait_till(self, step_name, state):
        while True:
            statuses = self.trail_client.status(fields=[StatusField.STATE])
            for status in statuses:
                if status[StatusField.NAME] == step_name and status[StatusField.STATE] == state:
                    return
            sleep(0.2)

    def test_simple_run(self):
        self.check_step_attributes(StatusField.STATE, {
            'action_function_a': Step.READY,
            'action_function_b': Step.READY,
            'action_function_c': Step.READY,
            'action_function_d': Step.READY,
            'action_function_e': Step.READY,
            'action_function_f': Step.READY,
            'action_function_g': Step.READY,
        })

        affected_steps = self.trail_client.start()
        self.check_affected_steps(affected_steps, [
            'action_function_a',
            'action_function_b',
            'action_function_c',
            'action_function_d',
            'action_function_e',
            'action_function_f',
            'action_function_g',
        ])

        self.wait_till('action_function_g', Step.SUCCESS)

        self.check_step_attributes(StatusField.STATE, {
            'action_function_a': Step.SUCCESS,
            'action_function_b': Step.SUCCESS,
            'action_function_c': Step.SUCCESS,
            'action_function_d': Step.SUCCESS,
            'action_function_e': Step.SUCCESS,
            'action_function_f': Step.SUCCESS,
            'action_function_g': Step.SUCCESS,
        })

    def test_pause(self):
        # Pausing with dry_run should have no impact
        affected_steps = self.trail_client.pause()

        self.check_affected_steps(affected_steps, [
            'action_function_a',
            'action_function_b',
            'action_function_c',
            'action_function_d',
            'action_function_e',
            'action_function_f',
            'action_function_g',
        ])

        self.check_step_attributes(StatusField.STATE, {
            'action_function_a': Step.READY,
            'action_function_b': Step.READY,
            'action_function_c': Step.READY,
            'action_function_d': Step.READY,
            'action_function_e': Step.READY,
            'action_function_f': Step.READY,
            'action_function_g': Step.READY,
        })

        # Pause all steps
        affected_steps = self.trail_client.pause(dry_run=False)

        self.check_affected_steps(affected_steps, [
            'action_function_a',
            'action_function_b',
            'action_function_c',
            'action_function_d',
            'action_function_e',
            'action_function_f',
            'action_function_g',
        ])

        self.check_step_attributes(StatusField.STATE, {
            'action_function_a': Step.PAUSED,    # Control reaches here and pauses, so this will be the state.
            'action_function_b': Step.TOPAUSE,
            'action_function_c': Step.TOPAUSE,
            'action_function_d': Step.TOPAUSE,
            'action_function_e': Step.TOPAUSE,
            'action_function_f': Step.TOPAUSE,
            'action_function_g': Step.TOPAUSE,
        })

        affected_steps = self.trail_client.start()

        # Check whether a step in Step.TOPAUSE gets correctly transitioned to Step.PAUSED
        # Run action_function_a so that the control reaches action_function_b and action_function_e and transitions
        # them from Step.TOPAUSE to Step.PAUSED.
        affected_steps = self.trail_client.resume(name='action_function_a', dry_run=False)
        self.check_affected_steps(affected_steps, ['action_function_a'])

        self.wait_till('action_function_b', Step.PAUSED)
        self.wait_till('action_function_e', Step.PAUSED)

        self.check_step_attributes(StatusField.STATE, {
            'action_function_a': Step.SUCCESS,
            'action_function_b': Step.PAUSED,
            'action_function_c': Step.TOPAUSE,
            'action_function_d': Step.TOPAUSE,
            'action_function_e': Step.PAUSED,
            'action_function_f': Step.TOPAUSE,
            'action_function_g': Step.TOPAUSE,
        })

    def test_resume(self):
        affected_steps = self.trail_client.pause(dry_run=False)
        affected_steps = self.trail_client.start()

        affected_steps = self.trail_client.resume(name='action_function_a', dry_run=False)
        self.check_step_attributes(StatusField.STATE, {
            'action_function_a': Step.RUN,
            'action_function_b': Step.TOPAUSE,
            'action_function_c': Step.TOPAUSE,
            'action_function_d': Step.TOPAUSE,
            'action_function_e': Step.TOPAUSE,
            'action_function_f': Step.TOPAUSE,
            'action_function_g': Step.TOPAUSE,
        })
        self.check_affected_steps(affected_steps, ['action_function_a'])

        affected_steps = self.trail_client.resume(dry_run=False)
        self.check_affected_steps(affected_steps, [
            'action_function_b',
            'action_function_c',
            'action_function_d',
            'action_function_e',
            'action_function_f',
            'action_function_g',
        ])

        self.wait_till('action_function_g', Step.SUCCESS)

        # Check the end state (everything succeeded)
        self.check_step_attributes(StatusField.STATE, {
            'action_function_a': Step.SUCCESS,
            'action_function_b': Step.SUCCESS,
            'action_function_c': Step.SUCCESS,
            'action_function_d': Step.SUCCESS,
            'action_function_e': Step.SUCCESS,
            'action_function_f': Step.SUCCESS,
            'action_function_g': Step.SUCCESS,
        })

    def test_pause_branch_and_resume_branch(self):
        #         +----> step_b ----> step_c ----> step_d ----+
        #         |                                           |
        # step_a -|                                           +----> step_g
        #         |                                           |
        #         +----> step_e -----------------> step_f ----+
        #
        affected_steps = self.trail_client.pause_branch(name='action_function_b', dry_run=False)

        self.check_affected_steps(affected_steps, [
            'action_function_b',
            'action_function_c',
            'action_function_d',
        ])

        self.check_step_attributes(StatusField.STATE, {
            'action_function_a': Step.READY,
            'action_function_b': Step.TOPAUSE,
            'action_function_c': Step.TOPAUSE,
            'action_function_d': Step.TOPAUSE,
            'action_function_e': Step.READY,
            'action_function_f': Step.READY,
            'action_function_g': Step.READY,
        })

        affected_steps = self.trail_client.resume_branch(name='action_function_b', dry_run=False)

        self.check_affected_steps(affected_steps, [
            'action_function_b',
            'action_function_c',
            'action_function_d',
        ])

        self.check_step_attributes(StatusField.STATE, {
            'action_function_a': Step.READY,
            'action_function_b': Step.WAIT,
            'action_function_c': Step.WAIT,
            'action_function_d': Step.WAIT,
            'action_function_e': Step.READY,
            'action_function_f': Step.READY,
            'action_function_g': Step.READY,
        })

    def test_pause_on_fail(self):
        # First shutdown the default setup trail.
        self.trail_client.shutdown(dry_run=False)

        def failing_action_function(trail_env, context):
            raise Exception('mock failure')

        step_a = Step(failing_action_function)
        step_b = Step(action_function_b)

        # Trail definition: The following example trail represents the following DAG:
        # step_a ----> step_b
        test_trail_definition = [(step_a, step_b)]

        context = Context(42)
        self.trail_server = TrailServer(test_trail_definition, delay=0.1, context=context)
        self.trail_server.serve(threaded=True)
        self.trail_client = TrailClient(self.trail_server.socket_file)

        self.trail_client.start()

        self.wait_till('failing_action_function', Step.PAUSED_ON_FAIL)

        self.check_step_attributes(StatusField.STATE, {
            'failing_action_function': Step.PAUSED_ON_FAIL,
            'action_function_b': Step.WAIT,
        })

        self.check_step_attributes(StatusField.RETURN_VALUE, {
            'failing_action_function': 'mock failure',
            'action_function_b': None,
        })

    def test_message_passing(self):
        # First shutdown the default setup trail.
        self.trail_client.shutdown(dry_run=False)

        def io_action_function(trail_env, context):
            reply = trail_env.input('Prompting user')
            trail_env.output('Reply received: {}'.format(reply))

        def slow_action_function(trail_env, context):
            sleep(0.2)

        step_a = Step(io_action_function)
        step_b = Step(slow_action_function)

        # Trail definition: The following example trail represents the following DAG:
        # step_a ----> step_b
        #
        test_trail_definition = [(step_a, step_b)]

        context = Context(42)
        self.trail_server = TrailServer(test_trail_definition, delay=0.1, context=context)
        self.trail_server.serve(threaded=True)
        self.trail_client = TrailClient(self.trail_server.socket_file)

        self.trail_client.start()

        self.wait_till('io_action_function', Step.RUN)

        statuses = self.trail_client.status(name='io_action_function')

        self.assertEqual(len(statuses), 1)

        prompt_message = statuses[0][StatusField.UNREPLIED_PROMPT_MESSAGE]
        self.assertEqual(prompt_message, 'Prompting user')

        # Reply to steps
        self.trail_client.send_message_to_steps(name='io_action_function', message='Done', dry_run=False)

        self.wait_till('io_action_function', Step.SUCCESS)

        # Check reply
        statuses = self.trail_client.status(name='io_action_function', fields=[StatusField.OUTPUT_MESSAGES])
        self.assertEqual(len(statuses), 1)
        output_messages = statuses[0][StatusField.OUTPUT_MESSAGES]
        self.assertEqual(output_messages, ['Reply received: Done'])

    def test_skip(self):
        affected_steps = self.trail_client.skip(foo='bar', dry_run=False)
        self.check_affected_steps(affected_steps, [
            'action_function_b',
            'action_function_f',
        ])

        self.check_step_attributes(StatusField.STATE, {
            'action_function_a': Step.READY,
            'action_function_b': Step.TOSKIP,
            'action_function_c': Step.READY,
            'action_function_d': Step.READY,
            'action_function_e': Step.READY,
            'action_function_f': Step.TOSKIP,
            'action_function_g': Step.READY,
        })

        self.trail_client.start()

        self.wait_till('action_function_g', Step.SUCCESS)

        self.check_step_attributes(StatusField.STATE, {
            'action_function_a': Step.SUCCESS,
            'action_function_b': Step.SKIPPED,
            'action_function_c': Step.SUCCESS,
            'action_function_d': Step.SUCCESS,
            'action_function_e': Step.SUCCESS,
            'action_function_f': Step.SKIPPED,
            'action_function_g': Step.SUCCESS,
        })

    def test_unskip(self):
        affected_steps = self.trail_client.skip(foo='bar', dry_run=False)
        self.check_affected_steps(affected_steps, [
            'action_function_b',
            'action_function_f',
        ])

        self.check_step_attributes(StatusField.STATE, {
            'action_function_a': Step.READY,
            'action_function_b': Step.TOSKIP,
            'action_function_c': Step.READY,
            'action_function_d': Step.READY,
            'action_function_e': Step.READY,
            'action_function_f': Step.TOSKIP,
            'action_function_g': Step.READY,
        })

        affected_steps = self.trail_client.unskip()
        self.check_affected_steps(affected_steps, [
            'action_function_b',
            'action_function_f',
        ])

        affected_steps = self.trail_client.unskip(foo='bar', dry_run=False)
        self.check_affected_steps(affected_steps, [
            'action_function_b',
            'action_function_f',
        ])

        self.check_step_attributes(StatusField.STATE, {
            'action_function_a': Step.READY,
            'action_function_b': Step.WAIT,
            'action_function_c': Step.READY,
            'action_function_d': Step.READY,
            'action_function_e': Step.READY,
            'action_function_f': Step.WAIT,
            'action_function_g': Step.READY,
        })

    def test_block(self):
        affected_steps = self.trail_client.block(foo='bar', dry_run=False)
        self.check_affected_steps(affected_steps, [
            'action_function_b',
            'action_function_f',
        ])

        self.check_step_attributes(StatusField.STATE, {
            'action_function_a': Step.READY,
            'action_function_b': Step.TOBLOCK,
            'action_function_c': Step.READY,
            'action_function_d': Step.READY,
            'action_function_e': Step.READY,
            'action_function_f': Step.TOBLOCK,
            'action_function_g': Step.READY,
        })

        self.trail_client.start()

        self.wait_till('action_function_b', Step.BLOCKED)
        self.wait_till('action_function_f', Step.BLOCKED)

        self.check_step_attributes(StatusField.STATE, {
            'action_function_a': Step.SUCCESS,
            'action_function_b': Step.BLOCKED,
            'action_function_c': Step.WAIT,
            'action_function_d': Step.WAIT,
            'action_function_e': Step.SUCCESS,
            'action_function_f': Step.BLOCKED,
            'action_function_g': Step.WAIT,
        })

    def test_unblock(self):
        affected_steps = self.trail_client.block(foo='bar', dry_run=False)

        affected_steps = self.trail_client.unblock(foo='bar')
        self.check_affected_steps(affected_steps, [
            'action_function_b',
            'action_function_f',
        ])

        affected_steps = self.trail_client.unblock(dry_run=False)
        self.check_affected_steps(affected_steps, [
            'action_function_b',
            'action_function_f',
        ])

        self.check_step_attributes(StatusField.STATE, {
            'action_function_a': Step.READY,
            'action_function_b': Step.WAIT,
            'action_function_c': Step.READY,
            'action_function_d': Step.READY,
            'action_function_e': Step.READY,
            'action_function_f': Step.WAIT,
            'action_function_g': Step.READY,
        })

    def test_interrupt(self):
        # First shutdown the default setup trail.
        self.trail_client.shutdown(dry_run=False)

        # We need a long running step to be able to invoke interrupt on it.
        def long_running_action_function(trail_env, context):
            sleep(3)
            return "This is action A. Value from context: {}".format(context.value)

        step_a = Step(long_running_action_function)
        step_b = Step(action_function_b)

        # Define a trail with slightly long running steps.
        # The following example trail represents the following DAG:
        # step_a ----> step_b
        test_trail_definition = [(step_a, step_b)]

        context = Context(42)
        self.trail_server = TrailServer(test_trail_definition, delay=0.1, context=context)
        self.trail_server.serve(threaded=True)
        self.trail_client = TrailClient(self.trail_server.socket_file)

        self.trail_client.start()

        self.wait_till('long_running_action_function', Step.RUN)

        # long_running_action_function is a long running step (5 seconds), so terminate it.
        affected_steps = self.trail_client.interrupt(dry_run=False)
        self.check_affected_steps(affected_steps, ['long_running_action_function'])

        self.check_step_attributes(StatusField.STATE, {
            'long_running_action_function': Step.INTERRUPTED,
            'action_function_b': Step.WAIT,
        })

        # Make sure interrupted is a state that can be resumed from (re-run).
        affected_steps = self.trail_client.resume()
        self.check_affected_steps(affected_steps, ['long_running_action_function'])

    def test_stop(self):
        # First shutdown the default setup trail.
        self.trail_client.shutdown(dry_run=False)

        # We need a long running step to be able to invoke interrupt on it.
        def long_running_action_function(trail_env, context):
            sleep(3)
            return "This is action A. Value from context: {}".format(context.value)

        step_a = Step(long_running_action_function)
        step_b = Step(action_function_b)

        # Define a trail with slightly long running steps.
        # The following example trail represents the following DAG:
        # step_a ----> step_b
        test_trail_definition = [(step_a, step_b)]

        context = Context(42)
        self.trail_server = TrailServer(test_trail_definition, delay=0.1, context=context)
        self.trail_server.serve(threaded=True)
        self.trail_client = TrailClient(self.trail_server.socket_file)

        self.trail_client.start()

        self.wait_till('long_running_action_function', Step.RUN)

        # long_running_action_function is a long running step (5 seconds), so stop the trail with interrupt.
        affected_steps = self.trail_client.stop(dry_run=False)

        self.wait_till('long_running_action_function', Step.BLOCKED)
        self.check_affected_steps(affected_steps, [
            'long_running_action_function',
        ])

        self.check_step_attributes(StatusField.STATE, {
            'long_running_action_function': Step.BLOCKED,
            'action_function_b': Step.TOBLOCK,
        })

    def test_next_step(self):
        self.trail_client.pause(dry_run=False)

        self.trail_client.start()

        affected_steps = self.trail_client.next_step()

        self.check_affected_steps(affected_steps, ['action_function_a'])

        affected_steps = self.trail_client.next_step(step_count=3, dry_run=False)

        self.check_affected_steps(affected_steps, [
            'action_function_a',
            'action_function_b',
            'action_function_e',
        ])

        self.wait_till('action_function_b', Step.SUCCESS)
        self.wait_till('action_function_e', Step.SUCCESS)
        self.wait_till('action_function_c', Step.PAUSED)
        self.wait_till('action_function_f', Step.PAUSED)

        self.check_step_attributes(StatusField.STATE, {
            'action_function_a': Step.SUCCESS,
            'action_function_b': Step.SUCCESS,
            'action_function_c': Step.PAUSED,
            'action_function_d': Step.TOPAUSE,
            'action_function_e': Step.SUCCESS,
            'action_function_f': Step.PAUSED,
            'action_function_g': Step.TOPAUSE,
        })
