"""Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.

Example of an interactive trail run.

This example shows how AutoTrail should be used by running an example trail in an interactive session.
This causes AutoTrail to run with a Python interactive session, allowing the user to manage the run of a trail by
issuing API calls from the interactive prompt.

For an automatic (non-interactive) session, see run_example_automatic.py

This consists of:
1) Defining the steps and tags.
2) Defining the trail.
3) Setting up the trail such that it runs interactively.
4) Starting an interacive shell.
"""

import logging
import os

from time import sleep

from autotrail import TrailServer, TrailClient, Step, interactive, StatusField, accepts_context, InteractiveTrail


logging.basicConfig(level=logging.DEBUG, filename=os.path.join('/', 'tmp', 'example_trail_server.log'))


# Context definitions
class Context(object):
    def __init__(self, seed):
        self.seed = seed
        self.values = []


def pre_processor(_, context):
    return ((context.seed,), {})


def post_processor(_, context, return_value):
    old_values = list(context.values)
    context.values.append(return_value)
    return 'Values before action: {}. Value after action: {}'.format(old_values, context.values)


# Step definitions
def action_function_a(value):
    sleep(5)
    return value+1


def action_function_b(value):
    sleep(10)
    return value+2


def action_function_c(value):
    sleep(1)
    return value+3


def action_function_d(value):
    sleep(10)
    return value+4


def action_function_e(value):
    sleep(5)
    return value+5


def action_function_f(value):
    sleep(1)
    return value+6


def action_function_g(value):
    sleep(10)
    return value+7


step_a = Step(action_function_a, pre_processor=pre_processor, post_processor=post_processor)
step_b = Step(action_function_b, pre_processor=pre_processor, post_processor=post_processor)
step_c = Step(action_function_c, pre_processor=pre_processor, post_processor=post_processor, foo='bar')
step_d = Step(action_function_d, pre_processor=pre_processor, post_processor=post_processor)
step_e = Step(action_function_e, pre_processor=pre_processor, post_processor=post_processor)
step_f = Step(action_function_f, pre_processor=pre_processor, post_processor=post_processor, foo='bar')
step_g = Step(action_function_g, pre_processor=pre_processor, post_processor=post_processor)


# Trail definition
# The following example trail represents the following DAG:
# The run-time (in seconds) of each step is mentioned below the step name.
#         +----> step_b ----> step_c ----> step_d ----+
#         |        10           1            10       |
# step_a -|                                           +----> step_g
#    5    |         5                         1       |        10
#         +----> step_e -----------------> step_f ----+
#
example_trail_definition = [
    (step_a, step_b),
    (step_a, step_e),

    # First branch
    (step_b, step_c),
    (step_c, step_d),
    (step_d, step_g),

    # Second branch
    (step_e, step_f),
    (step_f, step_g),
]


# Setup trail to run automatically (non-interactively)
context = Context(10)
example_trail_server = TrailServer(example_trail_definition, delay=0.5, context=context)

# Run the server in a separate thread so that we can interact with it right here.
example_trail_server.serve(threaded=True)


example_trail = TrailClient(example_trail_server.socket_file)
example = InteractiveTrail(example_trail)


# Start interactive session
interactive(globals(), prompt='Example Trail>')
example.stop(dry_run=False)
example.shutdown(dry_run=False)
