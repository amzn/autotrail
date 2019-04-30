"""Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.
"""

import logging
import os

from time import sleep

from autotrail import (TrailServer, TrailClient, InteractiveTrail, interactive, Step, StatusField, Instruction,
                       make_simple_templating_function)


logging.basicConfig(level=logging.DEBUG, filename=os.path.join('/', 'tmp', 'example_trail_server.log'))


# Action function and Step definitions
step_a = Step(Instruction('step_a', make_simple_templating_function('Check who are you using: /bin/whoami')))

step_b = Step(Instruction('step_b', make_simple_templating_function('Check the current directory with: /bin/pwd')))

step_c = Step(Instruction('step_c', make_simple_templating_function('List files using: ls -l')))

step_d = Step(Instruction('step_d', make_simple_templating_function('Find disk usage with: du -sh .')))

step_e = Step(Instruction('step_e', make_simple_templating_function('List the files changed last with: ls -ltr')))


# Trail definition
#                      |--> step_c --> step_e
# step_a --> step_b -->|
#                      |--> step_d
#
#
runbook_trail_definition = [
    (step_a, step_b),
    (step_b, step_c),
    (step_b, step_d),
    (step_c, step_e),
]


runbook_trail_server = TrailServer(runbook_trail_definition, delay=0.1)
runbook_trail_server.serve(threaded=True)
runbook_trail = TrailClient(runbook_trail_server.socket_file)
runbook_interactive = InteractiveTrail(runbook_trail)
runbook_trail.start()

# Convenience functions to save typing
def runbook(n=None):
    """This is the only function the user needs to use for the normal run of
    the runbook.

    Usage:
    runbook()  -- Start running the runbook trail if it isn't running.
    runbook()  -- Get next instruction. This will show all the instructions
                  that are:
                  1. In-progress, i.e., not marked done.
                  2. New instructions receieved with this invocation.
    runbook(4) -- Mark an instruction #4 complete.

    Keyword Arguments:
    n          -- Int - This is the instruction number that is printed on the
                  left-most part of the instruction.

    """
    if n is not None:
        affected_steps = runbook_trail.send_message_to_steps(n=n, message=True, dry_run=False)

        if len(affected_steps) != 1:
            print('Could not mark the step as completed.')
        else:
            print('Step {} was marked completed.'.format(n))

    runbook_interactive.status(
        fields=[StatusField.UNREPLIED_PROMPT_MESSAGE],
        states=[Step.RUN]
    )

# Start interactive session
interactive(globals(), trail=runbook_trail, prompt='Runbook Trail>')
runbook_interactive.stop(dry_run=False)
runbook_interactive.shutdown(dry_run=False)
