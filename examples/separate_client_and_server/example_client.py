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

import os

from autotrail import TrailClient, Step, interactive, StatusField, InteractiveTrail


socket_file = os.path.join('/', 'tmp', 'autotrail.example.server')
example_trail = TrailClient(socket_file)
example = InteractiveTrail(example_trail)


# Start interactive session
interactive(globals(), prompt='Example Trail>')
