"""Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.
"""

from __future__ import print_function

import code
import sys

from rlcompleter import readline


def interactive(namespace_dict, trail=None, prompt='AutoTrail>'):
    """This function, will start an interactive python prompt.
    This is a helper function provided to allow the use of AutoTrail in an interactive Python shell.

    It has the following features:
    1) All names defined before interactive is called, are available to the user.
       If a limited set is needed, then pass the appropriate dictionary as globals.
       This is useful because if the code defined a Trail object lets say "example_trail", then example_trail will be
       available to the user to then run commands like:
       example_trail.start()
       example_trail.pause()
    2) Command line completion:
       example_trail.pa<tab><tab> will produce:
       example_trail.pause(
    3) Exit handling -- When the user ends the interactive,
    4) If a trail object is provided, it will be stopped before exiting.
    """
    readline.parse_and_bind('tab: complete')
    sys.ps1 = '{} '.format(prompt)
    vars = namespace_dict.copy()
    vars.update(locals())
    shell = code.InteractiveConsole(vars)
    shell.interact('Welcome to AutoTrail')
