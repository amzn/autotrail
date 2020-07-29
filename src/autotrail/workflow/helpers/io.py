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

"""
import code
import sys

from rlcompleter import readline


def interactive(namespace_dict, prompt='AutoTrail> '):
    """Run an Python REPL with the given namespace objects available.

    :param namespace_dict:  A mapping of names and objects that will be available in the REPL.
    :param prompt:          The prompt displayed by the REPL.
    :return:                None
    """
    readline.parse_and_bind('tab: complete')
    sys.ps1 = prompt
    shell = code.InteractiveConsole(namespace_dict)
    shell.interact('Welcome to AutoTrail')
