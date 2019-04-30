"""Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.

I/O helpers

This module defines some helpers for user I/O and trails.
"""


from __future__ import print_function

import re
import sys

from six import iteritems

from autotrail.core.dag import topological_traverse
from autotrail.layer1.api import StatusField


def write_trail_definition_as_dot(trail_definition, to):
    """Convert a given trail definition (ordered pair representation) into the DOT file format.

    This function does not check for acyclicity, hence it cannot be called a DAG at this point.
    This function is useful in visually looking at the trail and finding cycles visually.

    Arguments:
    trail_definition -- The ordered pair representation of a trail.
                        This is basically a list of tuples, each tuple representing an ordered pair, which, in-turn
                        represents an edge.
    to               -- A file-like object to write to. Must support the write method.
    """
    # A trail definition using ordered pair approach will consist of a list of ordered pairs of the form:
    # [
    #     (parent_step, child_step),
    #     ...
    # ]
    # A DOT file is of the format:
    #  digraph graphname {
    #      source_vertex -> destination_vertex;
    #  }
    print('digraph trail {', file=to)
    print('node [style=rounded, shape=box];', file=to)

    template = '"{0}" -> "{1}";'
    for parent_step, child_step in trail_definition:
        print(template.format(str(parent_step), str(child_step)), file=to)
    print('}', file=to)


def print_step_statuses(step_statuses, to):
    """Print the step statuses in the following format:
    - Name         : <step name>
      n            : <'n' tag>
      <Field>      : <single line value>
      <List Field> : <first value>
                   : <second value>
    <blank line>

    The whitespace around ':' is computed based on the field names defined in StatusField.

    Arguments:
    step_statuses -- A list of dictionaries containing keys defined in the StatusField namespace.
    to            -- The stream to write to.
    """
    all_fields = (StatusField.N, StatusField.NAME, StatusField.RETURN_VALUE, StatusField.OUTPUT_MESSAGES,
                  StatusField.PROMPT_MESSAGES, StatusField.UNREPLIED_PROMPT_MESSAGE)
    max_field_length = len(max(all_fields, key=len))
    header = '- {{field:<{}}}: {{value}}'.format(max_field_length)
    body = '  {{field:<{}}}: {{value}}'.format(max_field_length)
    print('Status of steps:', file=to)
    for step_status in step_statuses:
        print(header.format(field=StatusField.NAME, value=step_status[StatusField.NAME]), file=to)
        print(body.format(field=StatusField.N, value=step_status[StatusField.N]), file=to)
        for field in step_status:
            if field in (StatusField.STATE, StatusField.RETURN_VALUE, StatusField.UNREPLIED_PROMPT_MESSAGE):
                print(body.format(field=field, value=step_status[field]), file=to)
            elif field in (StatusField.OUTPUT_MESSAGES, StatusField.PROMPT_MESSAGES):
                for i, message in enumerate(step_status[field]):
                    field = field if i == 0 else ''
                    print(body.format(field=field, value=str(message)), file=to)
        print('', file=to)


def print_error(method_name, error_message, to):
    """Print error received for an API call.

    Prints in the following format:
    - API Call: <API call name>
      Error: <error_message>

    Arguments:
    method_name -- The API method called.
    to          -- The stream to write to.
    """
    print('- API Call: {}'.format(method_name), file=to)
    print('  Error: {}'.format(error_message), file=to)


def print_api_result(method_name, api_result, to):
    """Print the API result as-is.

    Prints in the following format:
    - API Call: <The API method called>
      Result: <api_result>

    Arguments:
    method_name -- The API method called.
    api_result  -- The API result received (will be printed as-is).
    to          -- The stream to write to.
    """
    print('- API Call: {}'.format(method_name), file=to)
    print('  Result: {}'.format(api_result), file=to)


def print_no_result(method_name, to):
    """Print when an API call did not fetch a result.

    Prints in the following format:
    - API Call: <The API method called>
      No result received for the API call.

    Arguments:
    method_name -- The API method called.
    to          -- The stream to write to.
    """
    print('- API Call: {}'.format(method_name), file=to)
    print('  No result received for the API call.', file=to)


def print_affected_steps(method_name, affected_steps, dry_run, to):
    """Print the steps affected by an API call.

    Prints in the following format:
    - API Call: <The API method called>
      Mode: Dry Run <or 'Actual Run'>
      List of step names that are affected:
        # <'n' tag> - <'name' tag of step>
        ...

    Arguments:
    method_name    -- The API method called.
    affected_steps -- A list of tags (dictionaries) containing the 'n' and 'name' keys.
    to             -- The stream to write to.
    """
    print('- API Call: {}'.format(method_name), file=to)
    print('  Mode: {}'.format('Dry Run' if dry_run else 'Actual Run'), file=to)
    print('  List of step names that are affected:', file=to)
    for step_tags in affected_steps:
        print('    # {} - {}'.format(step_tags['n'], step_tags['name']), file=to)


def print_step_list(steps_tags, to):
    """Print the list of steps and their associated tags in a user friendly way to the given stream.

    Prints in the following format:
    - Name: <'name' tag>
      n: <'n' tag>
      other_key: <'other_key' tag>

    Arguments:
    steps_tags -- A list of dicts. Each dictionary should atleast contain the 'n' and 'name' keys.
    to         -- A file-like object to write to. Defaults to STDOUT.

    Printing format examples:
    """
    print('List of steps and their tags:', file=to)
    for step_tags in steps_tags:
        print('- Name: {}'.format(step_tags['name']), file=to)
        print('  n: {}'.format(step_tags['n']), file=to)
        for key, value in iteritems(step_tags):
            if key in ['name', 'n']:
                continue
            print('  {key}: {value}'.format(key=key, value=value), file=to)


class InteractiveTrail(object):
    """Class that handles a Trail object and adds interactivity.

    The behaviour is exactly like the Trail class but the methods always print to STDOUT or STDERR letting the user
    know about the actions taken.

    This class supports all the API methods of the Trail class. All it does is makes the API call using the given Trail
    object and parses the API result obtained and prints it to STDOUT.

    It also informs the user of any errors, eg., when the user tries to make an API call when the trail is not running.
    """
    def __init__(self, trail_client, stdout=sys.stdout, stderr=sys.stderr, status_printer=print_step_statuses,
                 affected_steps_printer=print_affected_steps, step_list_printer=print_step_list,
                 error_printer=print_error, no_result_printer=print_no_result, generic_printer=print_api_result):
        """Initialize an interactive wrapper for TrailClient.

        Arguments:
        trail_client           -- A TrailClient object or similar.

        Keyword Arguments:
        stdout                 -- The stream to write STDOUT messages to. Defaults to sys.stdout.
        stderr                 -- The stream to write STDERR messages to. Defaults to sys.stderr.
        status_printer         -- The function used to print the status API result.
                                  Should accept:
                                  1. The result of the API call
                                  2. The stream to write to.
        affected_steps_printer -- The function used to print affected steps returned by API calls.
                                  Should accept:
                                  1. The name of the API call.
                                  2. The result of the API call, a list of tags containing only the 'n' and 'name' tags.
                                  3. If this run was a dry_run or not.
                                  4. The stream to write to.
        step_list_printer      -- The function used to print a list of step tags.
                                  Should accept:
                                  1. The step tags (a list of dictionaries, containing all the tags of a step).
                                  2. The stream to write to.
        error_printer          -- The function used to print the error message received as a response to an API call.
                                  Should accept:
                                  1. The method name that resulted in an error.
                                  2. The error message recieved from the trail.
                                  3. The stream to write to.
        no_result_printer      -- The function used to print when no result was received by an API call.
                                  Should accept:
                                  1. The method name that was invoked.
                                  2. The stream to write to.
        generic_printer        -- The function used to print an API result that doesn't fall into the above categories.
                                  Should accept:
                                  1. The method that was invoked.
                                  2. The API result (dictionary).
                                  3. The stream to write to.
        """
        self.trail_client = trail_client
        self.stdout = stdout
        self.stderr = stderr
        self.error_printer = error_printer
        self.no_result_printer = no_result_printer
        self.status_printer = status_printer
        self.affected_steps_printer = affected_steps_printer
        self.step_list_printer = step_list_printer
        self.generic_printer = generic_printer

    def _call_trail_client_method(self, method, printer, *args, **kwargs):
        """Calls the given method of the Trail object with the provided args and kwargs and then calls the given printer
        function with the result.

        If the method call raises and exception, then it is printed using the error_printer.
        If no result was received, then it is printed using the no_result_printer.

        The args and kwargs vary based on the method of TrailClient being called.

        Arguments:
        method    -- String representing the method of the TrailClient class to be called.
        printer   -- The printer function to be called to print the API result. This function should accept only one
                     argument, viz., the result of the API call.
        *args     -- Any arguments to be passed to the TrailClient method.

        Keyword Arguments:
        **kwargs  -- Any keyword arguments to be passed to the TrailClient method.
        """
        try:
            result = getattr(self.trail_client, method)(*args, **kwargs)
        except Exception as error:
            self.error_printer(method, str(error), self.stderr)
            return

        if result:
            printer(result)
        else:
            self.no_result_printer(method, self.stderr)

    def start(self):
        """Send message to start a trail. This will change the state of all steps in 'Ready' state to 'Waiting' state.

        Prints:
        To STDOUT -- The 'name' and 'n' tags of steps that were affected.
        """
        printer = lambda result: self.affected_steps_printer('start', result, False, self.stdout)
        self._call_trail_client_method('start', printer)

    def stop(self, dry_run=True):
        """Send mesage to stop the run of a trail by blocking all steps. Running steps will be interrupted before being
        blocked.
        This is achieved by calling 'interrupt' followed by 'block' on all the steps.

        Keyword Arguments:
        dry_run=True -- Results in a list of steps that will be interrupted.

        Prints:
        To STDOUT  -- The 'name' and 'n' tags of steps that were interrupted.
        """
        printer = lambda result: self.affected_steps_printer('stop', result, False, self.stdout)
        self._call_trail_client_method('stop', printer, dry_run=dry_run)

    def shutdown(self, dry_run=True):
        """Send message to shutdown the trail server. After this call, no other API calls can be served.

        Prints:
        To STDOUT  -- True if the trail server was shutdown, False otherwise.
        """
        printer = lambda result: self.generic_printer('shutdown', result, self.stdout)
        self._call_trail_client_method('shutdown', printer, dry_run=dry_run)

    def send_message_to_steps(self, message, dry_run=True, **tags):
        """Send a message to matching steps.

        Keyword arguments:
        message      -- Any JSON encodable Python object.
        dry_run=True -- Results in a list of steps that the message will be sent to.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Prints:
        To STDOUT    -- The 'name' and 'n' tags of steps that were affected.
        """
        printer = lambda result: self.affected_steps_printer('send_message_to_steps', result, dry_run, self.stdout)
        self._call_trail_client_method('send_message_to_steps', printer, message, dry_run=dry_run, **tags)

    def list(self, **tags):
        """List all steps in a trail (in topological order).

        If no keyword arguments are provided, this will list all the steps in the queue.

        Keyword arguments:
        **tags       -- Any key=value pair provided in the arguments is treated as a tag.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Prints:
        To STDOUT    -- A list of Step names and all of their associated tags.
        """
        printer = lambda result: self.step_list_printer(result, self.stdout)
        self._call_trail_client_method('list', printer, **tags)

    def status(self, fields=None, states=None, **tags):
        """Send message to get trail status.

        Keyword Arguments:
        fields    -- List of field names to return in the results. A list of strings from StatusField class.
                     This will limit the status to the fields requested.
                     Defaults to:
                     [
                        StatusField.STATE,
                        StatusField.UNREPLIED_PROMPT_MESSAGE,
                        StatusField.OUTPUT_MESSAGES,
                        StatusField.RETURN_VALUE,
                     ]
                     If the trail has finished, the the fields used are:
                     [
                        StatusField.STATE,
                        StatusField.RETURN_VALUE,
                     ]

                     No matter what fields are passed, the following 2 fields will always be present in the result:
                        StatusField.N: <Sequence number of the step>
                        StatusField.NAME: <Name of the step>
                     This is because without these fields, it will be impossible to uniquely identify the steps.
        states    -- List of strings that represent the states of a Step.
                     This will limit the status to only the steps that are in the given list of states.
        tags      -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                     Each step by default gets a tag viz., name=<action_function_name>.

        Prints:
        To STDOUT -- The status of each step that matches the given tags and states.
                     It will contain the fields specified in the fields keyword argument along with the required fields
                     explained above.
        """
        printer = lambda result: self.status_printer(result, self.stdout)
        self._call_trail_client_method('status', printer, fields=fields, states=states, **tags)

    def steps_waiting_for_user_input(self, **tags):
        """Get status of steps that are waiting for user input.
        Limits the fields to only include the UNREPLIED_PROMPT_MESSAGE.
        Excludes steps that do not have any UNREPLIED_PROMPT_MESSAGE.

        Keyword Arguments:
        tags      -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                     Each step by default gets a tag viz., name=<action_function_name>.

        Prints:
        To STDOUT -- The status of only the steps that are waiting for user input containing the following 3 fields:
                     1. The 'name' and 'n' tags of steps that are waiting for user input.
                     2. Their unreplied prompt message.
        """
        printer = lambda result: self.status_printer(result, self.stdout)
        self._call_trail_client_method('steps_waiting_for_user_input', printer, **tags)

    def pause(self, dry_run=True, **tags):
        """Send message to pause steps.
        Mark the specified steps so that they are paused.
        A step can only be paused if it is ready or waiting.

        If tags are provided, then only the steps matching the tags will be paused.
        If no tags are provided, all possible steps will be marked to be paused.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be paused.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Prints:
        To STDOUT    -- The 'name' and 'n' tags of steps that were affected.
        """
        printer = lambda result: self.affected_steps_printer('pause', result, dry_run, self.stdout)
        self._call_trail_client_method('pause', printer, dry_run=dry_run, **tags)

    def pause_branch(self, dry_run=True, **tags):
        """Send message to pause all steps in matching branches.
        Look for all the steps that are present in the branches originating from each of the matching steps and mark
        them so that they are paused.
        A step can only be paused if it is ready or waiting.

        If tags are provided, then only the steps matching the tags will be paused.
        If no tags are provided, all possible steps will be marked to be paused.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be paused.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Prints:
        To STDOUT    -- The 'name' and 'n' tags of steps that were affected.
        """
        printer = lambda result: self.affected_steps_printer('pause_branch', result, dry_run, self.stdout)
        self._call_trail_client_method('pause_branch', printer, dry_run=dry_run, **tags)

    def interrupt(self, dry_run=True, **tags):
        """Send message to interrupt running steps.
        This will interrupt the running steps by sending them SIGINT. A step can be interrupted only if it is running.

        If tags are provided, then only the steps matching the tags will be interrupted.
        If no tags are provided, all possible steps will be marked to be interrupted.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be interrupted.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Prints:
        To STDOUT    -- The 'name' and 'n' tags of steps that were affected.
        """
        printer = lambda result: self.affected_steps_printer('interrupt', result, dry_run, self.stdout)
        self._call_trail_client_method('interrupt', printer, dry_run=dry_run, **tags)

    def resume(self, dry_run=True, **tags):
        """Send message to resume steps.
        Mark the specified steps so that they are resumed.
        A step can only be resumed if it is either paused, marked to pause, paused due to failure or interrupted.

        If tags are provided, then only the steps matching the tags will be resumed.
        If no tags are provided, all possible steps will be marked to be resumed.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be resumed.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Prints:
        To STDOUT    -- The 'name' and 'n' tags of steps that were affected.
        """
        printer = lambda result: self.affected_steps_printer('resume', result, dry_run, self.stdout)
        self._call_trail_client_method('resume', printer, dry_run=dry_run, **tags)

    def next_step(self, step_count=1, dry_run=True):
        """Send message to resume the next step.

        When multiple steps have been paused, or if an branch has been paused, they can be resumed one step at a time
        in the correct order using this method.

        Keyword arguments:
        step_count -- A number greater than 0. This is the number of steps to run before pausing again.
                      By default this will resume only one step.

        Prints:
        To STDOUT  -- The 'name' and 'n' tags of steps that were affected.
        """
        printer = lambda result: self.affected_steps_printer('next_step', result, dry_run, self.stdout)
        self._call_trail_client_method('next_step', printer, step_count=step_count, dry_run=dry_run)

    def resume_branch(self, dry_run=True, **tags):
        """Send message to resume all steps in matching branches.
        Look for all the steps that are present in the branches originating from each of the matching steps and mark
        them so that they are resumed.
        A step can only be resumed if it is either paused, marked to pause, paused due to failure or interrupted.

        If tags are provided, then only the steps matching the tags will be resumed.
        If no tags are provided, all possible steps will be marked to be resumed.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be resumed.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Prints:
        To STDOUT    -- The 'name' and 'n' tags of steps that were affected.
        """
        printer = lambda result: self.affected_steps_printer('resume_branch', result, dry_run, self.stdout)
        self._call_trail_client_method('resume_branch', printer, dry_run=dry_run, **tags)

    def skip(self, dry_run=True, **tags):
        """Send message to mark steps to be skipped.
        Mark the specified steps so that they are skipped when execution reaches them.
        A step can only be skipped if it is either paused, marked to pause, paused due to failure, waiting or ready.

        If tags are provided, then only the steps matching the tags will be skipped.
        If no tags are provided, all possible steps will be marked to be skipped.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be skipped.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Prints:
        To STDOUT    -- The 'name' and 'n' tags of steps that were affected.
        """
        printer = lambda result: self.affected_steps_printer('skip', result, dry_run, self.stdout)
        self._call_trail_client_method('skip', printer, dry_run=dry_run, **tags)

    def unskip(self, dry_run=True, **tags):
        """Send message to un-skip steps that have been marked to be skipped.
        A step can be un-skipped only if it is marked to be skipped but has not been skipped yet.
        Once a step has already been skipped, this API call will not affect them (they cannot be unskipped).

        If tags are provided, then only the steps matching the tags will be un-skipped.
        If no tags are provided, all possible steps will be marked to be skipped.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be unskipped.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Prints:
        To STDOUT    -- The 'name' and 'n' tags of steps that were affected.
        """
        printer = lambda result: self.affected_steps_printer('unskip', result, dry_run, self.stdout)
        self._call_trail_client_method('unskip', printer, dry_run=dry_run, **tags)

    def block(self, dry_run=True, **tags):
        """Send message to block steps.
        Mark the specified steps so that they are blocked.
        A step can only be blocked if it is either paused, marked to pause, paused due to failure, waiting or ready.

        If tags are provided, then only the steps matching the tags will be blocked.
        If no tags are provided, all possible steps will be marked to be skipped.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be blocked.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Prints:
        To STDOUT    -- The 'name' and 'n' tags of steps that were affected.
        """
        printer = lambda result: self.affected_steps_printer('block', result, dry_run, self.stdout)
        self._call_trail_client_method('block', printer, dry_run=dry_run, **tags)

    def unblock(self, dry_run=True, **tags):
        """Send message to un-block steps that have been marked to be blocked.
        A step can be un-blocked only if it is marked to be blocked but has not been blocked yet.
        Once a step has already been blocked, this API call will not affect them (they cannot be unblocked).

        If tags are provided, then only the steps matching the tags will be unblocked.
        If no tags are provided, all possible steps will be marked to be skipped.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be unblocked.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Prints:
        To STDOUT    -- The 'name' and 'n' tags of steps that were affected.
        """
        printer = lambda result: self.affected_steps_printer('unblock', result, dry_run, self.stdout)
        self._call_trail_client_method('unblock', printer, dry_run=dry_run, **tags)

    def set_pause_on_fail(self, dry_run=True, **tags):
        """Send message to set the pause_on_fail flag of a step.
        By default, all steps are marked with pause_on_fail so that they are paused if they fail. If a step's
        pause_on_fail attribute was explicitly set to False, it can be set back to True using this method.
        This API call sets this flag so that the Step will be paused if it fails.

        If tags are provided, then only the steps matching the tags will be paused.
        If no tags are provided, all possible steps will be marked to be paused.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be paused.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Prints:
        To STDOUT    -- The 'name' and 'n' tags of steps that were affected.
        """
        printer = lambda result: self.affected_steps_printer('set_pause_on_fail', result, dry_run, self.stdout)
        self._call_trail_client_method('set_pause_on_fail', printer, dry_run=dry_run, **tags)

    def unset_pause_on_fail(self, dry_run=True, **tags):
        """Send message to clear the pause_on_fail flag of a step.
        By default, all steps are marked with pause_on_fail so that they are paused if they fail.
        This allows the user to either re-run the step, skip it or block it.
        This API call unsets this flag. This will cause the Step to fully fail (irrecoverably) if the action function
        fails (raises an exception).

        If tags are provided, then only the steps matching the tags will be paused.
        If no tags are provided, all possible steps will be marked to be paused.

        Keyword arguments:
        dry_run=True -- Results in a list of steps that will be paused.
        **tags       -- Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.

        Prints:
        To STDOUT    -- The 'name' and 'n' tags of steps that were affected.
        """
        printer = lambda result: self.affected_steps_printer('unset_pause_on_fail', result, dry_run, self.stdout)
        self._call_trail_client_method('unset_pause_on_fail', printer, dry_run=dry_run, **tags)


class Names(object):
    """An object consisting of names as object attributes for easy auto-completion.

    Often during the run of a trail, a user may need to repeatedly type in various trail-specific names like:
    1. Name of a step.
    2. Tag keys.
    3. Tag values.

    The interactive session will provide auto-completion for the various API call methods, but not for the
    trail-specific names. Neither can we crowd the global namespace with them.

    Names provides an object whose attributes will contain the names, referring to the values.
    This places all the names in the namespace of the Names object, providing auto-completion.

    This class auto-converts special characters (characters that cannnot be an attribute of an object), to underscores.
    'this name' -> 'this_name'
    'this-name' -> 'this_name'
    '0starting_with_zero' -> '_starting_with_zero'

    Example:
    A trail has a step named 'some_long_step_name'. This step has a tag like:
        {'name': 'some_long_step_name'}
    Typing a long name like 'some_long_step_name' can be cumbersome.

    Using a Names object makes it easier as follows:
        names = Names(['some_long_step_name'])

    The above will create a Names object containing all the names as attributes. Therefore:
        names.some_long_step_name -> 'some_long_step_name'

    This allows the names like names.some_long_step_name to be available with auto-completion.
    """
    def __init__(self, names_list):
        replacer_regex = re.compile('(^\d|\W)')
        for name in names_list:
            valid_attribute_name = replacer_regex.sub('_', name)
            self.__dict__[valid_attribute_name] = name


def create_namespaces_for_tag_keys_and_values(root_step):
    """Creates two Names objects from all the tags in the given DAG.

    To know what a Names object is, please read the documentation of the Names class.

    Arguments:
    root_step   -- A Step like object which is the base of the DAG.

    Returns:
    A tuple of Names objects of the form: (<Names of keys>, <Names of values>)

    If a step is named 'first_step', then, it will have a tag called 'name'.
    So, at the least, the step will have the following tags:
    {
        'name': 'first_step',
        'n'   : 0,
    }

    Calling this function will return two Names objects as follows:
    k, v = create_namespaces_for_tag_keys_and_values(first_step)

    k.name       -> 'name'
    v.first_step -> 'first_step'
    """
    # Each step has a tags attribute which is a dictionary containing all the tags used. We need to take all the keys
    # and values and add them to our list of names. Adding them allows autocompletion to be used on them as well.
    keys = []
    values = []
    for step in topological_traverse(root_step):
        for key, value in iteritems(step.tags):
            # Exclude the 'n' tag, which is an integer.
            if key != 'n':
                keys.append(key)
                values.append(value)

    # Remove duplicate entries as the keys in tags are likely to be repeated.
    keys = set(keys)
    values = set(values)

    return (Names(keys), Names(values))
