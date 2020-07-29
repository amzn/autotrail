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

I/O helpers

This module defines some helpers for user I/O.
"""


import sys

from operator import itemgetter

from autotrail.workflow.default_workflow.api import StatusField


def print_step_statuses(step_statuses, to):
    """Print the step statuses in the following format:
    - Name         : <step name>
      n            : <'n' tag>
      <Field>      : <single line value>
      <List Field> : <first value>
                   : <second value>
    <blank line>

    The whitespace around ':' is computed based on the field names defined in StatusField.

    :param step_statuses:   A list of dictionaries containing keys defined in the StatusField namespace.
    :param to:              The stream to write to.
    :return:                None
    """
    all_fields = {StatusField.NAME, StatusField.TAGS, StatusField.STATE, StatusField.ACTIONS, StatusField.IO,
                  StatusField.RETURN_VALUE, StatusField.EXCEPTION}
    single_value_fields = [StatusField.STATE, StatusField.RETURN_VALUE, StatusField.EXCEPTION]
    multi_value_fields = [StatusField.IO, StatusField.OUTPUT, StatusField.ACTIONS]

    max_field_length = len(max(all_fields, key=len))
    header = '- {{field:<{}}}: {{value}}'.format(max_field_length)
    body = '  {{field:<{}}}: {{value}}'.format(max_field_length)
    print('Status of steps:', file=to)
    for step_id, step_status in sorted(step_statuses.items()):
        print(header.format(field=StatusField.NAME, value=step_status[StatusField.NAME]), file=to)
        print(body.format(field='n', value=step_id), file=to)
        for field in step_status:
            if field in single_value_fields:
                print(body.format(field=field, value=step_status[field]), file=to)
            elif field in multi_value_fields:
                if step_status[field]:
                    for i, message in enumerate(step_status[field]):
                        field = field if i == 0 else ''
                        print(body.format(field=field, value=str(message)), file=to)
                else:
                    print(body.format(field=field, value=step_status[field]), file=to)
            elif field == StatusField.TAGS:
                for key, value in step_status[StatusField.TAGS].items():
                    print(body.format(field=field, value='{} = {}'.format(key, value)), file=to)
                    field = ''
        print('', file=to)


def print_error(method_name, error_message, to):
    """Print error received for an API call.

    Prints in the following format:
    - API Call: <API call name>
      Error: <error_message>

    :param method_name:     The API method called.
    :param error_message:   The error message.
    :param to:              The stream to write to.
    :return:                None
    """
    print('- API Call: {}'.format(method_name), file=to)
    print('  Error: {}'.format(error_message), file=to)


def print_no_result(method_name, to):
    """Print when an API call did not fetch a result.

    Prints in the following format:
    - API Call: <The API method called>
      No result received for the API call.

    :param method_name: The API method called.
    :param to:          The stream to write to.
    :return:            None
    """
    print('- API Call: {}'.format(method_name), file=to)
    print('  No result received for the API call.', file=to)


def print_affected_steps(method_name, affected_step_ids, dry_run, step_id_to_name_mapping, to):
    """Print the steps affected by an API call.

    Prints in the following format:
    - API Call: <The API method called>
      Mode: Dry Run <or 'Actual Run'>
      List of step names that are affected:
        # <'n' tag> - <'name' tag of step>
        ...

    :param method_name:             The API method called.
    :param affected_step_ids:       The list of step IDs affected.
    :param dry_run:                 True or False representing if the API call was made with a dry run.
    :param step_id_to_name_mapping: A mapping of the form:
                                    {
                                      <Step 1 ID>: <Step 1 Name (str)>,
                                      ...
                                    }
    :param to:                      The stream to write to.
    :return:                        None
    """
    print('- API Call: {}'.format(method_name), file=to)
    print('  Dry Run: {}'.format(dry_run), file=to)
    print('  List of step names that are affected:', file=to)
    for step_id in sorted(affected_step_ids):
        print('    # {} - {}'.format(step_id, step_id_to_name_mapping[step_id]), file=to)


def print_step_list(steps_tags, to):
    """Print the list of steps and their associated tags in a user friendly way to the given stream.

    Prints in the following format:
    - Name: <'name' tag>
      n: <'n' tag>
      other_key: <'other_key' tag>

    :param steps_tags:  A list of dicts. Each dictionary should atleast contain the 'n' and 'name' keys.
    :param to:          The stream to write to.
    :return:            None
    """
    print('List of steps and their tags:', file=to)
    for step_tags in sorted(steps_tags, key=itemgetter('n')):
        print('- Name: {}'.format(step_tags['name']), file=to)
        print('  n: {}'.format(step_tags['n']), file=to)
        for key, value in step_tags.items():
            if key in ['name', 'n']:
                continue
            print('  {key}: {value}'.format(key=key, value=value), file=to)


class InteractiveClientWrapper:
    """Client wrapper that adds interactivity for the default workflow API.

    This class supports all the API methods of the workflow.default_workflow.api.WorkflowAPIHandler class.
    All it does is makes the API call using the given client, parses the API result obtained, and prints it to STDOUT.
    Any errors are printed to STDERR.
    """
    def __init__(self, method_api_client, stdout=sys.stdout, stderr=sys.stderr, status_printer=print_step_statuses,
                 affected_steps_printer=print_affected_steps, step_list_printer=print_step_list,
                 error_printer=print_error, no_result_printer=print_no_result):
        """Initialize an interactive wrapper.

        :param method_api_client        A MethodAPIClientWrapper object or similar.
        :param stdout:                  The stream to write STDOUT messages to. Defaults to sys.stdout.
        :param stderr:                  The stream to write STDERR messages to. Defaults to sys.stderr.
        :param status_printer:          The function used to print the status API result.
                                        Should accept:
                                        1. The result of the API call
                                        2. The stream to write to.
        :param affected_steps_printer:  The function used to print affected steps returned by API calls.
                                        Should accept:
                                        1. The name of the API call.
                                        2. The result of the API call, a list of tags containing only the 'n' and 'name'
                                           tags.
                                        3. If this run was a dry_run or not.
                                        4. The stream to write to.
        :param step_list_printer:       The function used to print a list of step tags.
                                        Should accept:
                                        1. The step tags (a list of dictionaries, containing all the tags of a step).
                                        2. The stream to write to.
        :param error_printer:           The function used to print the error message received as a response to an API
                                        call.
                                        Should accept:
                                        1. The method name that resulted in an error.
                                        2. The error message received from the API call.
                                        3. The stream to write to.
        :param no_result_printer:       The function used to print when no result was received by an API call.
                                        Should accept:
                                        1. The method name that was invoked.
                                        2. The stream to write to.
        """
        self.client = method_api_client
        self.stdout = stdout
        self.stderr = stderr

        try:
            tags_of_all_steps = self.client.list()
        except Exception as e:
            raise ValueError('Unable to query the trail due to error: {}'.format(e))

        if not tags_of_all_steps:
            raise ValueError('Unable to query the trail. No response to "status" API call.')

        self.step_id_to_name_mapping = {step_tags['n']: step_tags['name'] for step_tags in tags_of_all_steps}

        self.error_printer = error_printer
        self.no_result_printer = no_result_printer
        self.status_printer = status_printer
        self.affected_steps_printer = affected_steps_printer
        self.step_list_printer = step_list_printer

    def _make_affected_steps_printer(self, method, dry_run):
        """Create a function that accepts an API result containing a list of step tags and calls the
        'affected_steps_printer'.

        :param method:  String representing the method of the TrailClient class to be called.
        :param dry_run: Boolean. Whether the API call was made with a "True" or "False".
        :return:        A function that accepts an API result compatible with the affected_steps_printer.
        """
        return lambda result: self.affected_steps_printer(method, result, dry_run, self.step_id_to_name_mapping,
                                                          self.stdout)

    def _call_client_method(self, method, printer, *args, **kwargs):
        """Calls the given method of the WorkflowAPIHandler object with the provided args and kwargs and then calls the
        given printer function with the result.

        If the method call raises and exception, then it is printed using the error_printer.
        If no result was received, then it is printed using the no_result_printer.

        :param method:  String representing the method of the TrailClient class to be called.
        :param printer: The printer function to be called to print the API result. This function should accept only one
                        argument, viz., the result of the API call.
        :param args:    Any arguments to be passed to the client method. Depends on the method being called.
        :param kwargs:  Any keyword arguments to be passed to the client method. Depends on the method being called.
        :return:        None. Side-effect: calls the given printer function with the result.
        """
        try:
            result = getattr(self.client, method)(*args, **kwargs)
        except Exception as error:
            self.error_printer(method, str(error), self.stderr)
            return

        if result is not None:
            printer(result)
        else:
            self.no_result_printer(method, self.stderr)

    def start(self, dry_run=True):
        """Send message to start a trail. This will change the state of all steps in 'Ready' state to 'Waiting' state.

        :param dry_run: Boolean. API call doesn't have any effect when True.
        :return:        None. Prints the 'name' and 'n' tags of steps that were affected.
        """
        self._call_client_method('start', self._make_affected_steps_printer('start', dry_run), dry_run=dry_run)

    def shutdown(self, dry_run=True):
        """Send message to shutdown the trail server. After this call, no other API calls can be served.

        :param dry_run: Boolean. API call doesn't have any effect when True.
        :return:        None. Prints the 'name' and 'n' tags of steps that were interrupted.
        """
        self._call_client_method('shutdown', self._make_affected_steps_printer('shutdown', dry_run), dry_run=dry_run)

    def send_message_to_steps(self, message, dry_run=True, **tags):
        """Send a message to matching steps.

        :param message: Any JSON encodable Python object.
        :param dry_run: Boolean. API call doesn't have any effect when True.
        :param tags:    Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.
        :return:        None. Prints the 'name' and 'n' tags of steps that were affected.
        """
        self._call_client_method('send_message_to_steps',
                                 self._make_affected_steps_printer('send_message_to_steps', dry_run), message,
                                 dry_run=dry_run, **tags)

    def list(self, **tags):
        """List all steps in a trail (in topological order).

        :param tags:    Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.
                        If no tags are provided, this will list all the steps in the queue.
        :return:        None. Prints the list of matching Step names and all of their associated tags.
        """
        printer = lambda result: self.step_list_printer(result, self.stdout)
        self._call_client_method('list', printer, **tags)

    def status(self, fields=None, states=None, **tags):
        """Get trail status.

        :param fields:  List of field names to return in the results. A list of strings from StatusField class.
                        This will limit the status to the fields requested.
                        Defaults to all the fields in the StatusField namespace.
                        No matter what fields are passed, the following 2 fields will always be present in the result:
                            StatusField.N: <Sequence number of the step>
                            StatusField.NAME: <Name of the step>
                        This is because without these fields, it will be impossible to uniquely identify the steps.
        :param states:  List of strings that represent the states of a Step.
                        This will limit the status to only the steps that are in the given list of states.
        :param tags:    Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.
        :return:        None. Prints the status of each step that matches the given tags and states.
                        It will contain the fields specified in the fields keyword argument along with the required
                        fields explained above.
        """
        printer = lambda result: self.status_printer(result, self.stdout)
        self._call_client_method('status', printer, fields=fields, states=states, **tags)

    def steps_waiting_for_user_input(self, **tags):
        """Get status of steps that are waiting for user input.
        Limits the fields to only include the UNREPLIED_PROMPT_MESSAGE.
        Excludes steps that do not have any UNREPLIED_PROMPT_MESSAGE.

        :param tags:    Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.
        :return:        None. Prints the status of only the steps that are waiting for user input containing the
                        following 3 fields:
                        1. The 'name' and 'n' tags of steps that are waiting for user input.
                        2. Their unreplied prompt message.
        """
        printer = lambda result: self.status_printer(result, self.stdout)
        self._call_client_method('steps_waiting_for_user_input', printer, **tags)

    def pause(self, dry_run=True, **tags):
        """Send message to pause steps.
        Mark the specified steps so that they are paused.
        A step can only be paused if it is ready or waiting.

        :param dry_run: Boolean. API call doesn't have any effect when True.
        :param tags:    Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.
                        If tags are provided, then only the steps matching the tags will be paused.
                        If no tags are provided, all possible steps will be marked to be paused.
        :return:        None. Prints the 'name' and 'n' tags of steps that were affected.
        """
        self._call_client_method('pause', self._make_affected_steps_printer('pause', dry_run), dry_run=dry_run, **tags)

    def interrupt(self, dry_run=True, **tags):
        """Send message to interrupt running steps.
        This will interrupt the running steps by sending them SIGINT. A step can be interrupted only if it is running.

        :param dry_run: Boolean. API call doesn't have any effect when True.
        :param tags:    Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.
                        If tags are provided, then only the steps matching the tags will be interrupted.
                        If no tags are provided, all possible steps will be marked to be interrupted.
        :return:        None. Prints the 'name' and 'n' tags of steps that were interrupted.
        """
        self._call_client_method('interrupt', self._make_affected_steps_printer('interrupt', dry_run), dry_run=dry_run,
                                 **tags)

    def resume(self, dry_run=True, **tags):
        """Send message to resume steps.
        Mark the specified steps so that they are resumed.
        A step can only be resumed if it is either paused, marked to pause, paused due to failure or interrupted.

        :param dry_run: Boolean. API call doesn't have any effect when True.
        :param tags:    Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.
                        If tags are provided, then only the steps matching the tags will be resumed.
                        If no tags are provided, all possible steps will be marked to be resumed.
        :return:        None. Prints the 'name' and 'n' tags of steps that were resumed.
        """
        self._call_client_method('resume', self._make_affected_steps_printer('resume', dry_run), dry_run=dry_run,
                                 **tags)

    def rerun(self, dry_run=True, **tags):
        """Send message to resume steps.
        Mark the specified steps so that they are resumed.
        A step can only be resumed if it is either paused, marked to pause, paused due to failure or interrupted.

        :param dry_run: Boolean. API call doesn't have any effect when True.
        :param tags:    Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.
                        If tags are provided, then only the steps matching the tags will be re-run.
                        If no tags are provided, all possible steps will be marked to be re-run.
        :return:        None. Prints the 'name' and 'n' tags of steps that were re-run.
        """
        self._call_client_method('rerun', self._make_affected_steps_printer('rerun', dry_run), dry_run=dry_run, **tags)

    def skip(self, dry_run=True, **tags):
        """Send message to mark steps to be skipped.
        Mark the specified steps so that they are skipped when execution reaches them.
        A step can only be skipped if it is either paused, marked to pause, paused due to failure, waiting or ready.

        :param dry_run: Boolean. API call doesn't have any effect when True.
        :param tags:    Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.
                        If tags are provided, then only the steps matching the tags will be skipped.
                        If no tags are provided, all possible steps will be marked to be skipped.
        :return:        None. Prints the 'name' and 'n' tags of steps that were marked to be skipped.
        """
        self._call_client_method('skip', self._make_affected_steps_printer('skip', dry_run), dry_run=dry_run, **tags)

    def unskip(self, dry_run=True, **tags):
        """Send message to un-skip steps that have been marked to be skipped.
        A step can be un-skipped only if it is marked to be skipped but has not been skipped yet.
        Once a step has already been skipped, this API call will not affect them (they cannot be unskipped).

        :param dry_run: Boolean. API call doesn't have any effect when True.
        :param tags:    Any key=value pair provided in the arguments is treated as a tag, except for dry_run=True.
                        Each step by default gets a tag viz., name=<action_function_name>.
                        If tags are provided, then only the steps matching the tags will be un-skipped.
                        If no tags are provided, all possible steps will be marked to be skipped.
        :return:        None. Prints the 'name' and 'n' tags of steps that were unmarked from being skipped.
        """
        self._call_client_method('unskip', self._make_affected_steps_printer('unskip', dry_run), dry_run=dry_run,
                                 **tags)
