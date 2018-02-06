"""Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.

Helpers to setup a CLI client for a trail.

Using these helpers, one can interact with an InteractiveTrail object using a CLI wrapper so that the user can invoke
the client's methods from the CLI without the need to run an interactive session.

The main function to use is: setup_cli_args_for_trail_client
Also read up on the substitution_mapping and methods_cli_switch_mapping data-structures because they control how
setup_cli_args_for_trail_client behaves.
"""


import logging


def extract_args_and_kwargs(*args, **kwargs):
    """Accept args and kwargs and return a tuple containing them in the form (args, kwargs)."""
    return (args, kwargs)


class CLIArgs(object):
    """A namespace used to contain values that indiciate if a method neeeds the CLI switches for dry_run and tags."""
    DRY_RUN = 'DRY_RUN'
    TAGS = 'TAGS'


MESSAGE_TYPE_CONVERSION_MAPPING = {
    # Mapping used to convert an argument passed in the CLI to the correct type.
    'str': str,
    'int': int,
    'bool': bool,
}


METHODS_CLI_SWITCH_MAPPING = {
    # To understand the structure of this dictionary please refer to the description of methods_cli_switch_mapping
    # in the substitute_cli_switches function
    # All references to CLIArgs.DRY_RUN and CLIArgs.TAGS will be replaced with proper args, kwargs lists by a
    # call to substitute_cli_switches.
    'start': (
        'Send message to start a trail.',
        [
            extract_args_and_kwargs('--stepped', action='store_true', help=(
                'The trail will be paused immediately upon start. This has the effect of running only one step, '
                'allowing the user to run one step at a time and running the next step using the "next_step()" '
                'method.'))
        ]
    ),
    'stop': (
        ('Stop the run of a trail safely. This will stop the run of a trail but only after current in-progress '
         'steps are done.'),
        [
            extract_args_and_kwargs('--interrupt', action='store_true',
                                    help='Place an interrupt call on any running steps.')
        ]
    ),
    'next_step': (
        'Send message to run next step.',
        [
            extract_args_and_kwargs('--step-count', type=int, default=1, help=(
                'A number greater than 0. This is the number of steps to run before pausing again. '
                'By default this is set to 1 so that the trail will pause after running the next step.')),
            CLIArgs.DRY_RUN
        ]
    ),
    'send_message_to_steps': (
        'Send a message to matching steps.',
        [
            extract_args_and_kwargs('--message', type=str, required=True, help=(
                'A string, integer or boolean. Examples: 1) "yes\n": If you need to send a "yes" followed by a '
                'carriage return. Useful when sending a message to a command that is waiting for STDIN. 2) True: A '
                'boolean being sent. Usually used by Instruction helper step waiting for a step to be done by the '
                'user. 3) Any other object/string/number that any action function might be expecting.')),
            extract_args_and_kwargs('--type', type=str, required=True, choices=MESSAGE_TYPE_CONVERSION_MAPPING.keys(),
                                    help='The type of the message being passed.'),
            CLIArgs.DRY_RUN,
            CLIArgs.TAGS
        ]
    ),
    'is_running': (
        ('Checks whether the trail process is running or not. The trail process needs to be running in order to '
         'respond to API calls. This method can be used to check if the trail process is running or not.'),
        []
    ),
    'status': (
        'Get trail status. The output can be constrained by various criteria.',
        [
            extract_args_and_kwargs('--field', dest='fields', type=str, action='append', choices=[
                'n', 'Name', 'State', 'Return value', 'Output messages', 'Input messages', 'New output messages',
                'Prompt messages', 'Waiting for response'], help=(
                'Field names to return in the results. This will limit the status to the fields requested. Defaults '
                'to "n", "Name", "State", "Waiting for response" and "Return value". If the trail has finished, the '
                'fields used are: "Name", "State" and "Return value". No matter what fields are passed, '
                '"n" and "Name" will always be present. This is because without these fields, it will be impossible '
                'to uniquely identify the steps.')),
            extract_args_and_kwargs('--state', type=str, action='append', choices=[
                'Waiting', 'Running', 'Succeeded', 'Failed', 'Skipped', 'Blocked', 'Paused',
                'Paused due to failure'], help='Limit the status to only steps in these states.'),
            CLIArgs.TAGS
        ]
    ),
    'steps_waiting_for_user_input': (
        ('Get status of steps that are waiting for user input. This is exactly like a status call, but limits the '
         'fields to only include "Waiting for response" and excludes steps that are not "Waiting for response".'),
        [
            CLIArgs.TAGS
        ]
    ),
    'pause': (
        ('Pause Steps. Mark the specified steps so that they are paused when execution reaches them. A step can '
         'be paused only if it is waiting.'),
        [
            CLIArgs.DRY_RUN,
            CLIArgs.TAGS
        ]
    ),
    'pause_branch': (
        ('Pause all Steps in matching branches. Look for all the steps that are present in the branches originating '
         'from each of the matching steps and mark them so that they are paused. A step can be paused only if it is '
         'waiting.'),
        [
            CLIArgs.DRY_RUN,
            CLIArgs.TAGS
        ]
    ),
    'unset_pause_on_fail': (
        ('Clear the pause_on_fail flag of a step. By default, all steps are marked with pause_on_fail so that they '
         'are paused if they fail. This allows the user to either re-run the step, skip it or block it. This API call '
         'unsets this flag. This will cause the Step to fully fail (irrecoverably) if the action function fails '
         '(raises an exception).'),
        [
            CLIArgs.DRY_RUN,
            CLIArgs.TAGS
        ]
    ),
    'interrupt': (
        ('Interrupt Steps. Mark the specified steps so that they are interrupted. A step can be interrupted only if '
         'it is running.'),
        [
            CLIArgs.DRY_RUN,
            CLIArgs.TAGS
        ]
    ),
    'resume': (
        'Resume Steps. Mark the specified steps so that they are resumed. A step can be resumed only if it is paused.',
        [
            CLIArgs.DRY_RUN,
            CLIArgs.TAGS
        ]
    ),
    'resume_branch': (
        ('Resume all Steps in matching branches. Look for all the steps that are present in the branches originating '
         'from each of the matching steps and mark them so that they are resumed. A step can be resumed only if it is '
         'paused.'),
        [
            CLIArgs.DRY_RUN,
            CLIArgs.TAGS
        ]
    ),
    'list': (
        'List all steps in a trail (in topological order) along with their associated tags.',
        [
            CLIArgs.TAGS
        ]
    ),
    'skip': (
        'Skip steps. Mark the specified steps so that they are skipped. A step can be skipped only if it is waiting.',
        [
            CLIArgs.DRY_RUN,
            CLIArgs.TAGS
        ]
    ),
    'unskip': (
        ('Un-Skip steps. Mark the specified steps so that they are un-skipped. A step can be un-skipped only if it '
         'is skipped.'),
        [
            CLIArgs.DRY_RUN,
            CLIArgs.TAGS
        ]
    ),
    'block': (
        'Block steps. Mark the specified steps so that they are blocked. A step can be blocked only if it is WAITING.',
        [
            CLIArgs.DRY_RUN,
            CLIArgs.TAGS
        ]
    ),
    'unblock': (
        ('Un-Block steps. Mark the specified steps so that they are unblocked. A step can be blocked only if it is '
         'BLOCKED.'),
        [
            CLIArgs.DRY_RUN,
            CLIArgs.TAGS
        ]
    ),
}


PARAMETER_EXTRACTOR_MAPPING = {
    # To understand the structure of this dictionary please refer to the description of parameter_extractor_mapping
    # in the make_api_call function.
    'start': lambda namespace, tags: dict(stepped=namespace.stepped),
    'stop': lambda namespace, tags: dict(interrupt=namespace.interrupt),
    'next_step': lambda namespace, tags: dict(step_count=namespace.step_count, dry_run=namespace.dry_run),
    'send_message_to_steps': lambda namespace, tags: dict(
        message=MESSAGE_TYPE_CONVERSION_MAPPING[namespace.type](namespace.message),
        dry_run=namespace.dry_run, **tags),
    'is_running': lambda namespace, tags: dict(),
    'status': lambda namespace, tags: dict(fields=namespace.fields, states=namespace.state, **tags),
    'steps_waiting_for_user_input': lambda namespace, tags: dict(**tags),
    'pause': lambda namespace, tags: dict(dry_run=namespace.dry_run, **tags),
    'pause_branch': lambda namespace, tags: dict(dry_run=namespace.dry_run, **tags),
    'unset_pause_on_fail': lambda namespace, tags: dict(dry_run=namespace.dry_run, **tags),
    'interrupt': lambda namespace, tags: dict(dry_run=namespace.dry_run, **tags),
    'resume': lambda namespace, tags: dict(dry_run=namespace.dry_run, **tags),
    'resume_branch': lambda namespace, tags: dict(dry_run=namespace.dry_run, **tags),
    'list': lambda namespace, tags: dict(**tags),
    'skip': lambda namespace, tags: dict(dry_run=namespace.dry_run, **tags),
    'unskip': lambda namespace, tags: dict(dry_run=namespace.dry_run, **tags),
    'block': lambda namespace, tags: dict(dry_run=namespace.dry_run, **tags),
    'unblock': lambda namespace, tags: dict(dry_run=namespace.dry_run, **tags),
}


def get_tag_keys_and_values(list_of_tags):
    """Creates a dictonary of all the tag keys and values from the list of tags provided.

    If list_of_tags is:
    [
        {'n': 0, 'foo': 'bar'},
        {'n': 1},
        {'n': 2, 'foo': 'bar'},
    ]

    The returned dictonary will be:
    {
        'n': [0, 1, 2],
        'foo': ['bar'],
    }

    Notice that repeated tag values are removed and only unique values are preserved.
    """
    tag_key_values = {}

    for step_tags in list_of_tags:
        for key in step_tags:
            if key in tag_key_values:
                if step_tags[key] not in tag_key_values[key]:
                    tag_key_values[key].append(step_tags[key])
            else:
                tag_key_values[key] = [step_tags[key]]
    return tag_key_values


def cli_args_for_tag_keys_and_values(tag_keys_and_values):
    """Generate arguments for ArgumentParser.add_argument using the given tag_keys_and_values.

    For example:
        list(cli_args_for_tag_keys_and_values(tag_keys_and_values = [{'foo': 'bar'}]))
    Will return:
        [(('--foo',), {choices=['bar'], type=str})]
    Which when passed to ArgumentParser.add_argument will have the effect of:
        ArgumentParser.add_argument('--foo', choices=['bar'], type=str)

    Arguments:
    tag_keys_and_values -- Dictionary mapping tag keys with their valid values.
                           This  dictonary is of the form:
                           {
                               'n': [0, 1, 2],
                               'foo': ['bar'],
                           }
                           Which says that the tag 'n' can have the possible values of 0, 1 and 2.

    Returns:
    A generator that yields a tuple of the form (args, kwargs) which can be passed to ArgumentParser.add_argument.
    """
    return (extract_args_and_kwargs('--{}'.format(key), choices=possible_values, type=type(possible_values[0]))
            for key, possible_values in tag_keys_and_values.iteritems())


def get_tags_from_namespace(tag_keys_and_values, namespace):
    """Extract the tags from namespace using tag_keys_and_values.

    Arguments:
    tag_keys_and_values -- Dictionary mapping tag keys with their valid values.
                           This  dictonary is of the form:
                           {
                               'n': [0, 1, 2],
                               'foo': ['bar'],
                           }
                           Which says that the tag 'n' can have the possible values of 0, 1 and 2.
    namespace           -- A Namespace object containing the tags passed.

    Returns:
    A dictionary containing the tags extracted from the namespace object.
    """
    tags = {}
    for tag_key in tag_keys_and_values:
        cli_tag_value = getattr(namespace, tag_key, None)
        if cli_tag_value is None:
            continue
        if cli_tag_value not in tag_keys_and_values[tag_key]:
            raise ValueError(
                'Invalid value: {value} provided for the tag: {key}'.format(key=tag_key, value=cli_tag_value))
        tags[tag_key] = cli_tag_value
    return tags


def substitute_cli_switches(methods_cli_switch_mapping, substitution_mapping):
    """Substitutes certain strings/values in methods_cli_switch_mapping based on the given substitution_mapping.

    This function replaces all occurances of the special values present in methods_cli_switch_mapping with new values
    obtained from substitution_mapping.
    The "special values" and "new values" are obtained from substitution_mapping.
    Both the data-structures are explained below.

    Arguments:
    methods_cli_switch_mapping --  A mapping of methods to the information necessary to create CLI switches for them
                                   using ArgumentParser.
                                   The structure of this dictionary is as follows:
                                     '<method name>': ('<help text>', [(args, kwargs), ...])
                                   Where:
                                     '<method name>' is the method of the TrailClient object that will be invoked with
                                     the CLI switch.
                                     '<help text>' is the documentation explaining what this method does.
                                     [(args, kwargs), ...] is a list of tuples containing args and kwargs passed to the
                                     parser. For example:
                                         parser.add_argument('--stepped', action='store_true',
                                                             help='Pause all steps before starting.')
                                     Should be expressed here as:
                                         (('--stepped',), dict(action='store_true',
                                                               help='Pause all steps before starting.'))
                                     To avoid verbose repetition, if a method needs dry_run or tags to be passed, they
                                     need not be specified in full here. Instead, their requirement can be indicated
                                     for example, using CLIArgs.DRY_RUN and CLIArgs.TAGS.
                                     These substitution names can be anything that is defined in the
                                     substitution_mapping.

    substitution_mapping       -- A mapping of keys to values that will replace the keys' presence in
                                  methods_cli_switch_mapping.
                                  Each key is mapped to a list of tuples as is done in methods_cli_switch_mapping.
                                  i.e., each tuple is of the form: (args, kwargs)
                                  The list of tuples should args and kwargs will be passed to the parser.

    Side-effect:
    All references to the keys (defined in substitution_mapping) present in methods_cli_switch_mapping will be replaced
    with the new values obtained from substitution_mapping.

    Example:
        methods_cli_switch_mapping = {
            'example_method': ('example help', [
                (('--example',), dict(action='store_true')),
                CLIArgs.DRY_RUN],
        }
        substitution_mapping = {
            CLIArgs.DRY_RUN: [(('--submit',), dict(dest='dry_run', action='store_false', help=''))],
        }
        substitute_cli_switches(methods_cli_switch_mapping, substitution_mapping)

    The above will result in methods_cli_switch_mapping being modified to:
        methods_cli_switch_mapping = {
            'example_method': ('example help', [
                (('--example',), dict(action='store_true')),
                (('--submit',), dict(dest='dry_run', action='store_false', help=''))],
        }
    """
    for method in methods_cli_switch_mapping:
        help_text, args_and_kwargs_list = methods_cli_switch_mapping[method]
        new_args_and_kwargs_list = []
        for args_and_kwargs in args_and_kwargs_list:
            if args_and_kwargs in substitution_mapping.keys():
                new_args_and_kwargs_list.extend(substitution_mapping[str(args_and_kwargs)])
            else:
                new_args_and_kwargs_list.append(args_and_kwargs)
        methods_cli_switch_mapping[method] = (help_text, new_args_and_kwargs_list)


def add_arguments_to_parser(parser, args_and_kwargs_iterable):
    """Invoke the 'add_argument' method of the given parser object by iterating over and passing the args and kwargs
    obtained from args_and_kwargs_iterable.

    Arguments:
    parser                   -- A ArgumentParser or similar object.
    args_and_kwargs_iterable -- An iterable which yields a tuple consisting of the args (tuple) and kwargs (dict) to
                                be passed to add_argument. This iterable can be like:
                                [(args, kwargs)]
                                Where:
                                    args is a tuple that will be passed using *args.
                                    kwargs is a mapping that will be passed using **kwargs.

    Side-effect:
    parser.add_argument will be invoked with the args and kwargs supplied.
    """
    for args, kwargs in args_and_kwargs_iterable:
        parser.add_argument(*args, **kwargs)


def setup_cli_args_for_trail_client(parser, substitution_mapping,
                                    methods_cli_switch_mapping=METHODS_CLI_SWITCH_MAPPING):
    """

    Arguments:
    parser                     -- A ArgumentParser or similar object.
    substitution_mapping       -- A mapping of keys to values that will replace the keys' presence in
                                  methods_cli_switch_mapping.
                                  Each key is mapped to a list of tuples as is done in methods_cli_switch_mapping.
                                  i.e., each tuple is of the form: (args, kwargs)
                                  The list of tuples should args and kwargs will be passed to the parser.

    Keyword Arguments:
    methods_cli_switch_mapping --  A mapping of methods to the information necessary to create CLI switches for them
                                   using ArgumentParser.
                                   The structure of this dictionary is as follows:
                                     '<method name>': ('<help text>', [(args, kwargs), ...])
                                   Where:
                                     '<method name>' is the method of the TrailClient object that will be invoked with
                                     the CLI switch.
                                     '<help text>' is the documentation explaining what this method does.
                                     [(args, kwargs), ...] is a list of tuples containing args and kwargs passed to the
                                     parser. For example:
                                         parser.add_argument('--stepped', action='store_true',
                                                             help='Pause all steps before starting.')
                                     Should be expressed here as:
                                         (('--stepped',), dict(action='store_true',
                                                               help='Pause all steps before starting.'))
                                     To avoid verbose repetition, if a method needs dry_run or tags to be passed, they
                                     need not be specified in full here. Instead, their requirement can be indicated
                                     using CLIArgs.DRY_RUN and CLIArgs.TAGS.

    Side-effect:
    Multiple sub-parsers will be added to the given parser.
    One sub-parser for each method will be added.
    """
    substitute_cli_switches(methods_cli_switch_mapping, substitution_mapping)
    method_parser = parser.add_subparsers(help='Choose one of the methods.', dest='method')
    for method in methods_cli_switch_mapping:
        help_text, args_and_kwargs_list = methods_cli_switch_mapping[method]
        sub_parser = method_parser.add_parser(method, help=help_text)
        add_arguments_to_parser(sub_parser, args_and_kwargs_list)


def make_api_call(namespace, tags, client, parameter_extractor_mapping=PARAMETER_EXTRACTOR_MAPPING):
    """Make an API call using the client object with parameters from the namespace ang tags.
    Uses parameter_extractor_mapping to prepare the parameters as explained below.

    Arguments:
    namespace                       -- The namespace object containing attributes required to make the API call.
                                       The attributes required are defined in parameter_extractor_mapping.
                                       Requires the 'method' attribute to be present i.e., namespace.method should
                                       be a string.
    tags                            -- Dictionary for key-value pairs for the tags.
    client                          -- The object against which API calls will be made. It should support all methods
                                       that are passed as namespace.method.

    Keyword Arguments:
    parameter_extractor_mapping     -- A mapping of a method name to the function used to extract the required
                                       parameters from the given namespace. This mapping is of the form:
                                       {
                                        '<method>': <function/lambda that accepts namespace and tags.
                                                     Returns a dictionary>,
                                       }
                                       Where:
                                       - '<method>' is the method name (string) of the client object that will be
                                         invoked using the parameters.
                                       - namespace is an object containing the attributes that the lambda can accept.
                                       - tags is a dictionary of key-value pairs.
                                       - The returned dictionary will be used as kwargs for the method call to client
                                         object.

    Returns:
    The result of the method call.
    """
    kwargs = parameter_extractor_mapping[namespace.method](namespace, tags)
    logging.info('Method call: client.{}(**{})'.format(namespace.method, kwargs))
    getattr(client, namespace.method)(**kwargs)
