"""Example of a CLI client an interactive trail."""

import os

from argparse import ArgumentParser

from autotrail import TrailClient, Step, interactive, StatusField, InteractiveTrail
from autotrail.helpers.cli_client import (add_arguments_to_parser, CLIArgs, cli_args_for_tag_keys_and_values,
                                         extract_args_and_kwargs, get_tag_keys_and_values, get_tags_from_namespace,
                                         make_api_call, METHODS_CLI_SWITCH_MAPPING, PARAMETER_EXTRACTOR_MAPPING,
                                         setup_cli_args_for_trail_client)


socket_file = os.path.join('/', 'tmp', 'autotrail.example.server')
example_trail = TrailClient(socket_file)
example = InteractiveTrail(example_trail)
tag_keys_and_values = get_tag_keys_and_values(example_trail.list())

substitution_mapping = {
    CLIArgs.DRY_RUN: [extract_args_and_kwargs('--submit', dest='dry_run', action='store_false', help=(
        'Affect changes. Without this option all the API calls will run in dry-run mode.'))],
    CLIArgs.TAGS: list(cli_args_for_tag_keys_and_values(tag_keys_and_values)),
}

full_parser = ArgumentParser(description='CLI Client for the example trail.')
setup_cli_args_for_trail_client(full_parser, substitution_mapping)
cli_args = full_parser.parse_args()
tags = get_tags_from_namespace(tag_keys_and_values, cli_args)

make_api_call(cli_args, tags, example)
