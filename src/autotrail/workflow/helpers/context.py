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
from autotrail.core.api.management import read_messages
from autotrail.core.api.serializers import SerializerCallable, Serializer


def make_context(**kwargs):
    """Factory to create a context dictionary containing the given kwargs.

    :param kwargs:  Any key-value pairs.
    :return:        A dictionary of the following form:
                    {
                        'step_data': {},
                        <key 1 from kwargs>: <value 1 from kwargs>,
                        ...
                    }
    """
    context = {
        'step_data': {},
    }
    context.update(kwargs)
    return context


def serialize_step_data(context, timeout=0.1):
    """Serialize the step data in the context dictionary.

    :param context: The context mapping of the following form:
                    {
                        'step_data': {
                            <step id>: {
                                # All fields are optional in this sub-dictionary
                                'rw_connection':    The connection used to for two-way communication with a step.
                                'ro_connection':    The connection used for one-way communication out of a step.
                                'io':               The I/O messages collected from a step.
                                'output':           The output messages collected from a step.
                                'return_value:      The return value of running a step.
                                'exception':        The exception raised by the step.
                            },
                            ...
                        }
                        # Other custom fields.
                        ...
                    }
    :param timeout: The timeout in seconds to wait for messages from the step (I/O and output).
    :return:        A dictionary of the form:
                    {
                        <ID of Step 1>: {
                            'return_value': <The value returned by running the step> or None,
                            'exception': <Any exception raised by the step> or None,
                            'io': <List of messages sent to and from the step> or [],
                            'output': <List of output messages sent from the step> or [],
                        },
                        ...
                    }
    """
    serialized_step_data = {}
    for step_id, step_data in context['step_data'].items():
        io = list(read_messages(step_data['rw_connection'], timeout=timeout)) \
            if 'rw_connection' in step_data else []
        output = list(read_messages(step_data['ro_connection'], timeout=timeout)) \
            if 'ro_connection' in step_data else []
        step_data.setdefault('io', []).extend(io)
        step_data.setdefault('output', []).extend(output)

        serialized_step_data[step_id] = {
            'return_value': step_data.get('return_value'),
            'exception': step_data.get('exception'),
            'io': step_data['io'],
            'output': step_data['output']
        }
    return serialized_step_data


def make_context_serializer(context):
    """Factory to make a Serializer object for the step data in the context.

    :param context: A dictionary of the following form:
                    {
                        'step_data': {},
                        # Any custom key-value pairs.
                        ...
                    }
    :return:        A Serializer object that will serialize the 'step_data' attribute in the context. All other
                    keys will be ignored.
    """
    return Serializer([SerializerCallable(context, key='step_data', serializer_function=serialize_step_data)])
