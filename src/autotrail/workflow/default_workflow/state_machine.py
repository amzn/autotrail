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
from autotrail.core.api.management import APIHandlerResponse


class State:
    """Namespace for the states of the machines."""
    READY = 'Ready'
    WAITING = 'Waiting'
    TOSKIP = 'To Skip'
    SKIPPED = 'Skipped'
    PAUSED = 'Paused'
    RUNNING = 'Running'
    INTERRUPTED = 'Interrupted'
    SUCCEEDED = 'Successful'
    FAILED = 'Failed'
    ERROR = 'Error'


class Action:
    """Namespace for the actions available."""
    START = 'Start'
    RUN = 'Run'
    SUCCEED = 'Succeed'
    FAIL = 'Fail'
    ERROR = 'Error'
    PAUSE = 'Pause'
    INTERRUPT = 'Interrupt'
    RESUME = 'Resume'
    RERUN = 'Re-run'
    MARKSKIP = 'Mark to skip'
    SKIP = 'Skip'
    UNSKIP = 'Unskip'


class StepFailed(Exception):
    """Exception raised when a Step fails irrecoverably."""
    pass


TRANSITION_RULES = {
    # The rules for state transitions as a mapping of the following form:
    #     {
    #         <State 1 (str)>: {
    #             <Action 1 (str)>: <State 2 (str)>,
    #             ...
    #         },
    #         ...
    #     }
    # The above snippet defines the rule that if a Step (machine) is in "State 1", "Action 1" can be performed on it.
    # If "Action 1" is performed, then the machine transitions to "State 2".
    State.READY: {
        Action.START: State.WAITING,
        Action.PAUSE: State.PAUSED,
        Action.MARKSKIP: State.TOSKIP},
    State.WAITING: {
        Action.RUN: State.RUNNING,
        Action.PAUSE: State.PAUSED,
        Action.MARKSKIP: State.TOSKIP},
    State.TOSKIP: {
        Action.UNSKIP: State.WAITING,
        Action.SKIP: State.SKIPPED},
    State.PAUSED: {
        Action.RESUME: State.WAITING,
        Action.MARKSKIP: State.TOSKIP},
    State.ERROR: {
        Action.RERUN: State.WAITING,
        Action.MARKSKIP: State.TOSKIP},
    State.RUNNING: {
        Action.SUCCEED: State.SUCCEEDED,
        Action.FAIL: State.FAILED,
        Action.INTERRUPT: State.INTERRUPTED,
        Action.ERROR: State.ERROR},
    State.INTERRUPTED: {
        Action.RESUME: State.WAITING,
        Action.MARKSKIP: State.TOSKIP}}


def generate_preconditions(ordered_pairs, failure=False):
    """Factory to automatically generate the default pre-conditions (dictionary) for given ordered pairs of steps.

    :param ordered_pairs:   A list of ordered pairs like [(a, b)] where 'a' and 'b' have an 'id' attribute.
    :param failure:         Boolean controlling the preconditions as explained below.
    :return:                A list of ordered pairs like [(a, b)] (with failure=False) will return the following
                            preconditions:
                            {
                                <b.id>: {
                                    <a.id>: [State.SUCCEEDED, State.SKIPPED],
                                },
                            }
                            Since failure=False, it means that 'b' should be executed only if 'a' is successful.
                            However, 'b' may be executed by skipping 'a'. This is expressed in the above pre-condition.

                            Similarly, the list of ordered pairs like [(a, b)] (with failure=True) will return the
                            following preconditions:
                            {
                                <b.id>: {
                                    <a.id>: [State.FAILED],
                                },
                            }
                            Since failure=True, it means that 'b' should be executed only if 'a' has failed. Therefore,
                            'b' can't be executed by skipping 'a'. This is expressed in the above pre-condition.
                            All states are from the State namespace class.
    """
    cumulative_preconditions = {}
    states = [State.FAILED] if failure else [State.SUCCEEDED, State.SKIPPED]
    for step, linked_step in ordered_pairs:
        cumulative_preconditions.setdefault(linked_step.id, {}).update(
            {step.id: states})
        if not failure:
            cumulative_preconditions[linked_step.id].update(cumulative_preconditions.setdefault(step.id, {}))
    return cumulative_preconditions


def generate_machine_definitions(initial_state, preconditions, transition_rules):
    """Factory to generate the default state machine definitions (dictionary).

    :param initial_state:       A string from the State namespace representing the initial state of the associated
                                machine.
    :param preconditions:       A mapping of the form:
                                {
                                    <Machine name>: [<State 1>, <State 2>, ...],
                                }
                                Where, <State 1>, <State 2>, etc., are from the State namespace representing the various
                                    states the corresponding step needs to be in.
                                This is considered satisfied if and only if all the steps are in one of their associated
                                states.
                                The preconditions are applied *only* to the following state transitions:
                                    From State.WAITING under Action.RUN
                                    From State.TOSKIP under Action.SKIP
                                This is because all user-intervened actions can happen only when the step is waiting or
                                has not been skipped.
    :param transition_rules:    A mapping of the form:
                                {
                                    <State 1 (from the State namespace)>: {
                                        <Action 1 (from the Action namespace)>: <State 2 (from the State namespace)>,
                                        ...
                                    },
                                    ...
                                }
                                The above snippet defines the rule that if a Step (machine) is in "State 1", "Action 1"
                                can be performed on it.
                                If "Action 1" is performed, then the machine transitions to "State 2".
    :return:                    The above parameters are transformed into the state machine definitions data structure
                                whose structure is shown below:
                                {
                                    <Name>: (<State>, {<From>: {<Action>: (<To>, [<Precondition 1>,
                                                                                  <Precondition 2>,
                                                                                  ...]
                                                                           )
                                                              },
                                                      },
                                            )
                                }

                                Where:
                                    <Name> is a string representing a machine. In this case it is the ID of the step.
                                    <State> is from the State namespace representing the initial state of the associated
                                        machine.
                                    <From> is from the State namespace representing the state from which a transition
                                        may happen.
                                    <Action> is from the Action namespace representing an action that may be performed
                                        to make a state transition.
                                    <To> is from the State namespace representing the end state of an associated action.
                                    <Precondition 1>, <Precondition 2>, etc., are mappings of the form:
                                        {
                                            <Machine name>: [<State 1>, <State 2>, ...],
                                        }
                                        Where: <State 1>, <State 2>, etc., are from the State namespace representing the
                                            various states the corresponding machine needs to be in.
                                        Which is considered satisfied if and only if all the machines are in one of
                                            their associated states.
    """
    state_machine_definitions = {}
    for step_id, step_precondition in preconditions.items():
        step_transition_rules = {}
        for from_state, action_to_state_mapping in transition_rules.items():
            step_transition_rules[from_state] = {}
            for action, to_state in action_to_state_mapping.items():
                if step_precondition and ((from_state == State.WAITING and action == Action.RUN) or
                                          (from_state == State.TOSKIP and action == Action.SKIP)):
                    step_preconditions = [step_precondition]
                else:
                    step_preconditions = []

                step_transition_rules[from_state][action] = (to_state, step_preconditions)
        state_machine_definitions[step_id] = (initial_state, step_transition_rules)
    return state_machine_definitions


def make_state_machine_definitions(success_pairs, failure_pairs, transition_rules=None, initial_state=State.READY):
    """Factory to generate the default state machine definitions (dictionary) from the given ordered pairs.

    :param success_pairs:       A list of tuples (ordered pairs) of steps that is followed in the case of successful
                                execution. E.g., [(a, b)] means that 'b' will be executed only if 'a' is successful.
    :param failure_pairs:       A list of tuples (ordered pairs) of steps that is followed in the case of failed
                                execution. E.g., [(a, b)] means that 'b' will be executed only if 'a' has failed.
    :param transition_rules:    A mapping of the form:
                                {
                                    <State 1 (from the State namespace)>: {
                                        <Action 1 (from the Action namespace)>: <State 2 (from the State namespace)>,
                                        ...
                                    },
                                    ...
                                }
                                The above snippet defines the rule that if a Step (machine) is in "State 1", "Action 1"
                                can be performed on it.
                                If "Action 1" is performed, then the machine transitions to "State 2".
    :param initial_state:       A string from the State namespace representing the initial state of the associated
                                machine.
    :return:                    The above parameters are transformed into the state machine definitions data structure
                                whose structure is shown below:
                                {
                                    <Name>: (<State>, {<From>: {<Action>: (<To>, [<Precondition 1>,
                                                                                  <Precondition 2>,
                                                                                  ...]
                                                                           )
                                                              },
                                                      },
                                            )
                                }

                                Where:
                                    <Name> is a string representing a machine. In this case it is the ID of the step.
                                    <State> is from the State namespace representing the initial state of the associated
                                        machine.
                                    <From> is from the State namespace representing the state from which a transition
                                        may happen.
                                    <Action> is from the Action namespace representing an action that may be performed
                                        to make a state transition.
                                    <To> is from the State namespace representing the end state of an associated action.
                                    <Precondition 1>, <Precondition 2>, etc., are mappings of the form:
                                        {
                                            <Machine name>: [<State 1>, <State 2>, ...],
                                        }
                                        Where: <State 1>, <State 2>, etc., are from the State namespace representing the
                                            various states the corresponding machine needs to be in.
                                        Which is considered satisfied if and only if all the machines are in one of
                                            their associated states.

                                A list of ordered pairs like [(a, b)] (with failure=False) will produce the following
                                preconditions:
                                {
                                    <b.id>: {
                                        <a.id>: [State.SUCCEEDED, State.SKIPPED],
                                    },
                                }
                                Since failure=False, it means that 'b' should be executed only if 'a' is successful.
                                However, 'b' may be executed by skipping 'a'. This is expressed in the above
                                pre-condition.

                                Similarly, the list of ordered pairs like [(a, b)] (with failure=True) will produce the
                                following preconditions:
                                {
                                    <b.id>: {
                                        <a.id>: [State.FAILED],
                                    },
                                }
                                Since failure=True, it means that 'b' should be executed only if 'a' has failed.
                                Therefore, 'b' can't be executed by skipping 'a'. This is expressed in the above
                                pre-condition.

                                The preconditions are applied *only* to the following state transitions:
                                    From State.WAITING under Action.RUN
                                    From State.TOSKIP under Action.SKIP
                                This is because all user-intervened actions can happen only when the step is waiting or
                                has not been skipped.

                                All states are from the State namespace class.
    """
    transition_rules = transition_rules or TRANSITION_RULES

    preconditions = generate_preconditions(success_pairs)
    preconditions.update(generate_preconditions(failure_pairs, failure=True))

    return generate_machine_definitions(initial_state, preconditions, transition_rules)


class AutomaticActions:
    """Namespace for unbound methods that run automatic actions on steps.

    Each method must:
    1. Be decorated with @staticmethod.
    2. Accept (step, context) as parameters.
    3. May return any type, but it needs to be compliant with the action evaluations defined to be useful.
    """
    @staticmethod
    def check_step(step, context):
        """Check a step.

        :param step:    A step object
        :param context: The context dictionary.
        :return:        Returns 'running' (str) if the step process is running.
                        Returns 'success' (str) if the step has finished and updates the context mapping's 'step_data'
                        key as follows:
                            {
                                <step id>: {
                                    'return_value': <The value returned by the step execution> or None,
                                    'exception': <The exception raised by the step> or None,
                                }
                            }

                        If the step raised the StepFailed exception, the string 'failure' will be returned.
                        If the step raised any other exception, the string 'tempfail' will be returned.
        """
        result = step.get_result()
        if result is None:
            return 'running'

        step_data = context['step_data'].setdefault(step.id, {})
        return_value, exception = result
        step_data['return_value'] = return_value
        step_data['exception'] = exception

        if exception is None:
            return 'success'
        elif isinstance(exception, StepFailed):
            return 'failure'
        else:
            return 'tempfail'

    @staticmethod
    def start(step, context):
        """Execute a step by passing it the context.

        :param step:    A step object
        :param context: The context dictionary.
        :return:        None
        """
        step.start(context)

    @staticmethod
    def noop(step, context):
        """Does nothing. To be used when no action is needed but a state transition is.

        :param step:    A step object
        :param context: The context dictionary.
        :return:        None
        """
        pass


ACTION_EVALUATIONS = {
    # Data structure representing the rules for performing automated actions on the machines.
    # The structure is as follows:
    #     {
    #         <Action 1 for Machine 1(str)>: (<callable>, <successful return value>),
    #         ...
    #     }
    # Where, for a step (machine), if an action is available, its corresponding callable will be called with the
    # step and context objects.
    # If this callable returns the specified 'successful return value', then the action will be considered to have taken
    # place on the machine and the state will be transitioned accordingly.

    # Action            Function/worker                 Successful action
    Action.RUN:         (AutomaticActions.start,        None),
    Action.SUCCEED:     (AutomaticActions.check_step,   'success'),
    Action.FAIL:        (AutomaticActions.check_step,   'failure'),
    Action.ERROR:       (AutomaticActions.check_step,   'tempfail'),
    Action.SKIP:        (AutomaticActions.noop,         None)}


class APIHandlers:
    """API handlers to make modifications to machines (Steps).

    Each public method is an API handler function that must
    1. Accept (states, transitions) as the first 2 parameters.
       This is followed by any *args and **kwargs they may require.
       Where:
       states is a dictionary of the form:
            {
                <machine 1 name (str)>: <state of machine 1 (str)>,
                ...
            }
       transitions is a dictionary of the form:
            {
                <machine 1 name (str)>: [<possible action for machine 1 (str)>,
                                         <possible action for machine 2 (str)>,
                                         ...],
                ...
            }
    2. They must return an APIHandlerResponse object containing the following:
        return_value:   The result returned to the client making the API call.
        exception:      Any exception message to the client making the API call.
        relay_value:    This must be a mapping of the form: {<Machine name>: <Action to take>}
                        As a result of this, the machine will undergo the associated action.
    """
    def __init__(self, step_id_to_object_mapping, context):
        """Initialize the parameters available to all API handlers.

        :param step_id_to_object_mapping:   A mapping of the form:
                                            {
                                              <Step 1 ID>: <Step 1 object>,
                                              ...
                                            }
        :param context:                     The context dictionary.
        """
        self._context = context
        self._step_id_to_object_mapping = step_id_to_object_mapping

    def interrupt(self, states, transitions, step_ids):
        """API method that will interrupt a running step.

        :param states:                      A mapping of the form:
                                            {
                                                <machine 1 name (str)>: <state of machine 1 (str)>,
                                                ...
                                            }
        :param transitions:                 A mapping of the form:
                                            {
                                                <machine 1 name (str)>: [<possible action for machine 1 (str)>,
                                                                         <possible action for machine 2 (str)>,
                                                                         ...],
                                                ...
                                            }
        :param step_ids:                    An iterable of step IDs to be interrupted.
        :return:                            A tuple of the form: (<API result>, <Actions>), where;
                                            <API result> is the result returned to the client making the API call.
                                            <Actions> is a mapping of the form: {<Machine name>: <Action to take>}
        """
        result = {}
        actions = {}
        for step_id in step_ids:
            if Action.INTERRUPT in transitions[step_id]:
                self._step_id_to_object_mapping[step_id].terminate()
                result[step_id] = True
                actions[step_id] = Action.INTERRUPT
            else:
                result[step_id] = False
        return APIHandlerResponse(result, relay_value=actions)

    def send_messages(self, states, transitions, step_message_mapping):
        """API method to send messages to steps.

        This method has no affect on the state machine.

        :param states:                      A mapping of the form:
                                            {
                                                <machine 1 name (str)>: <state of machine 1 (str)>,
                                                ...
                                            }
        :param transitions:                 A mapping of the form:
                                            {
                                                <machine 1 name (str)>: [<possible action for machine 1 (str)>,
                                                                         <possible action for machine 2 (str)>,
                                                                         ...],
                                                ...
                                            }
        :param step_message_mapping:        A mapping of the form:
                                            {
                                                <Step ID>: <Message for the step>,
                                                ...
                                            }
        :return:                            A tuple of the form: (<API result>, {}), where;
                                            <API result> is the result returned to the client making the API call.
                                            {} represents no action to taken on the state machine.
        """
        result = []
        for step_id, message in step_message_mapping.items():
            try:
                self._context['step_data'][step_id]['rw_connection'].send(message)
                result.append(step_id)
            except (KeyError, AttributeError):
                pass
        return APIHandlerResponse(result, relay_value={})
