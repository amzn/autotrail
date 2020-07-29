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
import logging

from multiprocessing import Process


logger = logging.getLogger(__name__)


def parse_definitions(definitions):
    """Extract machine rules and initial states from the given machine definitions.

    :param definitions: Definitions are state machine definitions with the following structure:
                        {
                            <Machine name>: (<Initial State>, {<From state>: {<Action>: (<To State>, [<Precondition 1>,
                                                                                                      <Precondition 2>,
                                                                                                      ...])
                                                                              ...
                                                                             },
                                                              ...
                                                              },
                                            )
                            ...
                        }
                        Where:
                            <Machine name> is a string representing a machine.
                            <Initial State> is a string representing the initial state of the associated machine.
                            <From state> is a string representing the state from which a transition may happen.
                            <Action> is a string representing an action that may be performed to make a state
                            transition.
                            <To State> is a string representing the end state of an associated action.
                            <Precondition 1>, <Precondition 2>, etc., are mappings of the form:
                                {
                                    <Machine name>: [<State 1>, <State 2>, ...],
                                }
                                Where: <State 1>, <State 2>, etc., are strings representing the various states the
                                       corresponding machine needs to be in.
                                Which is considered satisfied if and only if all the machines are in one of their
                                associated states.

                        Each machine and its associated rules are independently defined. The states and actions
                        available to one machine may not be apply to another in the given definition. Each state and
                        action mentioned in this documentation is specific to its associated machine.
    :return:            A tuple of 2 mappings, viz., (<rules>, <states>) where:
                        <rules> is a mapping with the following structure:
                            {
                                <Machine name>: {<From state>: {<Action>: (<To State>, [<Precondition 1>,
                                                                                        <Precondition 2>,
                                                                                        ...])
                                                                ...
                                                               },
                                                ...
                                                },
                                ...
                            }
                            Where:
                                <Machine name> is a string representing a machine.
                                <From state> is a string representing the state from which a transition may happen.
                                <Action> is a string representing an action that may be performed to make a state
                                transition.
                                <To State> is a string representing the end state of an associated action.
                                <Precondition 1>, <Precondition 2>, etc., are mappings of the form:
                                    {
                                        <Machine name>: [<State 1>, <State 2>, ...],
                                    }
                                    Where: <State 1>, <State 2>, etc., are strings representing the various states the
                                           corresponding machine needs to be in.
                                    Which is considered satisfied if and only if all the machines are in one of their
                                    associated states.

                            Each machine and its associated rules are independently defined. The states and actions
                            available to one machine may not be apply to another in the given definition. Each state and
                            action mentioned in this documentation is specific to its associated machine.
                        <states> is a mapping representing the initial states of all the machines in the following form:
                            {
                                <Machine 1 name (str)>: <Machine 1 state (str)>,
                                ...
                            }
    """
    rules = {}
    states = {}
    for name, (state, definition) in definitions.items():
        rules[name] = definition
        states[name] = state
    return rules, states


def is_precondition_satisfied(states, precondition):
    """Check if the given precondition is satisfied.

    A precondition is considered satisfied if and only if all the machines are in one of their associated states.

    :param states:          A mapping representing the states of all the machines in the following form:
                            {
                                <Machine 1 name (str)>: <Machine 1 state (str)>,
                                ...
                            }
    :param precondition:    A precondition is a mapping of the form:
                            {
                                <Machine 1 name (str)>: [<State 1 (str)>, <State 2 (str)>, ...],
                                ...
                            }
                            Where: <State 1>, <State 2>, etc., are strings representing the various states the
                                   corresponding machine needs to be in.
    :return:                True if all the machines are in one of their associated states. False otherwise.
    """
    return all(states[name] in expected_states for name, expected_states in precondition.items())


def any_precondition_satisfied(states, preconditions):
    """Check if any given precondition is satisfied.

    A precondition is considered satisfied if and only if all the machines are in one of their associated states.

    :param states:          A mapping representing the states of all the machines in the following form:
                            {
                                <Machine 1 name (str)>: <Machine 1 state (str)>,
                                ...
                            }
    :param preconditions:   An iterable of preconditions, each of which is a mapping of the form:
                            {
                                <Machine 1 name (str)>: [<State 1 (str)>, <State 2 (str)>, ...],
                                ...
                            }
                            Where: <State 1>, <State 2>, etc., are strings representing the various states the
                                   corresponding machine needs to be in.
    :return:                True if all the machines are in any one of the preconditions are their associated states.
                            False otherwise.
    """
    return any(is_precondition_satisfied(states, precondition)
               for precondition in preconditions) if preconditions else True


def transition(transitions, actions):
    """Produce the end states of states based on the given actions and available transitions.

    :param transitions: A mapping representing the available actions for each machine in the form:
                        {
                            <Machine 1>: {
                                <Action 1 (str)>: <To State>,
                                ...
                            }
                            ...
                        }
                        Where:
                            <Machine 1> is a string representing a machine name.
                            <Action> is a string representing an action that may be performed to make a state
                            transition.
                            <To State> is a string representing the end state of an associated action.
    :param actions:     The actions to perform on each machine represented by the following mapping:
                        {
                            <Machine 1>: <Action 1>,
                            ...
                        }
    :return:            The end states determined using the given transitions mapping structured as follows:
                        {
                            <Machine 1 name (str)>: <Machine 1 state (str)>,
                            ...
                        }
    """
    return {name: transitions[name][action]
            for name, action in actions.items()
            if action in transitions.get(name, {})}


def determine_transitions(rules, states, name):
    """Determine the available transitions a given machine based on the provided rules.

    :param rules:           A mapping with the following structure:
                            {
                                <Machine name>: {<From state>: {<Action>: (<To State>, [<Precondition 1>,
                                                                                        <Precondition 2>,
                                                                                        ...])
                                                               ...
                                                               },
                                                ...
                                                },
                                ...
                            }
                            Where:
                                <Machine name> is a string representing a machine.
                                <From state> is a string representing the state from which a transition may happen.
                                <Action> is a string representing an action that may be performed to make a state
                                transition.
                                <To State> is a string representing the end state of an associated action.
                                <Precondition 1>, <Precondition 2>, etc., are mappings of the form:
                                    {
                                        <Machine name>: [<State 1>, <State 2>, ...],
                                    }
                                    Where: <State 1>, <State 2>, etc., are strings representing the various states the
                                           corresponding machine needs to be in.
                                    Which is considered satisfied if and only if all the machines are in one of their
                                    associated states.

                            Each machine and its associated rules are independently defined. The states and actions
                            available to one machine may not be apply to another in the given definition. Each state and
                            action mentioned in this documentation is specific to its associated machine.
    :param states:          A mapping representing the states of all the machines in the following form:
                            {
                                <Machine 1 name (str)>: <Machine 1 state (str)>,
                                ...
                            }
    :param name:            Machine name (str).
    :return:                A mapping representing the available actions in the form:
                            {
                                <Action 1 (str)>: <To State>,
                                ...
                            }
                            Where:
                                <Action> is a string representing an action that may be performed to make a state
                                transition.
                                <To State> is a string representing the end state of an associated action.
    """
    rules_for_current_state = rules[name].get(states[name], {})
    return {action: to_state
            for action, (to_state, preconditions) in rules_for_current_state.items()
            if any_precondition_satisfied(states, preconditions)}


def state_machine_evaluator(definitions, callback):
    """Evaluates the state machines following the given rules calling the given callback function in each iteration.

    The evaluation is done as follows:
    1. The machines start in their evaluations in their <Initial State>.
    2. The actions associated with each machine's state is determined.
    3. For each machine, the preconditions for the actions are evaluated. An action is eligible for execution if any
       one of the associated preconditions is satisfied.
    4. The callback is invoked by passing the current states and available transitions.

    :param definitions: Definitions are state machine definitions with the following structure:
                        {
                            <Machine name>: (<Initial State>, {<From state>: {<Action>: (<To State>, [<Precondition 1>,
                                                                                                      <Precondition 2>,
                                                                                                      ...])
                                                                             ...
                                                                             },
                                                              ...
                                                              },
                                            )
                            ...
                        }
                        Where:
                            <Machine name> is a string representing a machine.
                            <Initial State> is a string representing the initial state of the associated machine.
                            <From state> is a string representing the state from which a transition may happen.
                            <Action> is a string representing an action that may be performed to make a state
                            transition.
                            <To State> is a string representing the end state of an associated action.
                            <Precondition 1>, <Precondition 2>, etc., are mappings of the form:
                                {
                                    <Machine name>: [<State 1>, <State 2>, ...],
                                }
                                Where: <State 1>, <State 2>, etc., are strings representing the various states the
                                       corresponding machine needs to be in.
                                Which is considered satisfied if and only if all the machines are in one of their
                                associated states.

                        Each machine and its associated rules are independently defined. The states and actions
                        available to one machine may not be apply to another in the given definition. Each state and
                        action mentioned in this documentation is specific to its associated machine.

    :param callback:    A callable that accepts (states, transitions) as the only parameters and return actions where:
                        states is a mapping representing the states of all the machines using the following form:
                            {
                                <Machine 1 name (str)>: <Machine 1 state (str)>,
                                ...
                            }
                        transitions is a mapping representing the available actions on all the machines using the
                        following form:
                            {
                                <Machine 1 name (str)>: [<Action 1>, ...],
                                ...
                            }
                        actions is the a mapping returned by the callback with all the actions to execute on the
                        machines (i.e., perform state transitions according to the rules). It is of the following form:
                            {
                                <Machine 1 name (str)>: <Action 1>,
                                ...
                            }
                            If the returned action is not part of the available actions, it will have no effect. The
                            integrity of the state machines will not be compromised by injecting arbitrary actions.
    :return:            None. The evaluator returns when there are no possible actions for any of the machines (no state
                        transitions possible).
                        Any exception (Exception) raised by the callback function will be logged and re-raised.
    """
    rules, states = parse_definitions(definitions)
    while True:
        transitions = {name: determine_transitions(rules, states, name) for name in states}

        try:
            actions = callback(states, transitions)
        except Exception as e:
            logger.exception('Callback failed with error: {}'.format(e))
            raise

        if not any(transitions.values()):
            break

        states.update(transition(transitions, actions))


def run_state_machine_evaluator(state_machine_definitions, callback):
    """Run the state_machine_evaluator as a multiprocessing.Process."""
    return Process(target=state_machine_evaluator,
                   args=(state_machine_definitions, callback))
