"""Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.

In this elaborate example, we see how most development with AutoTrail looks like.
Some of the topics covered are:
1. Writing standalone functions and how their signature is retained when used with AutoTrail.
2. Conditional branching in a workflow.
3. Retry on some exceptions and fail on others.
4. Clean-up code execution on certain exceptions.
5. Mixing interactive and non-interactive client usage.
6. Sharing data between steps using a threadsafe context object.

In this example, we define and run the following workflow:
                 +--> decide to be lazy --> decide how much to sleep --> sleep happily -->+
                 |                                                                        |
start the day -->|                                                                        +--> end the day
                 |                                                                        |
                 +--> decide to work hard ----------------------------> express anger---->+

At the start of the day, we need the end-user to decide whether the workflow is going to be lazy or work hard.
This is conditional branching. The branching is done by the user providing a choice when running the script using the
--lazy or --hardwork switches.

Case when the workflow is lazy:
1. One step decides how much to sleep based on the factor provided by the user when running the script (using the
   --factor CLI switch). This decision is then written into the context object.
2. The sleep step reads the sleep duration from the context and sleeps. It then expresses satisfaction using its
   return value, which is written into the context.

Case when the workflow is hardwork:
1. One step decides to be excited about working hard and self-inspires.
2. Another step randomly choses various levels of anger. It then expresses its level of anger into the context.

At the end of the day, irrespective of the branch chosen, the last step prints what was expressed by the branch. In the
case of being lazy, satisfied and in the case of working hard, some level of anger.
"""


import logging
import os
import random

from time import sleep
from argparse import ArgumentParser

from autotrail import TrailServer, TrailClient, Step, interactive, StatusField
from autotrail.helpers.context import threadsafe_class
from autotrail.helpers.io import InteractiveTrail
from autotrail.helpers.step import accepts_nothing, create_conditional_step, extraction_wrapper


logging.basicConfig(filename=os.path.join('/', 'tmp', 'example_trail.log'), level=logging.DEBUG)


# Context definitions
# AutoTrail does not care what object is passed as a context. If there is no context, then None is used.
# There are no specific subclass or behaviour requirements for the context object.
# It is all dependent on the workflow and how the developer wants to design it.
#
# This context object is threadsafe so that steps running in parallel can update the values using the getters and
# setters. One can also use manager.dict() and manager.list() data structures.
# threadsafe_class decorator makes is convenient to simply use getters and setters.
@threadsafe_class
class Context(object):
    def __init__(self, branch_choice, factor=0):
        self.branch_choice = branch_choice # The branch choice provided by the end-user.
        self.factor = factor # The factor that decides how much sleep to get.
        self.delay = 0 # This is computed by the step that decides how much to sleep and populated by it.
        self.action = None # This is set by both the branches based on what they "feel".

    def get_branch_choice(self):
        return self.branch_choice

    def get_factor(self):
        return self.factor

    def set_delay(self, delay):
        self.delay = delay

    def get_delay(self):
        return self.delay

    def set_action(self, action):
        self.action = action

    def get_action(self):
        return self.action


# This is used like an enum instead of hard-coding strings, which render themselves to typos.
class BranchChoice(object):
    hardwork = 'Hard Work'
    lazy = 'Lazy'


# Exceptions
# A step has to either succeed or fail. When a step fails, its progeny will not get executed.
# A step fails by simply raising any exception. AutoTrail has no requirements on these exceptions.
# When choosing between branches of the above workflow, if one branch fails, then the step "end the day" cannot run
# because all its parents would not have succeeded.
# Therefore, to achieve branching, when a step fails, we can ask AutoTrail to skip all its progeny. Such steps
# behave like conditional steps. This is done by setting the attribute Step.skip_progeny_on_failure to True.
# A step in the branch we don't want to run can raise an exception of any type.
# We are using this exception as an example.
# We need to know before-hand, which steps are used for conditional branching and set the attribute.
# This is explained later in the example.
class BranchNotChosen(Exception):
    """Exception raised when a branch is not chosed to execute."""
    pass


# The step that expresses anger does so by raising exceptions.
# Some of these exceptions are retriable and others are not.
# Below are some exceptions that are raised by the step.
# How they are used to communicate what action to take will be explained later in this example.
class Angry(Exception):
    pass


class MildlyIrritated(Angry):
    pass


class Exasperated(Angry):
    pass


class Fuming(Angry):
    pass


# This function is used to retry on a given exception. It is a very simple linear retry.
def simply_retry(func, retry_on, max_tries=5, delay=1, *args, **kwargs):
    # Log the retry information if needed.
    count = 1
    while True:
        try:
            return func(*args, **kwargs)
        except retry_on:
            if count == 5:
                logging.info('Exception {} encountered more than {} times. Giving up!'.format(str(retry_on), max_tries))
                raise
            logging.info('Exception {} encountered. Will retry again after {} seconds.'.format(str(retry_on), delay))
            count += 1
        sleep(delay)


# When get_angry is called, it is going to raise the MildlyIrritated exception twice.
# If it is called again, it will either raise Exasperated or Fuming (by random choice).
# This is to express a retryable exception (MildlyIrritated) and non-retryable ones (Exasperated and Fuming).
# AutoTrail handles this transparently and expressing these ideas is very easy. It will be explained later when this
# function is wrapped into a Step.
ANGER_BUCKET = [MildlyIrritated('I am a bit irritated.')] * 2
def get_angry():
    if ANGER_BUCKET:
        raise ANGER_BUCKET.pop()
    raise random.choice([Exasperated('I am exasperated.'), Fuming('I am fuming!')])


# Step definitions
# Each action function (contained in a Step), accepts two parameters in the raw form: TrailEnvironment and context.
# 1. TrailEnvironment is passed by AutoTrail to aid communication with the end-user and any future feature additions
#    that we might want to be made available to steps. This is at a framework level.
# 2. context object as defined by the developer. This is specific to this workflow and we have defined it above
#    as the Context class. If this is not present, then None is passed explicitly.
#
# However, writing functions which don't need these but retain these parameters in their signature is ugly and
# counter-intuitive. @accepts_nothing is a decorator that takes care of this and as the name suggests, this function
# accepts neither paramter.
# Using such decorators allow the functions to retain a readable signature of the function.
@accepts_nothing
def starting_point():
    sleep(5)
    return 'Staring my day, (cursing the alarm)...'


# These are normal Python functions, which are converted into action functions and then contained in a Step.
# Look at the definitions of these functions: lazy, hardwork, sleep_wanted et., and they all look like normal Python
# functions that don't need to:
# 1. accept any special parameters
# 2. raise specific exceptions
# 3. subclassed from a specific class
# 4. overrride specific methods
# 5. follow some naming convention
# They can be just normal functions one would write in any Python program/library.

def lazy(branch_choice):
    if branch_choice != BranchChoice.lazy:
        raise BranchNotChosen('Decided not to be lazy. Sigh...')
    return 'I am going to be lazy! Hurr..zzzz.'


def hardwork(branch_choice):
    if branch_choice != BranchChoice.hardwork:
        raise BranchNotChosen('Decided to be lazy!')
    return 'I am pumped, inspired. I am going to work hard today!'


def sleep_wanted(factor):
    return 2*factor


def sleep_happily(sleep_time):
    sleep(sleep_time)
    if sleep_time > 5:
        return 'I have slept well.'
    else:
        return 'You woke me up too early, I am coming after you.'


def ending_point(action):
    return '[Here is how I feel] -- {}'.format(action)


# Now that we have the functions defined, we need to define action functions and wrap them in Steps.
# Below you will see the use of extraction_wrapper and create_conditional_step, two useful helpers that will allow
# us to easily convert the above functions into steps with behaviour that we want them to exhibit.


# Let us start with the most trivial case where we convert the starting_point function into a step.
start_the_day = Step(starting_point)
# If we had not used the @accepts_nothing decorator at the function defintion, we could have simply done:
# Step(accepts_nothing(starting_point))

# Now lets look at te "lazy" function.
# It has the following behaviour:
# 1. Accepts a string for branch choice.
# 2. If it matches BranchChoice.lazy, returns a string.
# 3. If it does not, then raises BranchNotChosen exception.
#
# We need to modify this behaviour to doing the following:
# 1. Accept TrailEnvironment and Context objects.
# 2. Extract the branch choice from the Context object using context.get_branch_choice method.
# 3. Call "lazy" function with the branch choice.
# 4. Use the return value as the return value of the step.
# 5. Use the exception raised as the return value of the step.
# 6. Since this is a conditional step, that decides whether this branch will be run or not, skip all progeny in case of
#    failure.
#
# We achieve this by a combination of create_conditional_step and extraction_wrapper.
# First we need to create an action function (that accepts TrailEnvironment, Context etc). This is done using
# extraction_wrapper.
be_lazy = create_conditional_step( # Make the Step conditional, i.e., skip progeny if the step fails.
    # extraction_wrapper is a very powerful function conversion wrapper. Please look-up the full documentation
    # for all the features it provides.
    extraction_wrapper(
        # The function that needs to be wrapped, i.e., converted into an action function. In this case it is:
        lazy,
        # Next we need to define all the parameter extractors, i.e., functions that will accept TrailEnvironment and
        # Context and return the kwargs required by the wrapped function (lazy). Since we are extracting only one
        # parameter and it can be expressed using a single lambda expression, we define our parameter extractors as
        # the below list:
        [
            # This extracts the value from the context using the Context.get_branch_choice function and returns a
            # dictionary.
            lambda trail_env, context: dict(branch_choice=context.get_branch_choice()),
        ]
        # The dictionary produced as a result of all the parameter extractors is passed as **kwargs to the function
        # lazy, in this case, it results in:
        # lazy(branch_choice=context.get_branch_choice())

        # By default, the return value of the function is the return value of the step. In this case, whatever string
        # the function lazy produces, becomes the return value of the step.
    ))

# The above example is replicated in the step definition below.
work_hard = create_conditional_step(extraction_wrapper(
    hardwork, [lambda trail_env, context: dict(branch_choice=context.get_branch_choice())]))


# The sleep_wanted function only returns the delay value based on the factor computation. We need to:
# 1. Show this as a result of the step (expressing how many seconds to sleep).
# 2. Write the value into the Context object to be used by a later step (sleep).
# This is done as follows:
decide_how_much_sleep = Step(extraction_wrapper(
    sleep_wanted,
    # Parameter extractors work just as explained with the "be_lazy" step above.
    [lambda trail_env, context: dict(factor=context.get_factor())],

    # Return handlers are a list of functions each accepting the wrapped function's return value and context and doing
    # something with it (handling them). In this case, we want to set a Context attribute using Context.set_delay.
    return_handlers=[lambda retval, context: context.set_delay(retval)],

    # Since there can be multiple return_handlers, we need a way to determine, what return value will be considered
    # the return value of the step. In this case, we produce a helpful message:
    return_value_extractor=lambda retval, context: 'Going to sleep {} seconds.'.format(retval)))


# The above example is replicated in the step definition below.
sleeping_happily = Step(extraction_wrapper(
    sleep_happily, [lambda trail_env, context: dict(sleep_time=context.get_delay())],
    return_handlers=[lambda retval, context: context.set_action(retval)]))


# These two functions are exception handlers, which are explained in the express_anger step definition below.
def exasperated(exception, context):
    context.set_action('I am really pissed.')
    return str(exception)


def fuming(exception, context):
    context.set_action('I need some anger management classes.')
    return str(exception)


# To express anger, we need to do the following:
# 1. Run the get_angry function using simply_retry, to retry on MildlyIrritated exception.
# 2. Catch the Exasperated and Fuming exceptions and call their respective handlers (which, in the real-world, could run
#    some clean-up code)
express_anger = Step(extraction_wrapper(
    # In this case the function being wrapped is not get_angry, but simply_retry.
    simply_retry,

    # We pass get_angry and MildlyIrritated as arguments to simply_retry.
    [lambda trail_env, context: dict(retry_on=MildlyIrritated, func=get_angry)],

    # Exception handlers are a list of tuples, pairing an exception with its handler function. In this case,
    # the functions exasperated and fuming are called, which:
    # 1. Accept the exception and the context.
    # 2. Do anything they want with it: They set the action to take using Context.set_action method.
    # 3. Return a value that will be the return value of the step: A string with the message of the exception.
    exception_handlers=[(Exasperated, exasperated), (Fuming, fuming)]),

    # By default, the Step will be named based on the function, which in this case is simply_retry. But the actual
    # function we are interested in is get_angry. This name can be explicitly passed, thereby, correctly naming the
    # step.
    name='get_angry')


end_the_day = Step(extraction_wrapper(
    ending_point, [lambda trail_env, context: dict(action=context.get_action())]),
    name='end_the_day')


# Trail definition
# The trail definition below represents the following DAG:
#                  +--> be_lazy --> decide_how_much_sleep --> sleeping_happily -->+
#                  |                                                              |
# start_the_day -->|                                                              +--> end_the_day
#                  |                                                              |
#                  +--> work_hard ----------------------------> express_anger---->+
#
my_day_trail_definition = [
    # The first element of the first tuple is the root of the DAG. In this case, "start_the_day" is the root.

    # Branch choices
    (start_the_day, be_lazy),
    (start_the_day, work_hard),

    # Lazy branch
    (be_lazy, decide_how_much_sleep),
    (decide_how_much_sleep, sleeping_happily),
    (sleeping_happily, end_the_day),

    # Hard work branch
    (work_hard, express_anger),
    (express_anger, end_the_day),
]


# Parse CLI args
parser = ArgumentParser(description='An elaborate example of AutoTrail development')
parser.add_argument('--factor', type=int, default=3, help='The factor to use for computing sleep time.')
branch_selector = parser.add_mutually_exclusive_group(required=True)
branch_selector.add_argument('--lazy', dest='branch_choice', action='store_const', const=BranchChoice.lazy)
branch_selector.add_argument('--hardwork', dest='branch_choice', action='store_const', const=BranchChoice.hardwork)
args = parser.parse_args()


# Setup trail server
context = Context(args.branch_choice, factor=args.factor)
day_trail_server = TrailServer(my_day_trail_definition, delay=0.5, context=context)
# Since we will be interacting (using a client) with the trail in the same script, we need to run it as a separate
# process.
day_trail_server.serve(threaded=True)


# Here we use 2 types of clients to interact with the trail.
# 1. A programmatic client, which returns dictionaries/lists containing API results.
day_trail_client = TrailClient(day_trail_server.socket_file)

# 2. An interactive client, which prints the API results to STDOUT.
#    This client can easily be made to write the API results to a file or any other stream. Please read the
#    documentation of the class to know the full functionality.
day_trail_interactive_client = InteractiveTrail(day_trail_client)


# To start an interactive session, uncomment the following:
# interactive_namespace = {
#     'server': day_trail_server,
#     'client': day_trail_client,
#     'day': day_trail_interactive_client,
# }
# interactive(interactive_namespace, day_trail_client, prompt='My Day>')
# Now, the end-user can interact with the trail using the methods of the InteractiveTrail class.


# For this example, we will automatically be printing the trail status until the last step finishes.
# With this function, we use the programmatic client to query the status and decide if the trail has finished or not.
def is_step_done(client, step_name):
    """Query the trail status using the client and return True if step_name has completed.

    Arguments:
    client    -- A TrailClient or similar object.
    step_name -- The 'name' tag of the step to check for completion.

    Returns:
    True      -- if the step has succeeded.
    False     -- otherwise.
    """
    # To understand the structure of the result returned by the API calls, please see the documentation of the
    # TrailClient class.
    statuses = client.status(fields=[StatusField.STATE], name=step_name)

    # In this case, the status call returns a list of step statuses.
    # Since we have exactly one step with each name and we are querying the status of steps with the given name,
    # there will be only one element in the result list. Hence we refer to the zeroth element of results.
    if statuses and statuses[0][StatusField.STATE] == Step.SUCCESS:
        return True
    return False


# Run the trail
# Since this is an interactive client, it will print the result of the API call to STDOUT.
day_trail_interactive_client.start()
print '\n\n'


# Print the status every 2 seconds while the trail is running.
while not is_step_done(day_trail_client, 'end_the_day'):
    print '=========================== Status of the trail ==========================='
    day_trail_interactive_client.status()
    print '===========================================================================\n\n'
    sleep(2)


# We have reached the end state we want and we need to shutdown the trail server.
day_trail_client.shutdown(dry_run=False)
print '[Example Trail] -- Shutdown the trail server. Example ends.'
