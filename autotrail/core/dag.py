"""Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.

Core Data structures and algorithms for AutoTrail:

Data Structures:
1) Vertex -- Used to represent DAGs. This is a base class.
2) Step   -- Used to represent steps in a trail. This extends Vertex.
3) Stack  -- A simple stack using lists. Used for depth first traversal.
4) Queue  -- A simple queue using deque. Used for breadth first traversal.

Algorithms:
1) Depth first traversal
2) Breadth first traversal
3) Topological while
4) Topological traverse
5) Acyclicity check for a DAG.
"""


from collections import deque


class Vertex(object):
    """ Represents a Vertex in the DAG.
    This is meant to be a base class.
    To make any class a vertex, simply inherit from Vertex, no additional/special member functions are needed.
    """
    def __init__(self):
        self.children = []
        self.parents = []

    def add_child(self, child_vertex):
        """Adds the given vertex as a child.

        Arguments:
        child_vertex : Add given vertex as a child vertex.
        """
        self.children.append(child_vertex)
        child_vertex.parents.append(self)

    def add_parent(self, parent_vertex):
        """Adds the given vertex as a parent.

        Arguments:
        parent_vertex : Add given vertex as a parent vertex
        """
        self.parents.append(parent_vertex)
        parent_vertex.children.append(self)


class Step(Vertex):
    """Step type used in AutoTrail.

    A Step is a container that contains the following:
    Action function -- An object/function representing the smallest unit of work to be ordered in the trail.
    Tags            -- Non-unique key-value pairs that help the dev-user and end-user in classifying multiple steps
                       into a single category.
                       These key-value pairs can be arbitrary.
                       There are two tags used to identify a step:
                       {
                        'name': If this is not supplied, it is computed based on the function name.
                                This can be Non-unique as multiple steps with the same function can have the same name.
                        'n': This is a number starting at 0 and increasing in topological order. This is set by the
                             framework and is guaranteed to be unique.
                       }
    State           -- that represents the status of a step.

    The other attributes viz., process, return_value and queue are not defined or used by the user but by the various
    functions that work on Step.
    """
    # Automatically managed states - These states are never controlled by the user.
    READY = 'Ready'
    RUN = 'Running'
    SUCCESS = 'Succeeded'
    FAILURE = 'Failed'
    SKIPPED = 'Skipped'
    BLOCKED = 'Blocked'
    PAUSED = 'Paused'
    INTERRUPTED = 'Interrupted'
    PAUSED_ON_FAIL = 'Paused due to failure'

    # Steps mananged by API calls. Users can put a step in these states using API calls.
    WAIT = 'Waiting'
    TOSKIP = 'To Skip'
    TOBLOCK = 'To Block'
    TOPAUSE = 'To Pause'

    def __init__(self, action_function, **tags):
        super(Step, self).__init__()

        # This is the action function that will be executed when the Step is run.
        self.action_function = action_function

        # All automatic and user-defined tags are stored in this attribute.
        # 'n' is a guaranteed unique attribute ie., no two Steps will have the same value for the 'n' tag.
        self.tags = tags

        # Automaticaly assign a name if none was explicitly provided by the user.
        if 'name' not in tags:
            self.tags['name'] = self._make_name()

        # The default state every Step is in.
        self.state = self.READY

        # This refers to the multiprocessing.Process object that runs the action function
        # (since it will be run as a sub-process)
        self.process = None

        # This stores the return value of the action function.
        self.return_value = None

        # This is a multiprocessing.Queue like object that is used by the sub-process running the action function to
        # write the result into. This result will be collected and self.return_value will be updated.
        self.result_queue = None

        # The multiprocessing.Queue like object that is used as an input source by the action function via the
        # TrailEnvironment.
        self.input_queue = None

        # The multiprocessing.Queue like object that is used as an output medium by the action function via the
        # TrailEnvironment.
        self.output_queue = None

        # The multiprocessing.Queue like object that is used as an output medium by the action function via the
        # TrailEnvironment.
        # The difference between this and self.output_queue is that messages in this queue need a reply, i.e., the
        # action_function will block on input by asking the user for the input values in this queue.
        self.prompt_queue = None

        # This is a list of all the messages sent by the action function that require a response ie., they are prompts
        # for input. Each of these messages need to have a corresponding reply in self.input_messages.
        # A mismatch means the step is blocking/waiting on some input.
        # Messages from the self.prompt_queue will be collected and entered into this list.
        self.prompt_messages = []

        # This is a list of all the messages sent by the action function.
        # Messages from the self.output_queue will be collected and entered into this list.
        self.output_messages = []

        # This is a list of all messages sent to the action function.
        # This contains messages collected from self.input_queue.
        self.input_messages = []

        # This enables AutoTrail to pause the Step if the action function fails.
        self.pause_on_fail = True

        # This value tells if the progeny of this step should be skipped if this step fails.
        # When True, if the step fails, all its progeny will be skipped.
        self.skip_progeny_on_failure = False

    def __str__(self):
        return self.tags['name']

    def _make_name(self):
        if hasattr(self.action_function, 'func_name'):
            return self.action_function.func_name
        else:
            return str(self.action_function)


class Stack(object):
    """Implements a Stack using lists.
    Provides a standard interface using push and pop.
    This is used as an accumulator for the traverse algorithm.
    """
    def __init__(self, v=None):
        """Initialize a Stack.

        Arguments:
        v -- An iterable to provide the initial set of values for the stack.
             Defaults to an empty stack.
        """
        if v is None:
            self.values = []
        else:
            self.values = list(v)

    def __len__(self):
        """Number of items in the stack.

        Returns:
        Integer -- Representing the number of items in the stack.
        """
        return len(self.values)

    def push(self, value):
        """Push an item onto the top of the stack.

        Arguments:
        value -- Can be any Python object.
        """
        self.values.append(value)

    def pop(self):
        """Pop an item from the top of the stack.

        Returns:
        The object from the top of the stack.
        """
        return self.values.pop()

    def __repr__(self):
        return 'Stack({})'.format(repr(self.values))


class Queue(object):
    """Implements a Queue using deque.
    Provides a standard interface using push and pop.
    This is used as an accumulator for the traverse algorithm.
    """
    def __init__(self, v=None):
        """Initialize a Queue.

        Arguments:
        v -- An iterable to provide the initial set of values for the queue.
             Defaults to an empty Queue.
        """
        if v is None:
            self.values = deque()
        else:
            self.values = deque(v)

    def __len__(self):
        """Number of items in the queue.

        Returns:
        Integer -- Representing the number of items in the queue.
        """
        return len(self.values)

    def push(self, value):
        """Push an item into the end of the queue.

        Arguments:
        value -- Can be any Python object.
        """

        self.values.append(value)

    def pop(self):
        """Pop the item at the beginning of the queue.

        Returns:
        The object at the beginning of the queue.
        """
        return self.values.popleft()

    def __repr__(self):
        return 'Queue({})'.format(repr(self.values))


def traverse(vertices_to_traverse):
    """traverses a DAG.

    Arguments:
    vertices_to_traverse -- The datastructure to use during traversal.
        If a Stack is passed, this results in a depth first traversal.
        If a Queue is passed, this results in a breadth first traversal.

        This data structure should consist of the root vertices and will be
        modified during traversal.

    Returns:
    Iterator over all vertices.
    """
    visited_vertices = set()

    while vertices_to_traverse:
        vertex = vertices_to_traverse.pop()
        if vertex not in visited_vertices:
            visited_vertices.add(vertex)
            yield vertex
            for child in vertex.children:
                vertices_to_traverse.push(child)


def breadth_first_traverse(vertex):
    """Performs a Breadth First traversal of a DAG starting at the given vertex.

    Arguments:
    vertex -- starting vertex for traversal.

    Returns:
    An iterator over the vertices.
    """
    vertices = Queue()
    vertices.push(vertex)
    return traverse(vertices)


def depth_first_traverse(vertex):
    """Performs a Depth First traversal of a DAG startign at the given vertex.

    Arguments:
    vertex -- starting vertex for traversal.

    Returns:
    An iterator over the vertices.
    """
    vertices = Stack()
    vertices.push(vertex)
    return traverse(vertices)


def is_dag_acyclic(root_vertex):
    """Perform an acyclicity check for a given DAG.

    Returns:
    True  -- If the DAG contains cycles.
    False -- If the DAG does not contain cycles.
    """
    visited = set()
    for vertex in topological_traverse(root_vertex):
        if vertex in visited:
            # DAG has cycles
            return False
        else:
            visited.add(vertex)
    return True


def topological_while(vertex, done_check, ignore_check):
    """Behaves like a while loop that iterates over a DAG respecting topological ordering.
    Does a topologically ordered traversal of a DAG starting at the given vertex (root vertex) but keeps yielding a
    vertex till it is marked done (see below).

    This algorithm is a modified and caller controllable version of a traversal respecting topological order.
    It is an implementation of Kahn's algorithm for topological sort. It will iterate over the vertices in the DAG and
    keep returning the same vertices until
    1) done_check(vertex) returns true for a vertex or
    2) ignore_check(vertex) returns true.

    Case when done_check(vertex) returns true -- The algorithm then considers this vertex to be visited and proceeds to
        check its children and returns only those children for which done_check is true for all parents
        (topological sort).

    Case when ignore_check(vertex) returns true -- The algorithm considers this vertex to be non-visitable and simply
        ignores it, resulting in ignoring all children downstream from this vertex.

    Using the functions done_check and ignore_check, the caller can appropriately mark the vertex as done or ignored.
    This algorithm is useful when a DAG needs to be traversed in a topologically sorted order but allowing the caller
    to take some actions with the vertices when being iterated.

    The continuous iteration of vertices until marked 'done' or 'ignored' allows the caller to use this a the main
    condition for the loop.

    Returns:
    Yields vertex type objects -- When a vertex is available
    Raises StopIteration       -- When no vertex is available

    Usage:
    for v in topological_while(root_vertex, custom_done_check_function, custom_ignore_check_function):
        # Do something with v.
        # All work done for v?
            # Change some attribute of v such that custom_done_check_function(v) returns True.
        # Cannot work v?
            # Change some attribute of v such that custom_ignore_check_function(v) returns True
            # This means that the entire subgraph from v will be skipped.
    """
    vertices = Queue()
    vertices.push(vertex)
    while vertices:
        vertex = vertices.pop()
        yield vertex
        if done_check(vertex):
            # Add next steps
            for next_vertex in vertex.children:
                # Check if all parents for the vertex are done.
                if all((done_check(v) for v in next_vertex.parents)):
                    vertices.push(next_vertex)
        elif not ignore_check(vertex):
            vertices.push(vertex)


def topological_traverse(vertex):
    """Perform a topological traversal of a DAG starting at the given Vertex using the more general
    topological_while algorithm.

    Yields vertex objects in topological order starting with the given vertex.
    """
    visited_vertices = set()
    # Once a vertex has been yielded (marked visited), consider it done.
    done_check = lambda x: True if x in visited_vertices else False
    ignore_check = lambda x: False  # Don't ignore any vertex (Never called)
    for vertex in topological_while(vertex, done_check, ignore_check):
        yield vertex
        visited_vertices.add(vertex)
