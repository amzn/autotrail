"""Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.
"""

import unittest
import uuid

from mock import MagicMock


from autotrail.core.dag import (
    Vertex, Step, Stack, Queue, traverse, breadth_first_traverse, depth_first_traverse, is_dag_acyclic,
    topological_while, topological_traverse)


class TestVertex(unittest.TestCase):
    def test_vertex_instance(self):
        vertex = Vertex()
        self.assertIsInstance(vertex, Vertex)
        self.assertEqual(vertex.parents, [])
        self.assertEqual(vertex.children, [])

    def test_vertex_add_child(self):
        vertex = Vertex()
        child_vertex = Vertex()
        vertex.add_child(child_vertex)

        self.assertEqual(vertex.children, [child_vertex])
        self.assertEqual(child_vertex.parents, [vertex])

    def test_vertex_add_parent(self):
        vertex = Vertex()
        parent_vertex = Vertex()
        vertex.add_parent(parent_vertex)

        self.assertEqual(vertex.parents, [parent_vertex])
        self.assertEqual(parent_vertex.children, [vertex])


class TestStep(unittest.TestCase):
    def test_instance(self):

        def mock_function():
            pass

        step = Step(mock_function)

        self.assertIsInstance(step, Step)
        self.assertEqual(step.action_function, mock_function)
        self.assertEqual(step.tags, {'name': 'mock_function'})
        self.assertEqual(step.state, step.READY)
        self.assertEqual(step.process, None)
        self.assertEqual(step.return_value, None)
        self.assertEqual(step.result_queue, None)
        self.assertEqual(step.input_queue, None)
        self.assertEqual(step.output_queue, None)
        self.assertEqual(step.prompt_queue, None)
        self.assertEqual(step.prompt_messages, [])
        self.assertEqual(step.output_messages, [])
        self.assertEqual(step.input_messages, [])
        self.assertTrue(step.pause_on_fail)
        self.assertEqual(str(step), 'mock_function')

        mock_env = MagicMock()
        mock_context = MagicMock()
        mock_return_value = MagicMock()
        mock_exception = MagicMock()

        args, kwargs = step.pre_processor(mock_env, mock_context)
        self.assertEqual(args, (mock_env, mock_context))
        self.assertEqual(kwargs, {})

        return_value = step.post_processor(mock_env, mock_context, mock_return_value)
        self.assertEqual(return_value, mock_return_value)

        return_value = step.failure_handler(mock_env, mock_context, mock_exception)
        self.assertEqual(return_value, mock_exception)

    def test_instance_with_callable_object(self):

        class MockAction(object):
            def __call__(self):
                pass
            def __str__(self):
                return 'MockAction'

        mock_action_object = MockAction()
        step = Step(mock_action_object)

        self.assertIsInstance(step, Step)
        self.assertEqual(step.action_function, mock_action_object)
        self.assertEqual(step.tags, {'name': 'MockAction'})
        self.assertEqual(step.state, step.READY)
        self.assertEqual(step.process, None)
        self.assertEqual(step.return_value, None)
        self.assertEqual(step.result_queue, None)
        self.assertEqual(step.input_queue, None)
        self.assertEqual(step.output_queue, None)
        self.assertEqual(step.prompt_queue, None)
        self.assertEqual(step.prompt_messages, [])
        self.assertEqual(step.output_messages, [])
        self.assertEqual(step.input_messages, [])
        self.assertTrue(step.pause_on_fail)
        self.assertEqual(str(step), 'MockAction')

    def test_instance_with_name(self):

        def mock_function():
            pass

        step = Step(mock_function, name='custom_name')
        self.assertIsInstance(step, Step)
        self.assertEqual(step.action_function, mock_function)
        self.assertEqual(step.tags, {'name': 'custom_name'})
        self.assertEqual(step.state, step.READY)
        self.assertEqual(step.process, None)
        self.assertEqual(step.return_value, None)
        self.assertEqual(step.result_queue, None)
        self.assertEqual(step.input_queue, None)
        self.assertEqual(step.output_queue, None)
        self.assertEqual(step.prompt_queue, None)
        self.assertEqual(step.prompt_messages, [])
        self.assertEqual(step.output_messages, [])
        self.assertEqual(step.input_messages, [])
        self.assertTrue(step.pause_on_fail)
        self.assertEqual(str(step), 'custom_name')


class TestStack(unittest.TestCase):
    def test_instance(self):
        stack = Stack()
        self.assertIsInstance(stack, Stack)
        self.assertEqual(len(stack), 0)
        self.assertEqual(repr(stack), 'Stack([])')

    def test_instance_with_list(self):
        stack = Stack([1, 2, 3])
        self.assertIsInstance(stack, Stack)
        self.assertEqual(len(stack), 3)
        self.assertEqual(stack.pop(), 3)
        self.assertEqual(stack.pop(), 2)
        self.assertEqual(stack.pop(), 1)

    def test_push_pop(self):
        stack = Stack()
        stack.push(0)
        self.assertEqual(len(stack), 1)
        self.assertEqual(repr(stack), 'Stack([0])')

        n = stack.pop()
        self.assertEqual(n, 0)
        self.assertEqual(len(stack), 0)
        self.assertEqual(repr(stack), 'Stack([])')

    def test_pop_empty(self):
        stack = Stack()
        with self.assertRaises(IndexError):
            stack.pop()


class TestQueue(unittest.TestCase):
    def test_instance(self):
        queue = Queue()
        self.assertIsInstance(queue, Queue)
        self.assertEqual(len(queue), 0)
        self.assertEqual(repr(queue), 'Queue(deque([]))')

    def test_instance_with_list(self):
        queue = Queue([1, 2, 3])
        self.assertIsInstance(queue, Queue)
        self.assertEqual(len(queue), 3)
        self.assertEqual(queue.pop(), 1)
        self.assertEqual(queue.pop(), 2)
        self.assertEqual(queue.pop(), 3)

    def test_push_pop(self):
        queue = Queue()
        queue.push(0)
        self.assertEqual(len(queue), 1)
        self.assertEqual(repr(queue), 'Queue(deque([0]))')

        n = queue.pop()
        self.assertEqual(n, 0)
        self.assertEqual(len(queue), 0)
        self.assertEqual(repr(queue), 'Queue(deque([]))')

    def test_pop_empty(self):
        queue = Queue()
        with self.assertRaises(IndexError):
            queue.pop()


class TestTraversalAlgorithms(unittest.TestCase):
    def setUp(self):
        self.vertex_0 = Vertex()
        self.vertex_1 = Vertex()
        self.vertex_2 = Vertex()
        self.vertex_3 = Vertex()
        self.vertex_4 = Vertex()
        self.vertex_5 = Vertex()
        self.vertex_6 = Vertex()

        # Create a DAG
        #          +--> vertex_1 --> vertex_4
        # vertex_0 |--> vertex_2 --> vertex_5
        #          +--> vertex_3 --> vertex_6
        self.vertex_0.add_child(self.vertex_1)
        self.vertex_0.add_child(self.vertex_2)
        self.vertex_0.add_child(self.vertex_3)
        self.vertex_1.add_child(self.vertex_4)
        self.vertex_2.add_child(self.vertex_5)
        self.vertex_3.add_child(self.vertex_6)

    def test_traverse_with_simple_graph(self):
        vertex_0 = Vertex()
        vertex_1 = Vertex()
        vertex_2 = Vertex()

        # Create a simple DAG
        # vertex_0 -> vertex_1 -> vertex_2
        vertex_0.add_child(vertex_1)
        vertex_1.add_child(vertex_2)

        vertices = Stack()
        vertices.push(vertex_0)
        traversal_list = [v for v in traverse(vertices)]
        self.assertEqual(traversal_list, [vertex_0, vertex_1, vertex_2])

        # In this simple DAG, Stack or Queue shouldn't make any difference.
        vertices = Queue()
        vertices.push(vertex_0)
        traversal_list = [v for v in traverse(vertices)]
        self.assertEqual(traversal_list, [vertex_0, vertex_1, vertex_2])

    def test_traverse_with_not_so_simple_graph(self):
        vertex_0 = Vertex()
        vertex_1 = Vertex()
        vertex_2 = Vertex()
        vertex_3 = Vertex()

        # Create a simple DAG
        # vertex_0 -> vertex_1 -> vertex_2
        #
        #             +--> vertex_1 -->+
        # vertex_0 -->|                |--> vertex_3
        #             +--> vertex_2 -->+
        vertex_0.add_child(vertex_1)
        vertex_0.add_child(vertex_2)
        vertex_1.add_child(vertex_3)
        vertex_2.add_child(vertex_3)

        vertex_counts = {
            vertex_0: 0,
            vertex_1: 0,
            vertex_2: 0,
            vertex_3: 0,
        }
        vertices = Stack()
        vertices.push(vertex_0)
        for vertex in traverse(vertices):
            vertex_counts[vertex] += 1

        # None of the vertices should have a count more than one.
        for vertex in vertex_counts:
            self.assertEqual(vertex_counts[vertex], 1)

    def test_breadth_first_traverse(self):
        traversal_list = [v for v in breadth_first_traverse(self.vertex_0)]

        # The traversal happened correctly if the index numbers of the
        # different vertex objects preserve the same order.
        #          +--> vertex_1 --> vertex_4
        # vertex_0 |--> vertex_2 --> vertex_5
        #          +--> vertex_3 --> vertex_6
        vertex_0_pos = traversal_list.index(self.vertex_0)
        vertex_1_pos = traversal_list.index(self.vertex_1)
        vertex_2_pos = traversal_list.index(self.vertex_2)
        vertex_3_pos = traversal_list.index(self.vertex_3)
        vertex_4_pos = traversal_list.index(self.vertex_4)
        vertex_5_pos = traversal_list.index(self.vertex_5)
        vertex_6_pos = traversal_list.index(self.vertex_6)
        # Assert local ordering ie., child vertices follow parents
        self.assertLess(vertex_0_pos, vertex_1_pos)
        self.assertLess(vertex_0_pos, vertex_2_pos)
        self.assertLess(vertex_0_pos, vertex_3_pos)
        self.assertLess(vertex_1_pos, vertex_4_pos)
        self.assertLess(vertex_2_pos, vertex_5_pos)
        self.assertLess(vertex_3_pos, vertex_6_pos)

        # Assert global ordering, no child vertices occur before any
        # parents due to breadth first traversal
        self.assertLess(vertex_1_pos, vertex_5_pos)
        self.assertLess(vertex_1_pos, vertex_6_pos)
        self.assertLess(vertex_2_pos, vertex_4_pos)
        self.assertLess(vertex_2_pos, vertex_6_pos)
        self.assertLess(vertex_3_pos, vertex_4_pos)
        self.assertLess(vertex_3_pos, vertex_5_pos)


    def test_depth_first_traverse(self):
        traversal_list = [v for v in depth_first_traverse(self.vertex_0)]

        # The traversal happened correctly if the index numbers of the
        # different vertex objects preserve the same order.
        #          +--> vertex_1 --> vertex_4
        # vertex_0 |--> vertex_2 --> vertex_5
        #          +--> vertex_3 --> vertex_6
        vertex_0_pos = traversal_list.index(self.vertex_0)
        vertex_1_pos = traversal_list.index(self.vertex_1)
        vertex_2_pos = traversal_list.index(self.vertex_2)
        vertex_3_pos = traversal_list.index(self.vertex_3)
        vertex_4_pos = traversal_list.index(self.vertex_4)
        vertex_5_pos = traversal_list.index(self.vertex_5)
        vertex_6_pos = traversal_list.index(self.vertex_6)
        # Assert local ordering ie., child vertices follow parents
        self.assertLess(vertex_0_pos, vertex_1_pos)
        self.assertLess(vertex_0_pos, vertex_2_pos)
        self.assertLess(vertex_0_pos, vertex_3_pos)
        self.assertLess(vertex_1_pos, vertex_4_pos)
        self.assertLess(vertex_2_pos, vertex_5_pos)
        self.assertLess(vertex_3_pos, vertex_6_pos)

        # Assert global ordering ie., there is atleast 1 vertex between
        # any of the vertices 1, 2 and 3 (due to depth first traversal).
        self.assertTrue(abs(vertex_1_pos - vertex_2_pos) > 1)
        self.assertTrue(abs(vertex_1_pos - vertex_3_pos) > 1)
        self.assertTrue(abs(vertex_2_pos - vertex_3_pos) > 1)


class TestCyclicityAlgorithms(unittest.TestCase):
    def test_is_dag_acyclic(self):
        # Create an acyclic DAG
        # vertex_0 --> vertex_1 --> vertex_2
        vertex_0 = Vertex()
        vertex_1 = Vertex()
        vertex_2 = Vertex()
        vertex_0.add_child(vertex_1)
        vertex_1.add_child(vertex_2)

        # Create a cyclic DAG
        #    +------------+
        #    |            |
        #    v     +--> vertex_b
        # vertex_a |
        #          +--> vertex_c --> vertex_d
        vertex_a = Vertex()
        vertex_b = Vertex()
        vertex_c = Vertex()
        vertex_d = Vertex()
        vertex_a.add_child(vertex_b)
        vertex_b.add_child(vertex_a)
        vertex_a.add_child(vertex_c)
        vertex_c.add_child(vertex_d)

        self.assertTrue(is_dag_acyclic(vertex_0))
        self.assertFalse(is_dag_acyclic(vertex_a))


class TestTopologicalWhile(unittest.TestCase):
    def setUp(self):
        # Create a DAG
        #          +--> vertex_1 --> vertex_4 -->+
        # vertex_0 |--> vertex_2                 |--> vertex_5
        #          +--> vertex_3 --------------->+
        self.vertex_0 = Vertex()
        self.vertex_1 = Vertex()
        self.vertex_2 = Vertex()
        self.vertex_3 = Vertex()
        self.vertex_4 = Vertex()
        self.vertex_5 = Vertex()

        self.vertex_0.add_child(self.vertex_1)
        self.vertex_0.add_child(self.vertex_2)
        self.vertex_0.add_child(self.vertex_3)
        self.vertex_1.add_child(self.vertex_4)
        self.vertex_3.add_child(self.vertex_5)
        self.vertex_4.add_child(self.vertex_5)

    def test_topological_while_iteration_never_done(self):
        # Don't mark any vertex as done.
        def done_check(vertex):
            return False

        # Don't ignore any vertex
        def ignore_check(vertex):
            return False

        total_count = 0
        vertex_counts = {
            self.vertex_0: 0,
            self.vertex_1: 0,
            self.vertex_2: 0,
            self.vertex_3: 0,
            self.vertex_4: 0,
            self.vertex_5: 0,
        }
        for vertex in topological_while(self.vertex_0, done_check, ignore_check):
            vertex_counts[vertex] += 1
            if total_count < 90:
                total_count += 1
            else:
                break

        # Topological while should not visit any other vertex because done_check always returns false.
        self.assertEqual(vertex_counts[self.vertex_0], 91)
        self.assertEqual(vertex_counts[self.vertex_1], 0)
        self.assertEqual(vertex_counts[self.vertex_2], 0)
        self.assertEqual(vertex_counts[self.vertex_3], 0)
        self.assertEqual(vertex_counts[self.vertex_4], 0)
        self.assertEqual(vertex_counts[self.vertex_5], 0)

    def test_topological_while_iteration_with_done(self):
        # Mark every returned vertex as done.
        def done_check(vertex):
            return True

        # Don't ignore any vertex
        def ignore_check(vertex):
            return False

        total_count = 0
        vertex_counts = {
            self.vertex_0: 0,
            self.vertex_1: 0,
            self.vertex_2: 0,
            self.vertex_3: 0,
            self.vertex_4: 0,
            self.vertex_5: 0,
        }
        for vertex in topological_while(self.vertex_0, done_check, ignore_check):
            vertex_counts[vertex] += 1
            if total_count < 90:
                total_count += 1
            else:
                break

        # Topological while should visit every vertex exactly equal to the number of its parents.
        self.assertEqual(vertex_counts[self.vertex_0], 1)
        self.assertEqual(vertex_counts[self.vertex_1], 1)
        self.assertEqual(vertex_counts[self.vertex_2], 1)
        self.assertEqual(vertex_counts[self.vertex_3], 1)
        self.assertEqual(vertex_counts[self.vertex_4], 1)
        self.assertEqual(vertex_counts[self.vertex_5], 2)

    def test_topological_while_iteration_with_ignore(self):
        # Don't mark any vertex as done.
        def done_check(vertex):
            return False

        # Ignore every vertex
        def ignore_check(vertex):
            return True

        total_count = 0
        vertex_counts = {
            self.vertex_0: 0,
            self.vertex_1: 0,
            self.vertex_2: 0,
            self.vertex_3: 0,
            self.vertex_4: 0,
            self.vertex_5: 0,
        }
        for vertex in topological_while(self.vertex_0, done_check, ignore_check):
            vertex_counts[vertex] += 1
            if total_count < 90:
                total_count += 1
            else:
                break

        # Topological while should not visit any vertex other than the root vertex.
        self.assertEqual(vertex_counts[self.vertex_0], 1)
        self.assertEqual(vertex_counts[self.vertex_1], 0)
        self.assertEqual(vertex_counts[self.vertex_2], 0)
        self.assertEqual(vertex_counts[self.vertex_3], 0)
        self.assertEqual(vertex_counts[self.vertex_4], 0)
        self.assertEqual(vertex_counts[self.vertex_5], 0)

    def test_topological_while_with_done_and_ignore_markers(self):
        visited_vertices = []
        ignored_vertices = []
        # Mark a vertex done if it has been marked as visited with the visted_marker
        def done_check(vertex):
            if vertex in visited_vertices:
                return True
            else:
                return False

        # Ignore vertices that have been marked with the ignore_marker
        def ignore_check(vertex):
            if vertex in ignored_vertices:
                return True
            else:
                return False

        total_count = 0
        vertex_counts = {
            self.vertex_0: 0,
            self.vertex_1: 0,
            self.vertex_2: 0,
            self.vertex_3: 0,
            self.vertex_4: 0,
            self.vertex_5: 0,
        }

        # For the DAG:
        #          +--> vertex_1 --> vertex_4 -->+
        # vertex_0 |--> vertex_2                 |--> vertex_5
        #          +--> vertex_3 --------------->+
        #
        # Mark vertex_1 as ignored and check that vertex_4 and vertex_5 are not visited.
        for vertex in topological_while(self.vertex_0, done_check, ignore_check):
            vertex_counts[vertex] += 1
            if vertex == self.vertex_1:
                ignored_vertices.append(vertex) # mark this to ignore
            else:
                visited_vertices.append(vertex) # mark this as done.

        # Topological while should not visit vertex_4 and vertex_5 and only once every other vertex
        self.assertEqual(vertex_counts[self.vertex_0], 1)
        self.assertEqual(vertex_counts[self.vertex_1], 1)
        self.assertEqual(vertex_counts[self.vertex_2], 1)
        self.assertEqual(vertex_counts[self.vertex_3], 1)
        self.assertEqual(vertex_counts[self.vertex_4], 0)
        self.assertEqual(vertex_counts[self.vertex_5], 0)

    def test_topological_traverse(self):
        # For the DAG:
        #          +--> vertex_1 --> vertex_4 -->+
        # vertex_0 |--> vertex_2                 |--> vertex_5
        #          +--> vertex_3 --------------->+
        #
        vertices = [v for v in topological_traverse(self.vertex_0)]

        vertex_0_pos = vertices.index(self.vertex_0)
        vertex_1_pos = vertices.index(self.vertex_1)
        vertex_2_pos = vertices.index(self.vertex_2)
        vertex_3_pos = vertices.index(self.vertex_3)
        vertex_4_pos = vertices.index(self.vertex_4)
        vertex_5_pos = vertices.index(self.vertex_5)

        # Check if the correct order is preserved
        self.assertLess(vertex_0_pos, vertex_1_pos)
        self.assertLess(vertex_0_pos, vertex_2_pos)
        self.assertLess(vertex_0_pos, vertex_3_pos)
        self.assertLess(vertex_1_pos, vertex_4_pos)
        self.assertLess(vertex_4_pos, vertex_5_pos)
        self.assertLess(vertex_3_pos, vertex_5_pos)
