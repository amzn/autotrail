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
def generate_workflow_graph_as_dot(ordered_pairs, stream, id_extractor=id, rankdir='LR', name='workflow'):
    """Generate a graph for the given ordered pairs in the DOT langauge.

    :param ordered_pairs:   An iterable of tuples, each tuple representing an ordered pair, which, in-turn represents
                            an edge.
    :param stream:          A file-like object to write to. Must support the write method.
    :param id_extractor:    Callable that takes a step as an argument and extracts an id for it. Defaults to id().
    :param rankdir:         Determines the orientation for the graph. Defaults to 'LR'.
                            See: https://graphviz.org/doc/info/attrs.html#k:rankdir
    :param name:            The name for the graph.
    :return:                None
    """
    print('digraph {} {{'.format(name), file=stream)
    print('graph [rankdir={}];'.format(rankdir), file=stream)
    print('node [style=rounded, shape=box];', file=stream)

    template = '"{}_{}" -> "{}_{}";'
    for origin, terminus in ordered_pairs:
        print(template.format(str(origin), id_extractor(origin), str(terminus), id_extractor(terminus)),
              file=stream)
    print('}', file=stream)
