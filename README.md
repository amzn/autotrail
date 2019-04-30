# AutoTrail
AutoTrail is a highly modular, partial automation workflow engine providing excellent run time execution control.

## User guides and examples
- Please read the documentation of the `TrailServer` to understand how to run a trail server.
- The `layer2/trail.py/TrailClient` documents how to use the various API calls.
- Please go through the fully working examples provided in:
  - `examples/interactive_trail.py` -- for an interactive use of AutoTrail.
  The example trail used is defined in the file.
  - `examples/runbook.py` shows how a trail can behave like a runbook.
  - `examples/elaborate_example.py` shows how a trail run different branches based on cli switches.
  - `examples/separate_client_and_server/example_server.py` and `examples/separate_client_and_server/example_client.py` show an
  example of running the trail server and client separately.

### How to run the example trail?
```zsh
# Make sure the autotrail directory is in your PYTHONPATH
python examples/interactive_trail.py
```

## How do I effectively use the framework?
6. `TrailServer` -- Layer2 class to manage running the `trail_manager`. (Located at: `autotrail/layer2/trail.py`)
7. `TrailClient` -- Layer2 class to make API calls using method calls. (This is the client class). (Located at: `autotrail/layer2/trail.py`)
8. `InteractiveTrail` -- A helper class that adds interactivity (printing to STDOUT and STDERR) to the API methods provided by the `TrailClient` class. (Located at: `autotrail/helpers.py`)

## How do I develop code for the framework?
9. `Step` -- This is the container for functions to be executed. (Located at: `autotrail/core/dag.py`)
9. `extraction_wrapper` -- This function helps preserve your functions to retain their signature, without having to comply to a specific signature. (Located at: `autotrail/helpers.py`)
10. `ShellCommand` -- A pre-baked action function that can run shell commands and send the STDOUT and STDERR messages as messages to the user. (Located at: `autotrail/helpers.py`)
11. `Instruction` -- A pre-baked action function that sends instructions to the user, which can be used to convert parts of the workflow into manual steps. (Located at: `autotrail/helpers.py`)
10. `make_simple_templating_function` and `make_context_attribute_based_templating_function` -- These can reduce a lot of effort by easily extracting values from context object attributes. (Located at: `autotrail/helpers.py`)
12. `threadsafe_class` -- A helper that makes a context class threadsafe so that parallel steps can write to the context object. (Located at: `autotrail/helpers.py`)
13. `write_trail_definition_as_dot` -- A function that writes the trail DAG as a DOT file, which can then be converted to an SVG or other formats using the GNU `dot` program. (Located at: `autotrail/helpers.py`)
14. `monitor_trail` -- A helper function to keep track of the trail status and invoke call-back functions when status changes per specification. (Located at: `autotrail/helpers.py`)
15. `create_conditional_step`-- A helper function to easily create a conditional step (branching in the trail). (Located at: `autotrail/helpers.py`)

## How is the code structured?
AutoTrail code is laid out in *layers*. They are as follows:
- Core
  - Contains the core data structures and algorithms and can be found in the `autotrail/core` package.
  - This contains definitions of the most basic classes (data structures) and functions (algorithms) that perform the most fundamental operations.
  - These are used extensively by the higher layers.
- Layer1
  - Contains the lowest level implementation of the workflow engine.
  - It provides the data structures and algorithms to run the server, the workflow and respond to API calls.
  - Using functions of this layer requires initialization of some resources by the user. The higher layers make certain assumptions and are easier to use.
  - Code for this layer can be found in the `autotrail/layer1` package.
  - The layer1 client is used by the layer2 client to run a trail.
- Layer2
  - Contains the more user-friendly classes for direct use.
  - This layer provides the `TrailServer` and `TrailClient` classes, which are used to run a trail (and its server) and make API calls to it.
  - These classes can be found in `autotrail/layer2/trail.py`.
  - All layer2 code can be found in the `autotrail/layer2` package.
  - To see how the API calls are called, please read the documentation of the `layer2/trail.py/TrailClient` class.

## What is the overall flow of logic?
1. The user runs the server using the `autotrail/layer2/trail.py/TrailServer` class.
2. The user then interacts with the server using the `autotrail/layer2/trail.py/TrailClient` class by passing it the socket file used in the `TrailServer`. (See the examples to know how this is done.)
3. The user makes API calls by calling the public methods provided by the `autotrail/layer2/trail.py/TrailClient` class. Eg., Pause a trail by using `TrailClient.pause()`. For more details about the API calls, please read the documentation of the `TrailClient` class.
4. When an API call is made by a user or script (by using a method of the `TrailClient` class), it sends the API call as a JSON encoded dictionary to the socket.
5. The Layer1 server (`layer1/trail.py/trail_manager`) periodically calls `core/socket_communication.py/serve_socket` function, which looks for any requests, accepts them and calls the `layer1/api.py/handle_api_call` function, which then takes the necessary steps to complete the API request and prepares a response. To understand how API call handlers are defined, please read the documentation of the `layer1/api.py/APICallDefinition` namedtuple.
6. This response is sent to the user by the `serve_socket` function and a "payload" value (the return value) of `handle_api_call` is returned back to `trail_manager`. If this value is `False`, then it signals the `trail_manager` to shutdown the server and exit.
7. The response is a JSON encoded dictionary, which is received by the `TrailClient` and the result is extracted and returned based on the method documentation. Typically, exceptions are raised when errors are encountered.

### How does the framework work?
Read the documentation of the following classes and functions (in the order given) to understand how things work:

1. `topological_while` -- This algorithm is the main engine used by the framework. (Located at: `autotrail/core/dag.py`)
2. `trail_manager` -- This is the layer1 server, the lowest level of trail operation.
Pay specific attention to understanding the `state_functions` and `ignorable_state_functions` data structures, which are used to define the state transitions. (Located at: `autotrail/layer1/trail.py`)
3. `serve_socket` -- The function that runs the Unix socket server for the API calls. (Located at: `autotrail/core/socket_communication.py`)
4. `handle_api_call` -- The function that handles the API calls and takes actions on steps. Specifically, please read the documentation of `APICallDefinition`, the namedtuple that is used to draw up the entire specification of an API call. (Located at: `autotrail/layer1/api.py`)

### How do I effectively use the framework?
6. `TrailServer` -- Layer2 class to manage running the `trail_manager`. (Located at: `autotrail/layer2/trail.py`)
7. `TrailClient` -- Layer2 class to make API calls using method calls. (This is the client class). (Located at: `autotrail/layer2/trail.py`)
8. `InteractiveTrail` -- A helper class that adds interactivity (printing to STDOUT and STDERR) to the API methods provided by the `TrailClient` class. (Located at: `autotrail/helpers.py`)

### How do I develop code for the framework?
9. `Step` -- This is the container for functions to be executed. (Located at: `autotrail/core/dag.py`)
9. `extraction_wrapper` -- This function helps preserve your functions to retain their signature, without having to comply to a specific signature. (Located at: `autotrail/helpers.py`)
10. `ShellCommand` -- A pre-baked action function that can run shell commands and send the STDOUT and STDERR messages as messages to the user. (Located at: `autotrail/helpers.py`)
11. `Instruction` -- A pre-baked action function that sends instructions to the user, which can be used to convert parts of the workflow into manual steps. (Located at: `autotrail/helpers.py`)
10. `make_simple_templating_function` and `make_context_attribute_based_templating_function` -- These can reduce a lot of effort by easily extracting values from context object attributes. (Located at: `autotrail/helpers.py`)
12. `threadsafe_class` -- A helper that makes a context class threadsafe so that parallel steps can write to the context object. (Located at: `autotrail/helpers.py`)
13. `write_trail_definition_as_dot` -- A function that writes the trail DAG as a DOT file, which can then be converted to an SVG or other formats using the GNU `dot` program. (Located at: `autotrail/helpers.py`)
14. `monitor_trail` -- A helper function to keep track of the trail status and invoke call-back functions when status changes per specification. (Located at: `autotrail/helpers.py`)
15. `create_conditional_step`-- A helper function to easily create a conditional step (branching in the trail). (Located at: `autotrail/helpers.py`)
