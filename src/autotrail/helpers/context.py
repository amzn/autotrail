"""Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
limitations under the License.

Context helpers

This module defines some helpers that make writing context classes very easy.
"""


from multiprocessing.managers import BaseManager


def threadsafe_class(user_defined_class):
    """This decorator takes a class and converts it into a thread-safe class so that an object of this class can be
    shared between two or more concurrent action functions.

    This works using BaseManager from multiprocessing.managers and simply provides a convenient way of using it.

    Constraints:
    In order to make a class threadsafe, there are some constraints on how the class behaves (or how objects of the
    class behave) and how it is used. They are as follows:
    1) All users of the class must get and set any values using getters and setters. The class must define them and
       access the actual attributes within these.
    2) Python data structures such as list, dictionary are allowed and when decorated, will become threadsafe, but any
       interactions with these must be through getters and setters.
    To understand the constraints more, please read Managers, more specifically multiprocessing.managers.BaseManager.

    Example:
        @threadsafe_class
        class MyClass(object):
            def __init__(self):
                self.values = {}

            def set(self, key, value):
                self.values[key] = value

            def get(self):
                return self.values

        # This instance is threadsafe and can be shared between two or more concurrent action functions.
        an_instance = MyClass()
    """
    BaseManager.register(user_defined_class.__name__, user_defined_class)
    manager = BaseManager()
    manager.start()
    return getattr(manager, user_defined_class.__name__)
