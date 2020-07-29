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
from multiprocessing import Manager


class Serializer:
    """A callable object that invokes all the given serializer callables and collates them into a single serialized
    dictionary presented as a multiprocessing.Manager dictionary.
    """
    def __init__(self, serializer_callables):
        """Define the list of callables that will be invoked in-order.

        :param serializer_callables:    An iterable of callables, each of which accepts no parameters and returns a
                                        dictionary that can be serialized into a multiprocessing.Manager dictionary.

        The 'serialized' instance attribute contains the serialized dictionary.
        """
        self._serializer_callables = serializer_callables
        self.serialized = Manager().dict()

    def __call__(self):
        """Call each of the defined callables in-order and update the multiprocessing.Manager dictionary with their
        returned dictionaries.

        :return: None
        """
        for serializer_callable in self._serializer_callables:
            self.serialized.update(serializer_callable())


class SerializerCallable:
    """A callable object that calls the given serializer function with the given object returns a serialized
    dictionary of the form {<key>: <serialized value>}.
    """
    def __init__(self, obj, key=None, serializer_function=None):
        """Setup the object to be serialized, its name in the dictionary and the serializer function.

        :param obj:                 The object to be serialized.
        :param key:                 The key in the serialized dictionary.
        :param serializer_function: The function that will be used to serialize the object (obj). This must accept
                                    only one parameter, viz., the object and return a serialized value.
        """
        self._obj = obj
        self._serializer_function = serializer_function

        # The below cannot be simplified to self._key = key or str(self._obj) because 0 is a valid accepted key.
        if key is None:
            self._key = str(self._obj)
        else:
            self._key = key

    def __call__(self):
        """Serialize the associated object and return a serialized dictionary.

        :return:    A dictionary of the form: {<key>: <serialized value>}
                    Where, the key is defined during class instantiation and the serialized value is obtained by
                        calling the serializer function.
        """
        return {self._key: self._serializer_function(self._obj) if self._serializer_function else self._obj}
