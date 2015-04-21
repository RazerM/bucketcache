"""abc functionality from Python 3"""
from __future__ import absolute_import

try:
    from abc import abstractclassmethod
except ImportError:
    class abstractclassmethod(classmethod):
        """
        A decorator indicating abstract classmethods.

        Similar to abstractmethod.

        Usage:

            class C(metaclass=ABCMeta):
                @abstractclassmethod
                def my_abstract_classmethod(cls, ...):
                    ...

        'abstractclassmethod' is deprecated. Use 'classmethod' with
        'abstractmethod' instead.
        """

        __isabstractmethod__ = True

        def __init__(self, callable):
            callable.__isabstractmethod__ = True
            super(abstractclassmethod, self).__init__(callable)
