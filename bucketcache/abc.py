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


try:
    from abc import abstractstaticmethod
except ImportError:
    class abstractstaticmethod(staticmethod):
        """
        A decorator indicating abstract staticmethods.

        Similar to abstractmethod.

        Usage:

            class C(metaclass=ABCMeta):
                @abstractstaticmethod
                def my_abstract_staticmethod(...):
                    ...

        'abstractstaticmethod' is deprecated. Use 'staticmethod' with
        'abstractmethod' instead.
        """

        __isabstractmethod__ = True

        def __init__(self, callable):
            callable.__isabstractmethod__ = True
            super(abstractstaticmethod, self).__init__(callable)
