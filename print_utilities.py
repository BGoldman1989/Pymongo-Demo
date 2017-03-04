"""
Contains special functions for printing MongoDB documents returned from a query.
"""
from pprint import PrettyPrinter
from pymongo.cursor import Cursor
from pymongo.command_cursor import CommandCursor
from bson.son import SON
import sys as _sys
import warnings

__all__ = ["pprint","pformat","isreadable","isrecursive","saferepr",
           "PrettyPrinter"]

# cache these for faster access:
_commajoin = ", ".join
_id = id
_len = len
_type = type

"""
Prints resulting data from MongoDB Query
"""
def print_result(result):
    if isinstance(result,Cursor) or isinstance(result,CommandCursor):
        for doc in result:
            __pprint(doc)
    else:
        __pprint(result)
    print ''

"""
Expanded pprint function to enable pretty printing of SON objects
"""
def __pprint(object, stream=None, indent=1, width=80, depth=None):
    """Pretty-print a Python object to a stream [default is sys.stdout]."""
    printer = PrettyPrinterExt(
        stream=stream, indent=indent, width=width, depth=depth)
    printer.pprint(object)

"""
#Extends PrettyPrinter class from pprint library to handle SON objects
"""
class PrettyPrinterExt(PrettyPrinter):

    def __init__(self, indent=1, width=80, depth=None, stream=None):
        """Handle pretty printing operations onto a stream using a set of
        configured parameters.

        indent
            Number of spaces to indent for each level of nesting.

        width
            Attempted maximum number of columns in the output.

        depth
            The maximum depth to print out nested structures.

        stream
            The desired output stream.  If omitted (or false), the standard
            output stream available at construction will be used."""

        PrettyPrinter.__init__(self,indent=1, width=80, depth=None, stream=None)

    def pprint(self,object):
        if isinstance(object,SON):
            self._format_son(object, self._stream, 0, 0, {}, 0)
        else:
            self._format(object, self._stream, 0, 0, {}, 0)
        self._stream.write("\n")

    def _format_son(self, object, stream, indent, allowance, context, level):
        level = level + 1
        objid = _id(object)
        if objid in context:
            stream.write(_recursion(object))
            self._recursive = True
            self._readable = False
            return
        rep = self._repr(object, context, level - 1)
        typ = _type(object)
        sepLines = _len(rep) > (self._width - 1 - indent - allowance)
        write = stream.write

        if self._depth and level > self._depth:
            write(rep)
            return

        r = getattr(typ, "__repr__", None)

        if issubclass(typ, SON) or (issubclass(typ, dict) and r is dict.__repr__):
            write('{')
            if self._indent_per_level > 1:
                write((self._indent_per_level - 1) * ' ')
            length = _len(object)
            if length:
                context[objid] = 1
                indent = indent + self._indent_per_level
                items = object.items()
                key, ent = items[0]
                rep = self._repr(key, context, level)
                write(rep)
                write(': ')
                self._format_son(ent, stream, indent + _len(rep) + 2,
                             allowance + 1, context, level)
                if length > 1:
                    for key, ent in items[1:]:
                        rep = self._repr(key, context, level)
                        if sepLines:
                            write(',\n%s%s: ' % (' '*indent, rep))
                        else:
                            write(', %s: ' % rep)
                        self._format_son(ent, stream, indent + _len(rep) + 2,
                                      allowance + 1, context, level)
                indent = indent - self._indent_per_level
                del context[objid]
            write('}')
            return

        if ((issubclass(typ, list) and r is list.__repr__) or
            (issubclass(typ, tuple) and r is tuple.__repr__) or
            (issubclass(typ, set) and r is set.__repr__) or
            (issubclass(typ, frozenset) and r is frozenset.__repr__)
           ):
            length = _len(object)
            if issubclass(typ, list):
                write('[')
                endchar = ']'
            elif issubclass(typ, tuple):
                write('(')
                endchar = ')'
            else:
                if not length:
                    write(rep)
                    return
                write(typ.__name__)
                write('([')
                endchar = '])'
                indent += len(typ.__name__) + 1
                object = _sorted(object)
            if self._indent_per_level > 1 and sepLines:
                write((self._indent_per_level - 1) * ' ')
            if length:
                context[objid] = 1
                indent = indent + self._indent_per_level
                self._format(object[0], stream, indent, allowance + 1,
                             context, level)
                if length > 1:
                    for ent in object[1:]:
                        if sepLines:
                            write(',\n' + ' '*indent)
                        else:
                            write(', ')
                        self._format(ent, stream, indent,
                                      allowance + 1, context, level)
                indent = indent - self._indent_per_level
                del context[objid]
            if issubclass(typ, tuple) and length == 1:
                write(',')
            write(endchar)
            return

        write(rep)

def _recursion(object):
    return ("<Recursion on %s with id=%s>"
            % (_type(object).__name__, _id(object)))

def _sorted(iterable):
    with warnings.catch_warnings():
        if _sys.py3kwarning:
            warnings.filterwarnings("ignore", "comparing unequal types "
                                    "not supported", DeprecationWarning)
        return sorted(iterable)