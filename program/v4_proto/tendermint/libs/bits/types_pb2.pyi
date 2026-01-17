from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class BitArray(_message.Message):
    __slots__ = ("bits", "elems")
    BITS_FIELD_NUMBER: _ClassVar[int]
    ELEMS_FIELD_NUMBER: _ClassVar[int]
    bits: int
    elems: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, bits: _Optional[int] = ..., elems: _Optional[_Iterable[int]] = ...) -> None: ...
