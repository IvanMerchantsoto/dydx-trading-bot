from google.protobuf import any_pb2 as _any_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DelayedMessage(_message.Message):
    __slots__ = ("id", "msg", "block_height")
    ID_FIELD_NUMBER: _ClassVar[int]
    MSG_FIELD_NUMBER: _ClassVar[int]
    BLOCK_HEIGHT_FIELD_NUMBER: _ClassVar[int]
    id: int
    msg: _any_pb2.Any
    block_height: int
    def __init__(self, id: _Optional[int] = ..., msg: _Optional[_Union[_any_pb2.Any, _Mapping]] = ..., block_height: _Optional[int] = ...) -> None: ...
