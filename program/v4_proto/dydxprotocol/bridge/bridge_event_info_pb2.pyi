from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class BridgeEventInfo(_message.Message):
    __slots__ = ("next_id", "eth_block_height")
    NEXT_ID_FIELD_NUMBER: _ClassVar[int]
    ETH_BLOCK_HEIGHT_FIELD_NUMBER: _ClassVar[int]
    next_id: int
    eth_block_height: int
    def __init__(self, next_id: _Optional[int] = ..., eth_block_height: _Optional[int] = ...) -> None: ...
