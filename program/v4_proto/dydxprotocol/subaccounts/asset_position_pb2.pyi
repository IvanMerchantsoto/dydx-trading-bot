from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class AssetPosition(_message.Message):
    __slots__ = ("asset_id", "quantums", "index")
    ASSET_ID_FIELD_NUMBER: _ClassVar[int]
    QUANTUMS_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    asset_id: int
    quantums: bytes
    index: int
    def __init__(self, asset_id: _Optional[int] = ..., quantums: _Optional[bytes] = ..., index: _Optional[int] = ...) -> None: ...
