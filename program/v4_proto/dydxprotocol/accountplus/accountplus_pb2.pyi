from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AccountState(_message.Message):
    __slots__ = ("address", "timestamp_nonce_details")
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_NONCE_DETAILS_FIELD_NUMBER: _ClassVar[int]
    address: str
    timestamp_nonce_details: TimestampNonceDetails
    def __init__(self, address: _Optional[str] = ..., timestamp_nonce_details: _Optional[_Union[TimestampNonceDetails, _Mapping]] = ...) -> None: ...

class TimestampNonceDetails(_message.Message):
    __slots__ = ("timestamp_nonces", "max_ejected_nonce")
    TIMESTAMP_NONCES_FIELD_NUMBER: _ClassVar[int]
    MAX_EJECTED_NONCE_FIELD_NUMBER: _ClassVar[int]
    timestamp_nonces: _containers.RepeatedScalarFieldContainer[int]
    max_ejected_nonce: int
    def __init__(self, timestamp_nonces: _Optional[_Iterable[int]] = ..., max_ejected_nonce: _Optional[int] = ...) -> None: ...
