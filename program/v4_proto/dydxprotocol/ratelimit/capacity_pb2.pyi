from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from v4_proto.dydxprotocol.ratelimit import limit_params_pb2 as _limit_params_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DenomCapacity(_message.Message):
    __slots__ = ("denom", "capacity_list")
    DENOM_FIELD_NUMBER: _ClassVar[int]
    CAPACITY_LIST_FIELD_NUMBER: _ClassVar[int]
    denom: str
    capacity_list: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, denom: _Optional[str] = ..., capacity_list: _Optional[_Iterable[bytes]] = ...) -> None: ...

class LimiterCapacity(_message.Message):
    __slots__ = ("limiter", "capacity")
    LIMITER_FIELD_NUMBER: _ClassVar[int]
    CAPACITY_FIELD_NUMBER: _ClassVar[int]
    limiter: _limit_params_pb2.Limiter
    capacity: bytes
    def __init__(self, limiter: _Optional[_Union[_limit_params_pb2.Limiter, _Mapping]] = ..., capacity: _Optional[bytes] = ...) -> None: ...
