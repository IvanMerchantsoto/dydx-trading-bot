from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class StakingTier(_message.Message):
    __slots__ = ("fee_tier_name", "levels")
    FEE_TIER_NAME_FIELD_NUMBER: _ClassVar[int]
    LEVELS_FIELD_NUMBER: _ClassVar[int]
    fee_tier_name: str
    levels: _containers.RepeatedCompositeFieldContainer[StakingLevel]
    def __init__(self, fee_tier_name: _Optional[str] = ..., levels: _Optional[_Iterable[_Union[StakingLevel, _Mapping]]] = ...) -> None: ...

class StakingLevel(_message.Message):
    __slots__ = ("min_staked_base_tokens", "fee_discount_ppm")
    MIN_STAKED_BASE_TOKENS_FIELD_NUMBER: _ClassVar[int]
    FEE_DISCOUNT_PPM_FIELD_NUMBER: _ClassVar[int]
    min_staked_base_tokens: bytes
    fee_discount_ppm: int
    def __init__(self, min_staked_base_tokens: _Optional[bytes] = ..., fee_discount_ppm: _Optional[int] = ...) -> None: ...
