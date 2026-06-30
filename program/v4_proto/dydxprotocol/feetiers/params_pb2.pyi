from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PerpetualFeeParams(_message.Message):
    __slots__ = ("tiers",)
    TIERS_FIELD_NUMBER: _ClassVar[int]
    tiers: _containers.RepeatedCompositeFieldContainer[PerpetualFeeTier]
    def __init__(self, tiers: _Optional[_Iterable[_Union[PerpetualFeeTier, _Mapping]]] = ...) -> None: ...

class PerpetualFeeTier(_message.Message):
    __slots__ = ("name", "absolute_volume_requirement", "total_volume_share_requirement_ppm", "maker_volume_share_requirement_ppm", "maker_fee_ppm", "taker_fee_ppm")
    NAME_FIELD_NUMBER: _ClassVar[int]
    ABSOLUTE_VOLUME_REQUIREMENT_FIELD_NUMBER: _ClassVar[int]
    TOTAL_VOLUME_SHARE_REQUIREMENT_PPM_FIELD_NUMBER: _ClassVar[int]
    MAKER_VOLUME_SHARE_REQUIREMENT_PPM_FIELD_NUMBER: _ClassVar[int]
    MAKER_FEE_PPM_FIELD_NUMBER: _ClassVar[int]
    TAKER_FEE_PPM_FIELD_NUMBER: _ClassVar[int]
    name: str
    absolute_volume_requirement: int
    total_volume_share_requirement_ppm: int
    maker_volume_share_requirement_ppm: int
    maker_fee_ppm: int
    taker_fee_ppm: int
    def __init__(self, name: _Optional[str] = ..., absolute_volume_requirement: _Optional[int] = ..., total_volume_share_requirement_ppm: _Optional[int] = ..., maker_volume_share_requirement_ppm: _Optional[int] = ..., maker_fee_ppm: _Optional[int] = ..., taker_fee_ppm: _Optional[int] = ...) -> None: ...
