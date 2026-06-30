from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from typing import ClassVar as _ClassVar

DESCRIPTOR: _descriptor.FileDescriptor

class PerpetualMarketType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PERPETUAL_MARKET_TYPE_UNSPECIFIED: _ClassVar[PerpetualMarketType]
    PERPETUAL_MARKET_TYPE_CROSS: _ClassVar[PerpetualMarketType]
    PERPETUAL_MARKET_TYPE_ISOLATED: _ClassVar[PerpetualMarketType]
PERPETUAL_MARKET_TYPE_UNSPECIFIED: PerpetualMarketType
PERPETUAL_MARKET_TYPE_CROSS: PerpetualMarketType
PERPETUAL_MARKET_TYPE_ISOLATED: PerpetualMarketType
