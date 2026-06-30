import datetime

from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AffiliateAttribution(_message.Message):
    __slots__ = ("role", "referrer_address", "referred_volume_quote_quantums")
    class Role(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        ROLE_UNSPECIFIED: _ClassVar[AffiliateAttribution.Role]
        ROLE_TAKER: _ClassVar[AffiliateAttribution.Role]
        ROLE_MAKER: _ClassVar[AffiliateAttribution.Role]
    ROLE_UNSPECIFIED: AffiliateAttribution.Role
    ROLE_TAKER: AffiliateAttribution.Role
    ROLE_MAKER: AffiliateAttribution.Role
    ROLE_FIELD_NUMBER: _ClassVar[int]
    REFERRER_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    REFERRED_VOLUME_QUOTE_QUANTUMS_FIELD_NUMBER: _ClassVar[int]
    role: AffiliateAttribution.Role
    referrer_address: str
    referred_volume_quote_quantums: int
    def __init__(self, role: _Optional[_Union[AffiliateAttribution.Role, str]] = ..., referrer_address: _Optional[str] = ..., referred_volume_quote_quantums: _Optional[int] = ...) -> None: ...

class BlockStats(_message.Message):
    __slots__ = ("fills",)
    class Fill(_message.Message):
        __slots__ = ("taker", "maker", "notional", "affiliate_fee_generated_quantums", "affiliate_attributions")
        TAKER_FIELD_NUMBER: _ClassVar[int]
        MAKER_FIELD_NUMBER: _ClassVar[int]
        NOTIONAL_FIELD_NUMBER: _ClassVar[int]
        AFFILIATE_FEE_GENERATED_QUANTUMS_FIELD_NUMBER: _ClassVar[int]
        AFFILIATE_ATTRIBUTIONS_FIELD_NUMBER: _ClassVar[int]
        taker: str
        maker: str
        notional: int
        affiliate_fee_generated_quantums: int
        affiliate_attributions: _containers.RepeatedCompositeFieldContainer[AffiliateAttribution]
        def __init__(self, taker: _Optional[str] = ..., maker: _Optional[str] = ..., notional: _Optional[int] = ..., affiliate_fee_generated_quantums: _Optional[int] = ..., affiliate_attributions: _Optional[_Iterable[_Union[AffiliateAttribution, _Mapping]]] = ...) -> None: ...
    FILLS_FIELD_NUMBER: _ClassVar[int]
    fills: _containers.RepeatedCompositeFieldContainer[BlockStats.Fill]
    def __init__(self, fills: _Optional[_Iterable[_Union[BlockStats.Fill, _Mapping]]] = ...) -> None: ...

class StatsMetadata(_message.Message):
    __slots__ = ("trailing_epoch",)
    TRAILING_EPOCH_FIELD_NUMBER: _ClassVar[int]
    trailing_epoch: int
    def __init__(self, trailing_epoch: _Optional[int] = ...) -> None: ...

class EpochStats(_message.Message):
    __slots__ = ("epoch_end_time", "stats")
    class UserWithStats(_message.Message):
        __slots__ = ("user", "stats")
        USER_FIELD_NUMBER: _ClassVar[int]
        STATS_FIELD_NUMBER: _ClassVar[int]
        user: str
        stats: UserStats
        def __init__(self, user: _Optional[str] = ..., stats: _Optional[_Union[UserStats, _Mapping]] = ...) -> None: ...
    EPOCH_END_TIME_FIELD_NUMBER: _ClassVar[int]
    STATS_FIELD_NUMBER: _ClassVar[int]
    epoch_end_time: _timestamp_pb2.Timestamp
    stats: _containers.RepeatedCompositeFieldContainer[EpochStats.UserWithStats]
    def __init__(self, epoch_end_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., stats: _Optional[_Iterable[_Union[EpochStats.UserWithStats, _Mapping]]] = ...) -> None: ...

class GlobalStats(_message.Message):
    __slots__ = ("notional_traded",)
    NOTIONAL_TRADED_FIELD_NUMBER: _ClassVar[int]
    notional_traded: int
    def __init__(self, notional_traded: _Optional[int] = ...) -> None: ...

class UserStats(_message.Message):
    __slots__ = ("taker_notional", "maker_notional", "affiliate_30d_revenue_generated_quantums", "affiliate_30d_referred_volume_quote_quantums", "affiliate_30d_attributed_volume_quote_quantums")
    TAKER_NOTIONAL_FIELD_NUMBER: _ClassVar[int]
    MAKER_NOTIONAL_FIELD_NUMBER: _ClassVar[int]
    AFFILIATE_30D_REVENUE_GENERATED_QUANTUMS_FIELD_NUMBER: _ClassVar[int]
    AFFILIATE_30D_REFERRED_VOLUME_QUOTE_QUANTUMS_FIELD_NUMBER: _ClassVar[int]
    AFFILIATE_30D_ATTRIBUTED_VOLUME_QUOTE_QUANTUMS_FIELD_NUMBER: _ClassVar[int]
    taker_notional: int
    maker_notional: int
    affiliate_30d_revenue_generated_quantums: int
    affiliate_30d_referred_volume_quote_quantums: int
    affiliate_30d_attributed_volume_quote_quantums: int
    def __init__(self, taker_notional: _Optional[int] = ..., maker_notional: _Optional[int] = ..., affiliate_30d_revenue_generated_quantums: _Optional[int] = ..., affiliate_30d_referred_volume_quote_quantums: _Optional[int] = ..., affiliate_30d_attributed_volume_quote_quantums: _Optional[int] = ...) -> None: ...

class CachedStakedBaseTokens(_message.Message):
    __slots__ = ("staked_base_tokens", "cached_at")
    STAKED_BASE_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CACHED_AT_FIELD_NUMBER: _ClassVar[int]
    staked_base_tokens: bytes
    cached_at: int
    def __init__(self, staked_base_tokens: _Optional[bytes] = ..., cached_at: _Optional[int] = ...) -> None: ...
