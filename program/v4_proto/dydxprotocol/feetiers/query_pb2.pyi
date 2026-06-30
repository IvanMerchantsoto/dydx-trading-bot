from v4_proto.cosmos_proto import cosmos_pb2 as _cosmos_pb2
from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from v4_proto.google.api import annotations_pb2 as _annotations_pb2
from v4_proto.dydxprotocol.feetiers import params_pb2 as _params_pb2
from v4_proto.dydxprotocol.feetiers import per_market_fee_discount_pb2 as _per_market_fee_discount_pb2
from v4_proto.dydxprotocol.feetiers import staking_tier_pb2 as _staking_tier_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class QueryPerpetualFeeParamsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class QueryPerpetualFeeParamsResponse(_message.Message):
    __slots__ = ("params",)
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    params: _params_pb2.PerpetualFeeParams
    def __init__(self, params: _Optional[_Union[_params_pb2.PerpetualFeeParams, _Mapping]] = ...) -> None: ...

class QueryUserFeeTierRequest(_message.Message):
    __slots__ = ("user",)
    USER_FIELD_NUMBER: _ClassVar[int]
    user: str
    def __init__(self, user: _Optional[str] = ...) -> None: ...

class QueryUserFeeTierResponse(_message.Message):
    __slots__ = ("index", "tier")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    TIER_FIELD_NUMBER: _ClassVar[int]
    index: int
    tier: _params_pb2.PerpetualFeeTier
    def __init__(self, index: _Optional[int] = ..., tier: _Optional[_Union[_params_pb2.PerpetualFeeTier, _Mapping]] = ...) -> None: ...

class QueryPerMarketFeeDiscountParamsRequest(_message.Message):
    __slots__ = ("clob_pair_id",)
    CLOB_PAIR_ID_FIELD_NUMBER: _ClassVar[int]
    clob_pair_id: int
    def __init__(self, clob_pair_id: _Optional[int] = ...) -> None: ...

class QueryPerMarketFeeDiscountParamsResponse(_message.Message):
    __slots__ = ("params",)
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    params: _per_market_fee_discount_pb2.PerMarketFeeDiscountParams
    def __init__(self, params: _Optional[_Union[_per_market_fee_discount_pb2.PerMarketFeeDiscountParams, _Mapping]] = ...) -> None: ...

class QueryAllMarketFeeDiscountParamsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class QueryAllMarketFeeDiscountParamsResponse(_message.Message):
    __slots__ = ("params",)
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    params: _containers.RepeatedCompositeFieldContainer[_per_market_fee_discount_pb2.PerMarketFeeDiscountParams]
    def __init__(self, params: _Optional[_Iterable[_Union[_per_market_fee_discount_pb2.PerMarketFeeDiscountParams, _Mapping]]] = ...) -> None: ...

class QueryStakingTiersRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class QueryStakingTiersResponse(_message.Message):
    __slots__ = ("staking_tiers",)
    STAKING_TIERS_FIELD_NUMBER: _ClassVar[int]
    staking_tiers: _containers.RepeatedCompositeFieldContainer[_staking_tier_pb2.StakingTier]
    def __init__(self, staking_tiers: _Optional[_Iterable[_Union[_staking_tier_pb2.StakingTier, _Mapping]]] = ...) -> None: ...

class QueryUserStakingTierRequest(_message.Message):
    __slots__ = ("address",)
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    address: str
    def __init__(self, address: _Optional[str] = ...) -> None: ...

class QueryUserStakingTierResponse(_message.Message):
    __slots__ = ("fee_tier_name", "staked_base_tokens", "discount_ppm")
    FEE_TIER_NAME_FIELD_NUMBER: _ClassVar[int]
    STAKED_BASE_TOKENS_FIELD_NUMBER: _ClassVar[int]
    DISCOUNT_PPM_FIELD_NUMBER: _ClassVar[int]
    fee_tier_name: str
    staked_base_tokens: bytes
    discount_ppm: int
    def __init__(self, fee_tier_name: _Optional[str] = ..., staked_base_tokens: _Optional[bytes] = ..., discount_ppm: _Optional[int] = ...) -> None: ...
