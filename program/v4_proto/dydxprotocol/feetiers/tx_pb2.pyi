from v4_proto.cosmos_proto import cosmos_pb2 as _cosmos_pb2
from v4_proto.cosmos.msg.v1 import msg_pb2 as _msg_pb2
from v4_proto.dydxprotocol.feetiers import params_pb2 as _params_pb2
from v4_proto.dydxprotocol.feetiers import per_market_fee_discount_pb2 as _per_market_fee_discount_pb2
from v4_proto.dydxprotocol.feetiers import staking_tier_pb2 as _staking_tier_pb2
from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MsgUpdatePerpetualFeeParams(_message.Message):
    __slots__ = ("authority", "params")
    AUTHORITY_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    authority: str
    params: _params_pb2.PerpetualFeeParams
    def __init__(self, authority: _Optional[str] = ..., params: _Optional[_Union[_params_pb2.PerpetualFeeParams, _Mapping]] = ...) -> None: ...

class MsgUpdatePerpetualFeeParamsResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class MsgSetMarketFeeDiscountParams(_message.Message):
    __slots__ = ("authority", "params")
    AUTHORITY_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    authority: str
    params: _containers.RepeatedCompositeFieldContainer[_per_market_fee_discount_pb2.PerMarketFeeDiscountParams]
    def __init__(self, authority: _Optional[str] = ..., params: _Optional[_Iterable[_Union[_per_market_fee_discount_pb2.PerMarketFeeDiscountParams, _Mapping]]] = ...) -> None: ...

class MsgSetMarketFeeDiscountParamsResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class MsgSetStakingTiers(_message.Message):
    __slots__ = ("authority", "staking_tiers")
    AUTHORITY_FIELD_NUMBER: _ClassVar[int]
    STAKING_TIERS_FIELD_NUMBER: _ClassVar[int]
    authority: str
    staking_tiers: _containers.RepeatedCompositeFieldContainer[_staking_tier_pb2.StakingTier]
    def __init__(self, authority: _Optional[str] = ..., staking_tiers: _Optional[_Iterable[_Union[_staking_tier_pb2.StakingTier, _Mapping]]] = ...) -> None: ...

class MsgSetStakingTiersResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
