from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from v4_proto.dydxprotocol.clob import block_rate_limit_config_pb2 as _block_rate_limit_config_pb2
from v4_proto.dydxprotocol.clob import clob_pair_pb2 as _clob_pair_pb2
from v4_proto.dydxprotocol.clob import equity_tier_limit_config_pb2 as _equity_tier_limit_config_pb2
from v4_proto.dydxprotocol.clob import liquidations_config_pb2 as _liquidations_config_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GenesisState(_message.Message):
    __slots__ = ("clob_pairs", "liquidations_config", "block_rate_limit_config", "equity_tier_limit_config")
    CLOB_PAIRS_FIELD_NUMBER: _ClassVar[int]
    LIQUIDATIONS_CONFIG_FIELD_NUMBER: _ClassVar[int]
    BLOCK_RATE_LIMIT_CONFIG_FIELD_NUMBER: _ClassVar[int]
    EQUITY_TIER_LIMIT_CONFIG_FIELD_NUMBER: _ClassVar[int]
    clob_pairs: _containers.RepeatedCompositeFieldContainer[_clob_pair_pb2.ClobPair]
    liquidations_config: _liquidations_config_pb2.LiquidationsConfig
    block_rate_limit_config: _block_rate_limit_config_pb2.BlockRateLimitConfiguration
    equity_tier_limit_config: _equity_tier_limit_config_pb2.EquityTierLimitConfiguration
    def __init__(self, clob_pairs: _Optional[_Iterable[_Union[_clob_pair_pb2.ClobPair, _Mapping]]] = ..., liquidations_config: _Optional[_Union[_liquidations_config_pb2.LiquidationsConfig, _Mapping]] = ..., block_rate_limit_config: _Optional[_Union[_block_rate_limit_config_pb2.BlockRateLimitConfiguration, _Mapping]] = ..., equity_tier_limit_config: _Optional[_Union[_equity_tier_limit_config_pb2.EquityTierLimitConfiguration, _Mapping]] = ...) -> None: ...
