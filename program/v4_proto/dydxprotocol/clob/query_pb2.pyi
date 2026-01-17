from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from v4_proto.google.api import annotations_pb2 as _annotations_pb2
from v4_proto.cosmos.base.query.v1beta1 import pagination_pb2 as _pagination_pb2
from v4_proto.dydxprotocol.clob import block_rate_limit_config_pb2 as _block_rate_limit_config_pb2
from v4_proto.dydxprotocol.clob import clob_pair_pb2 as _clob_pair_pb2
from v4_proto.dydxprotocol.clob import equity_tier_limit_config_pb2 as _equity_tier_limit_config_pb2
from v4_proto.dydxprotocol.clob import order_pb2 as _order_pb2
from v4_proto.dydxprotocol.clob import matches_pb2 as _matches_pb2
from v4_proto.dydxprotocol.clob import liquidations_config_pb2 as _liquidations_config_pb2
from v4_proto.dydxprotocol.clob import mev_pb2 as _mev_pb2
from v4_proto.dydxprotocol.indexer.off_chain_updates import off_chain_updates_pb2 as _off_chain_updates_pb2
from v4_proto.dydxprotocol.subaccounts import streaming_pb2 as _streaming_pb2
from v4_proto.dydxprotocol.subaccounts import subaccount_pb2 as _subaccount_pb2
from v4_proto.dydxprotocol.prices import streaming_pb2 as _streaming_pb2_1
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class QueryGetClobPairRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...

class QueryClobPairResponse(_message.Message):
    __slots__ = ("clob_pair",)
    CLOB_PAIR_FIELD_NUMBER: _ClassVar[int]
    clob_pair: _clob_pair_pb2.ClobPair
    def __init__(self, clob_pair: _Optional[_Union[_clob_pair_pb2.ClobPair, _Mapping]] = ...) -> None: ...

class QueryAllClobPairRequest(_message.Message):
    __slots__ = ("pagination",)
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    pagination: _pagination_pb2.PageRequest
    def __init__(self, pagination: _Optional[_Union[_pagination_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class QueryClobPairAllResponse(_message.Message):
    __slots__ = ("clob_pair", "pagination")
    CLOB_PAIR_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    clob_pair: _containers.RepeatedCompositeFieldContainer[_clob_pair_pb2.ClobPair]
    pagination: _pagination_pb2.PageResponse
    def __init__(self, clob_pair: _Optional[_Iterable[_Union[_clob_pair_pb2.ClobPair, _Mapping]]] = ..., pagination: _Optional[_Union[_pagination_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class MevNodeToNodeCalculationRequest(_message.Message):
    __slots__ = ("block_proposer_matches", "validator_mev_metrics")
    BLOCK_PROPOSER_MATCHES_FIELD_NUMBER: _ClassVar[int]
    VALIDATOR_MEV_METRICS_FIELD_NUMBER: _ClassVar[int]
    block_proposer_matches: _mev_pb2.ValidatorMevMatches
    validator_mev_metrics: _mev_pb2.MevNodeToNodeMetrics
    def __init__(self, block_proposer_matches: _Optional[_Union[_mev_pb2.ValidatorMevMatches, _Mapping]] = ..., validator_mev_metrics: _Optional[_Union[_mev_pb2.MevNodeToNodeMetrics, _Mapping]] = ...) -> None: ...

class MevNodeToNodeCalculationResponse(_message.Message):
    __slots__ = ("results",)
    class MevAndVolumePerClob(_message.Message):
        __slots__ = ("clob_pair_id", "mev", "volume")
        CLOB_PAIR_ID_FIELD_NUMBER: _ClassVar[int]
        MEV_FIELD_NUMBER: _ClassVar[int]
        VOLUME_FIELD_NUMBER: _ClassVar[int]
        clob_pair_id: int
        mev: float
        volume: int
        def __init__(self, clob_pair_id: _Optional[int] = ..., mev: _Optional[float] = ..., volume: _Optional[int] = ...) -> None: ...
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[MevNodeToNodeCalculationResponse.MevAndVolumePerClob]
    def __init__(self, results: _Optional[_Iterable[_Union[MevNodeToNodeCalculationResponse.MevAndVolumePerClob, _Mapping]]] = ...) -> None: ...

class QueryEquityTierLimitConfigurationRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class QueryEquityTierLimitConfigurationResponse(_message.Message):
    __slots__ = ("equity_tier_limit_config",)
    EQUITY_TIER_LIMIT_CONFIG_FIELD_NUMBER: _ClassVar[int]
    equity_tier_limit_config: _equity_tier_limit_config_pb2.EquityTierLimitConfiguration
    def __init__(self, equity_tier_limit_config: _Optional[_Union[_equity_tier_limit_config_pb2.EquityTierLimitConfiguration, _Mapping]] = ...) -> None: ...

class QueryBlockRateLimitConfigurationRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class QueryBlockRateLimitConfigurationResponse(_message.Message):
    __slots__ = ("block_rate_limit_config",)
    BLOCK_RATE_LIMIT_CONFIG_FIELD_NUMBER: _ClassVar[int]
    block_rate_limit_config: _block_rate_limit_config_pb2.BlockRateLimitConfiguration
    def __init__(self, block_rate_limit_config: _Optional[_Union[_block_rate_limit_config_pb2.BlockRateLimitConfiguration, _Mapping]] = ...) -> None: ...

class QueryStatefulOrderRequest(_message.Message):
    __slots__ = ("order_id",)
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    order_id: _order_pb2.OrderId
    def __init__(self, order_id: _Optional[_Union[_order_pb2.OrderId, _Mapping]] = ...) -> None: ...

class QueryStatefulOrderResponse(_message.Message):
    __slots__ = ("order_placement", "fill_amount", "triggered")
    ORDER_PLACEMENT_FIELD_NUMBER: _ClassVar[int]
    FILL_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    TRIGGERED_FIELD_NUMBER: _ClassVar[int]
    order_placement: _order_pb2.LongTermOrderPlacement
    fill_amount: int
    triggered: bool
    def __init__(self, order_placement: _Optional[_Union[_order_pb2.LongTermOrderPlacement, _Mapping]] = ..., fill_amount: _Optional[int] = ..., triggered: bool = ...) -> None: ...

class QueryLiquidationsConfigurationRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class QueryLiquidationsConfigurationResponse(_message.Message):
    __slots__ = ("liquidations_config",)
    LIQUIDATIONS_CONFIG_FIELD_NUMBER: _ClassVar[int]
    liquidations_config: _liquidations_config_pb2.LiquidationsConfig
    def __init__(self, liquidations_config: _Optional[_Union[_liquidations_config_pb2.LiquidationsConfig, _Mapping]] = ...) -> None: ...

class QueryNextClobPairIdRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class QueryNextClobPairIdResponse(_message.Message):
    __slots__ = ("next_clob_pair_id",)
    NEXT_CLOB_PAIR_ID_FIELD_NUMBER: _ClassVar[int]
    next_clob_pair_id: int
    def __init__(self, next_clob_pair_id: _Optional[int] = ...) -> None: ...

class QueryLeverageRequest(_message.Message):
    __slots__ = ("owner", "number")
    OWNER_FIELD_NUMBER: _ClassVar[int]
    NUMBER_FIELD_NUMBER: _ClassVar[int]
    owner: str
    number: int
    def __init__(self, owner: _Optional[str] = ..., number: _Optional[int] = ...) -> None: ...

class QueryLeverageResponse(_message.Message):
    __slots__ = ("clob_pair_leverage",)
    CLOB_PAIR_LEVERAGE_FIELD_NUMBER: _ClassVar[int]
    clob_pair_leverage: _containers.RepeatedCompositeFieldContainer[ClobPairLeverageInfo]
    def __init__(self, clob_pair_leverage: _Optional[_Iterable[_Union[ClobPairLeverageInfo, _Mapping]]] = ...) -> None: ...

class ClobPairLeverageInfo(_message.Message):
    __slots__ = ("clob_pair_id", "custom_imf_ppm")
    CLOB_PAIR_ID_FIELD_NUMBER: _ClassVar[int]
    CUSTOM_IMF_PPM_FIELD_NUMBER: _ClassVar[int]
    clob_pair_id: int
    custom_imf_ppm: int
    def __init__(self, clob_pair_id: _Optional[int] = ..., custom_imf_ppm: _Optional[int] = ...) -> None: ...

class StreamOrderbookUpdatesRequest(_message.Message):
    __slots__ = ("clob_pair_id", "subaccount_ids", "market_ids", "filter_orders_by_subaccount_id")
    CLOB_PAIR_ID_FIELD_NUMBER: _ClassVar[int]
    SUBACCOUNT_IDS_FIELD_NUMBER: _ClassVar[int]
    MARKET_IDS_FIELD_NUMBER: _ClassVar[int]
    FILTER_ORDERS_BY_SUBACCOUNT_ID_FIELD_NUMBER: _ClassVar[int]
    clob_pair_id: _containers.RepeatedScalarFieldContainer[int]
    subaccount_ids: _containers.RepeatedCompositeFieldContainer[_subaccount_pb2.SubaccountId]
    market_ids: _containers.RepeatedScalarFieldContainer[int]
    filter_orders_by_subaccount_id: bool
    def __init__(self, clob_pair_id: _Optional[_Iterable[int]] = ..., subaccount_ids: _Optional[_Iterable[_Union[_subaccount_pb2.SubaccountId, _Mapping]]] = ..., market_ids: _Optional[_Iterable[int]] = ..., filter_orders_by_subaccount_id: bool = ...) -> None: ...

class StreamOrderbookUpdatesResponse(_message.Message):
    __slots__ = ("updates",)
    UPDATES_FIELD_NUMBER: _ClassVar[int]
    updates: _containers.RepeatedCompositeFieldContainer[StreamUpdate]
    def __init__(self, updates: _Optional[_Iterable[_Union[StreamUpdate, _Mapping]]] = ...) -> None: ...

class StreamUpdate(_message.Message):
    __slots__ = ("block_height", "exec_mode", "orderbook_update", "order_fill", "taker_order", "subaccount_update", "price_update")
    BLOCK_HEIGHT_FIELD_NUMBER: _ClassVar[int]
    EXEC_MODE_FIELD_NUMBER: _ClassVar[int]
    ORDERBOOK_UPDATE_FIELD_NUMBER: _ClassVar[int]
    ORDER_FILL_FIELD_NUMBER: _ClassVar[int]
    TAKER_ORDER_FIELD_NUMBER: _ClassVar[int]
    SUBACCOUNT_UPDATE_FIELD_NUMBER: _ClassVar[int]
    PRICE_UPDATE_FIELD_NUMBER: _ClassVar[int]
    block_height: int
    exec_mode: int
    orderbook_update: StreamOrderbookUpdate
    order_fill: StreamOrderbookFill
    taker_order: StreamTakerOrder
    subaccount_update: _streaming_pb2.StreamSubaccountUpdate
    price_update: _streaming_pb2_1.StreamPriceUpdate
    def __init__(self, block_height: _Optional[int] = ..., exec_mode: _Optional[int] = ..., orderbook_update: _Optional[_Union[StreamOrderbookUpdate, _Mapping]] = ..., order_fill: _Optional[_Union[StreamOrderbookFill, _Mapping]] = ..., taker_order: _Optional[_Union[StreamTakerOrder, _Mapping]] = ..., subaccount_update: _Optional[_Union[_streaming_pb2.StreamSubaccountUpdate, _Mapping]] = ..., price_update: _Optional[_Union[_streaming_pb2_1.StreamPriceUpdate, _Mapping]] = ...) -> None: ...

class StreamOrderbookUpdate(_message.Message):
    __slots__ = ("snapshot", "updates")
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    UPDATES_FIELD_NUMBER: _ClassVar[int]
    snapshot: bool
    updates: _containers.RepeatedCompositeFieldContainer[_off_chain_updates_pb2.OffChainUpdateV1]
    def __init__(self, snapshot: bool = ..., updates: _Optional[_Iterable[_Union[_off_chain_updates_pb2.OffChainUpdateV1, _Mapping]]] = ...) -> None: ...

class StreamOrderbookFill(_message.Message):
    __slots__ = ("clob_match", "orders", "fill_amounts")
    CLOB_MATCH_FIELD_NUMBER: _ClassVar[int]
    ORDERS_FIELD_NUMBER: _ClassVar[int]
    FILL_AMOUNTS_FIELD_NUMBER: _ClassVar[int]
    clob_match: _matches_pb2.ClobMatch
    orders: _containers.RepeatedCompositeFieldContainer[_order_pb2.Order]
    fill_amounts: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, clob_match: _Optional[_Union[_matches_pb2.ClobMatch, _Mapping]] = ..., orders: _Optional[_Iterable[_Union[_order_pb2.Order, _Mapping]]] = ..., fill_amounts: _Optional[_Iterable[int]] = ...) -> None: ...

class StreamTakerOrder(_message.Message):
    __slots__ = ("order", "liquidation_order", "taker_order_status")
    ORDER_FIELD_NUMBER: _ClassVar[int]
    LIQUIDATION_ORDER_FIELD_NUMBER: _ClassVar[int]
    TAKER_ORDER_STATUS_FIELD_NUMBER: _ClassVar[int]
    order: _order_pb2.Order
    liquidation_order: _order_pb2.StreamLiquidationOrder
    taker_order_status: StreamTakerOrderStatus
    def __init__(self, order: _Optional[_Union[_order_pb2.Order, _Mapping]] = ..., liquidation_order: _Optional[_Union[_order_pb2.StreamLiquidationOrder, _Mapping]] = ..., taker_order_status: _Optional[_Union[StreamTakerOrderStatus, _Mapping]] = ...) -> None: ...

class StreamTakerOrderStatus(_message.Message):
    __slots__ = ("order_status", "remaining_quantums", "optimistically_filled_quantums")
    ORDER_STATUS_FIELD_NUMBER: _ClassVar[int]
    REMAINING_QUANTUMS_FIELD_NUMBER: _ClassVar[int]
    OPTIMISTICALLY_FILLED_QUANTUMS_FIELD_NUMBER: _ClassVar[int]
    order_status: int
    remaining_quantums: int
    optimistically_filled_quantums: int
    def __init__(self, order_status: _Optional[int] = ..., remaining_quantums: _Optional[int] = ..., optimistically_filled_quantums: _Optional[int] = ...) -> None: ...
