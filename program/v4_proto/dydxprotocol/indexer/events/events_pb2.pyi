from v4_proto.cosmos_proto import cosmos_pb2 as _cosmos_pb2
from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from v4_proto.dydxprotocol.indexer.shared import removal_reason_pb2 as _removal_reason_pb2
from v4_proto.dydxprotocol.indexer.protocol.v1 import clob_pb2 as _clob_pb2
from v4_proto.dydxprotocol.indexer.protocol.v1 import perpetual_pb2 as _perpetual_pb2
from v4_proto.dydxprotocol.indexer.protocol.v1 import subaccount_pb2 as _subaccount_pb2
from v4_proto.dydxprotocol.indexer.protocol.v1 import vault_pb2 as _vault_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class FundingUpdateV1(_message.Message):
    __slots__ = ("perpetual_id", "funding_value_ppm", "funding_index")
    PERPETUAL_ID_FIELD_NUMBER: _ClassVar[int]
    FUNDING_VALUE_PPM_FIELD_NUMBER: _ClassVar[int]
    FUNDING_INDEX_FIELD_NUMBER: _ClassVar[int]
    perpetual_id: int
    funding_value_ppm: int
    funding_index: bytes
    def __init__(self, perpetual_id: _Optional[int] = ..., funding_value_ppm: _Optional[int] = ..., funding_index: _Optional[bytes] = ...) -> None: ...

class FundingEventV1(_message.Message):
    __slots__ = ("updates", "type")
    class Type(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        TYPE_UNSPECIFIED: _ClassVar[FundingEventV1.Type]
        TYPE_PREMIUM_SAMPLE: _ClassVar[FundingEventV1.Type]
        TYPE_FUNDING_RATE_AND_INDEX: _ClassVar[FundingEventV1.Type]
        TYPE_PREMIUM_VOTE: _ClassVar[FundingEventV1.Type]
    TYPE_UNSPECIFIED: FundingEventV1.Type
    TYPE_PREMIUM_SAMPLE: FundingEventV1.Type
    TYPE_FUNDING_RATE_AND_INDEX: FundingEventV1.Type
    TYPE_PREMIUM_VOTE: FundingEventV1.Type
    UPDATES_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    updates: _containers.RepeatedCompositeFieldContainer[FundingUpdateV1]
    type: FundingEventV1.Type
    def __init__(self, updates: _Optional[_Iterable[_Union[FundingUpdateV1, _Mapping]]] = ..., type: _Optional[_Union[FundingEventV1.Type, str]] = ...) -> None: ...

class MarketEventV1(_message.Message):
    __slots__ = ("market_id", "price_update", "market_create", "market_modify")
    MARKET_ID_FIELD_NUMBER: _ClassVar[int]
    PRICE_UPDATE_FIELD_NUMBER: _ClassVar[int]
    MARKET_CREATE_FIELD_NUMBER: _ClassVar[int]
    MARKET_MODIFY_FIELD_NUMBER: _ClassVar[int]
    market_id: int
    price_update: MarketPriceUpdateEventV1
    market_create: MarketCreateEventV1
    market_modify: MarketModifyEventV1
    def __init__(self, market_id: _Optional[int] = ..., price_update: _Optional[_Union[MarketPriceUpdateEventV1, _Mapping]] = ..., market_create: _Optional[_Union[MarketCreateEventV1, _Mapping]] = ..., market_modify: _Optional[_Union[MarketModifyEventV1, _Mapping]] = ...) -> None: ...

class MarketPriceUpdateEventV1(_message.Message):
    __slots__ = ("price_with_exponent",)
    PRICE_WITH_EXPONENT_FIELD_NUMBER: _ClassVar[int]
    price_with_exponent: int
    def __init__(self, price_with_exponent: _Optional[int] = ...) -> None: ...

class MarketBaseEventV1(_message.Message):
    __slots__ = ("pair", "min_price_change_ppm")
    PAIR_FIELD_NUMBER: _ClassVar[int]
    MIN_PRICE_CHANGE_PPM_FIELD_NUMBER: _ClassVar[int]
    pair: str
    min_price_change_ppm: int
    def __init__(self, pair: _Optional[str] = ..., min_price_change_ppm: _Optional[int] = ...) -> None: ...

class MarketCreateEventV1(_message.Message):
    __slots__ = ("base", "exponent")
    BASE_FIELD_NUMBER: _ClassVar[int]
    EXPONENT_FIELD_NUMBER: _ClassVar[int]
    base: MarketBaseEventV1
    exponent: int
    def __init__(self, base: _Optional[_Union[MarketBaseEventV1, _Mapping]] = ..., exponent: _Optional[int] = ...) -> None: ...

class MarketModifyEventV1(_message.Message):
    __slots__ = ("base",)
    BASE_FIELD_NUMBER: _ClassVar[int]
    base: MarketBaseEventV1
    def __init__(self, base: _Optional[_Union[MarketBaseEventV1, _Mapping]] = ...) -> None: ...

class SourceOfFunds(_message.Message):
    __slots__ = ("subaccount_id", "address")
    SUBACCOUNT_ID_FIELD_NUMBER: _ClassVar[int]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    subaccount_id: _subaccount_pb2.IndexerSubaccountId
    address: str
    def __init__(self, subaccount_id: _Optional[_Union[_subaccount_pb2.IndexerSubaccountId, _Mapping]] = ..., address: _Optional[str] = ...) -> None: ...

class TransferEventV1(_message.Message):
    __slots__ = ("sender_subaccount_id", "recipient_subaccount_id", "asset_id", "amount", "sender", "recipient")
    SENDER_SUBACCOUNT_ID_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_SUBACCOUNT_ID_FIELD_NUMBER: _ClassVar[int]
    ASSET_ID_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    SENDER_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_FIELD_NUMBER: _ClassVar[int]
    sender_subaccount_id: _subaccount_pb2.IndexerSubaccountId
    recipient_subaccount_id: _subaccount_pb2.IndexerSubaccountId
    asset_id: int
    amount: int
    sender: SourceOfFunds
    recipient: SourceOfFunds
    def __init__(self, sender_subaccount_id: _Optional[_Union[_subaccount_pb2.IndexerSubaccountId, _Mapping]] = ..., recipient_subaccount_id: _Optional[_Union[_subaccount_pb2.IndexerSubaccountId, _Mapping]] = ..., asset_id: _Optional[int] = ..., amount: _Optional[int] = ..., sender: _Optional[_Union[SourceOfFunds, _Mapping]] = ..., recipient: _Optional[_Union[SourceOfFunds, _Mapping]] = ...) -> None: ...

class OrderFillEventV1(_message.Message):
    __slots__ = ("maker_order", "order", "liquidation_order", "fill_amount", "maker_fee", "taker_fee", "total_filled_maker", "total_filled_taker", "affiliate_rev_share", "maker_builder_fee", "taker_builder_fee", "maker_builder_address", "taker_builder_address", "maker_order_router_fee", "taker_order_router_fee", "maker_order_router_address", "taker_order_router_address")
    MAKER_ORDER_FIELD_NUMBER: _ClassVar[int]
    ORDER_FIELD_NUMBER: _ClassVar[int]
    LIQUIDATION_ORDER_FIELD_NUMBER: _ClassVar[int]
    FILL_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    MAKER_FEE_FIELD_NUMBER: _ClassVar[int]
    TAKER_FEE_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FILLED_MAKER_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FILLED_TAKER_FIELD_NUMBER: _ClassVar[int]
    AFFILIATE_REV_SHARE_FIELD_NUMBER: _ClassVar[int]
    MAKER_BUILDER_FEE_FIELD_NUMBER: _ClassVar[int]
    TAKER_BUILDER_FEE_FIELD_NUMBER: _ClassVar[int]
    MAKER_BUILDER_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    TAKER_BUILDER_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    MAKER_ORDER_ROUTER_FEE_FIELD_NUMBER: _ClassVar[int]
    TAKER_ORDER_ROUTER_FEE_FIELD_NUMBER: _ClassVar[int]
    MAKER_ORDER_ROUTER_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    TAKER_ORDER_ROUTER_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    maker_order: _clob_pb2.IndexerOrder
    order: _clob_pb2.IndexerOrder
    liquidation_order: LiquidationOrderV1
    fill_amount: int
    maker_fee: int
    taker_fee: int
    total_filled_maker: int
    total_filled_taker: int
    affiliate_rev_share: int
    maker_builder_fee: int
    taker_builder_fee: int
    maker_builder_address: str
    taker_builder_address: str
    maker_order_router_fee: int
    taker_order_router_fee: int
    maker_order_router_address: str
    taker_order_router_address: str
    def __init__(self, maker_order: _Optional[_Union[_clob_pb2.IndexerOrder, _Mapping]] = ..., order: _Optional[_Union[_clob_pb2.IndexerOrder, _Mapping]] = ..., liquidation_order: _Optional[_Union[LiquidationOrderV1, _Mapping]] = ..., fill_amount: _Optional[int] = ..., maker_fee: _Optional[int] = ..., taker_fee: _Optional[int] = ..., total_filled_maker: _Optional[int] = ..., total_filled_taker: _Optional[int] = ..., affiliate_rev_share: _Optional[int] = ..., maker_builder_fee: _Optional[int] = ..., taker_builder_fee: _Optional[int] = ..., maker_builder_address: _Optional[str] = ..., taker_builder_address: _Optional[str] = ..., maker_order_router_fee: _Optional[int] = ..., taker_order_router_fee: _Optional[int] = ..., maker_order_router_address: _Optional[str] = ..., taker_order_router_address: _Optional[str] = ...) -> None: ...

class DeleveragingEventV1(_message.Message):
    __slots__ = ("liquidated", "offsetting", "perpetual_id", "fill_amount", "total_quote_quantums", "is_buy", "is_final_settlement")
    LIQUIDATED_FIELD_NUMBER: _ClassVar[int]
    OFFSETTING_FIELD_NUMBER: _ClassVar[int]
    PERPETUAL_ID_FIELD_NUMBER: _ClassVar[int]
    FILL_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    TOTAL_QUOTE_QUANTUMS_FIELD_NUMBER: _ClassVar[int]
    IS_BUY_FIELD_NUMBER: _ClassVar[int]
    IS_FINAL_SETTLEMENT_FIELD_NUMBER: _ClassVar[int]
    liquidated: _subaccount_pb2.IndexerSubaccountId
    offsetting: _subaccount_pb2.IndexerSubaccountId
    perpetual_id: int
    fill_amount: int
    total_quote_quantums: int
    is_buy: bool
    is_final_settlement: bool
    def __init__(self, liquidated: _Optional[_Union[_subaccount_pb2.IndexerSubaccountId, _Mapping]] = ..., offsetting: _Optional[_Union[_subaccount_pb2.IndexerSubaccountId, _Mapping]] = ..., perpetual_id: _Optional[int] = ..., fill_amount: _Optional[int] = ..., total_quote_quantums: _Optional[int] = ..., is_buy: bool = ..., is_final_settlement: bool = ...) -> None: ...

class LiquidationOrderV1(_message.Message):
    __slots__ = ("liquidated", "clob_pair_id", "perpetual_id", "total_size", "is_buy", "subticks")
    LIQUIDATED_FIELD_NUMBER: _ClassVar[int]
    CLOB_PAIR_ID_FIELD_NUMBER: _ClassVar[int]
    PERPETUAL_ID_FIELD_NUMBER: _ClassVar[int]
    TOTAL_SIZE_FIELD_NUMBER: _ClassVar[int]
    IS_BUY_FIELD_NUMBER: _ClassVar[int]
    SUBTICKS_FIELD_NUMBER: _ClassVar[int]
    liquidated: _subaccount_pb2.IndexerSubaccountId
    clob_pair_id: int
    perpetual_id: int
    total_size: int
    is_buy: bool
    subticks: int
    def __init__(self, liquidated: _Optional[_Union[_subaccount_pb2.IndexerSubaccountId, _Mapping]] = ..., clob_pair_id: _Optional[int] = ..., perpetual_id: _Optional[int] = ..., total_size: _Optional[int] = ..., is_buy: bool = ..., subticks: _Optional[int] = ...) -> None: ...

class SubaccountUpdateEventV1(_message.Message):
    __slots__ = ("subaccount_id", "updated_perpetual_positions", "updated_asset_positions")
    SUBACCOUNT_ID_FIELD_NUMBER: _ClassVar[int]
    UPDATED_PERPETUAL_POSITIONS_FIELD_NUMBER: _ClassVar[int]
    UPDATED_ASSET_POSITIONS_FIELD_NUMBER: _ClassVar[int]
    subaccount_id: _subaccount_pb2.IndexerSubaccountId
    updated_perpetual_positions: _containers.RepeatedCompositeFieldContainer[_subaccount_pb2.IndexerPerpetualPosition]
    updated_asset_positions: _containers.RepeatedCompositeFieldContainer[_subaccount_pb2.IndexerAssetPosition]
    def __init__(self, subaccount_id: _Optional[_Union[_subaccount_pb2.IndexerSubaccountId, _Mapping]] = ..., updated_perpetual_positions: _Optional[_Iterable[_Union[_subaccount_pb2.IndexerPerpetualPosition, _Mapping]]] = ..., updated_asset_positions: _Optional[_Iterable[_Union[_subaccount_pb2.IndexerAssetPosition, _Mapping]]] = ...) -> None: ...

class StatefulOrderEventV1(_message.Message):
    __slots__ = ("order_place", "order_removal", "conditional_order_placement", "conditional_order_triggered", "long_term_order_placement", "order_replacement", "twap_order_placement")
    class StatefulOrderPlacementV1(_message.Message):
        __slots__ = ("order",)
        ORDER_FIELD_NUMBER: _ClassVar[int]
        order: _clob_pb2.IndexerOrder
        def __init__(self, order: _Optional[_Union[_clob_pb2.IndexerOrder, _Mapping]] = ...) -> None: ...
    class StatefulOrderRemovalV1(_message.Message):
        __slots__ = ("removed_order_id", "reason")
        REMOVED_ORDER_ID_FIELD_NUMBER: _ClassVar[int]
        REASON_FIELD_NUMBER: _ClassVar[int]
        removed_order_id: _clob_pb2.IndexerOrderId
        reason: _removal_reason_pb2.OrderRemovalReason
        def __init__(self, removed_order_id: _Optional[_Union[_clob_pb2.IndexerOrderId, _Mapping]] = ..., reason: _Optional[_Union[_removal_reason_pb2.OrderRemovalReason, str]] = ...) -> None: ...
    class ConditionalOrderPlacementV1(_message.Message):
        __slots__ = ("order",)
        ORDER_FIELD_NUMBER: _ClassVar[int]
        order: _clob_pb2.IndexerOrder
        def __init__(self, order: _Optional[_Union[_clob_pb2.IndexerOrder, _Mapping]] = ...) -> None: ...
    class ConditionalOrderTriggeredV1(_message.Message):
        __slots__ = ("triggered_order_id",)
        TRIGGERED_ORDER_ID_FIELD_NUMBER: _ClassVar[int]
        triggered_order_id: _clob_pb2.IndexerOrderId
        def __init__(self, triggered_order_id: _Optional[_Union[_clob_pb2.IndexerOrderId, _Mapping]] = ...) -> None: ...
    class LongTermOrderPlacementV1(_message.Message):
        __slots__ = ("order",)
        ORDER_FIELD_NUMBER: _ClassVar[int]
        order: _clob_pb2.IndexerOrder
        def __init__(self, order: _Optional[_Union[_clob_pb2.IndexerOrder, _Mapping]] = ...) -> None: ...
    class LongTermOrderReplacementV1(_message.Message):
        __slots__ = ("old_order_id", "order")
        OLD_ORDER_ID_FIELD_NUMBER: _ClassVar[int]
        ORDER_FIELD_NUMBER: _ClassVar[int]
        old_order_id: _clob_pb2.IndexerOrderId
        order: _clob_pb2.IndexerOrder
        def __init__(self, old_order_id: _Optional[_Union[_clob_pb2.IndexerOrderId, _Mapping]] = ..., order: _Optional[_Union[_clob_pb2.IndexerOrder, _Mapping]] = ...) -> None: ...
    class TwapOrderPlacementV1(_message.Message):
        __slots__ = ("order",)
        ORDER_FIELD_NUMBER: _ClassVar[int]
        order: _clob_pb2.IndexerOrder
        def __init__(self, order: _Optional[_Union[_clob_pb2.IndexerOrder, _Mapping]] = ...) -> None: ...
    ORDER_PLACE_FIELD_NUMBER: _ClassVar[int]
    ORDER_REMOVAL_FIELD_NUMBER: _ClassVar[int]
    CONDITIONAL_ORDER_PLACEMENT_FIELD_NUMBER: _ClassVar[int]
    CONDITIONAL_ORDER_TRIGGERED_FIELD_NUMBER: _ClassVar[int]
    LONG_TERM_ORDER_PLACEMENT_FIELD_NUMBER: _ClassVar[int]
    ORDER_REPLACEMENT_FIELD_NUMBER: _ClassVar[int]
    TWAP_ORDER_PLACEMENT_FIELD_NUMBER: _ClassVar[int]
    order_place: StatefulOrderEventV1.StatefulOrderPlacementV1
    order_removal: StatefulOrderEventV1.StatefulOrderRemovalV1
    conditional_order_placement: StatefulOrderEventV1.ConditionalOrderPlacementV1
    conditional_order_triggered: StatefulOrderEventV1.ConditionalOrderTriggeredV1
    long_term_order_placement: StatefulOrderEventV1.LongTermOrderPlacementV1
    order_replacement: StatefulOrderEventV1.LongTermOrderReplacementV1
    twap_order_placement: StatefulOrderEventV1.TwapOrderPlacementV1
    def __init__(self, order_place: _Optional[_Union[StatefulOrderEventV1.StatefulOrderPlacementV1, _Mapping]] = ..., order_removal: _Optional[_Union[StatefulOrderEventV1.StatefulOrderRemovalV1, _Mapping]] = ..., conditional_order_placement: _Optional[_Union[StatefulOrderEventV1.ConditionalOrderPlacementV1, _Mapping]] = ..., conditional_order_triggered: _Optional[_Union[StatefulOrderEventV1.ConditionalOrderTriggeredV1, _Mapping]] = ..., long_term_order_placement: _Optional[_Union[StatefulOrderEventV1.LongTermOrderPlacementV1, _Mapping]] = ..., order_replacement: _Optional[_Union[StatefulOrderEventV1.LongTermOrderReplacementV1, _Mapping]] = ..., twap_order_placement: _Optional[_Union[StatefulOrderEventV1.TwapOrderPlacementV1, _Mapping]] = ...) -> None: ...

class AssetCreateEventV1(_message.Message):
    __slots__ = ("id", "symbol", "has_market", "market_id", "atomic_resolution")
    ID_FIELD_NUMBER: _ClassVar[int]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    HAS_MARKET_FIELD_NUMBER: _ClassVar[int]
    MARKET_ID_FIELD_NUMBER: _ClassVar[int]
    ATOMIC_RESOLUTION_FIELD_NUMBER: _ClassVar[int]
    id: int
    symbol: str
    has_market: bool
    market_id: int
    atomic_resolution: int
    def __init__(self, id: _Optional[int] = ..., symbol: _Optional[str] = ..., has_market: bool = ..., market_id: _Optional[int] = ..., atomic_resolution: _Optional[int] = ...) -> None: ...

class PerpetualMarketCreateEventV1(_message.Message):
    __slots__ = ("id", "clob_pair_id", "ticker", "market_id", "status", "quantum_conversion_exponent", "atomic_resolution", "subticks_per_tick", "step_base_quantums", "liquidity_tier")
    ID_FIELD_NUMBER: _ClassVar[int]
    CLOB_PAIR_ID_FIELD_NUMBER: _ClassVar[int]
    TICKER_FIELD_NUMBER: _ClassVar[int]
    MARKET_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    QUANTUM_CONVERSION_EXPONENT_FIELD_NUMBER: _ClassVar[int]
    ATOMIC_RESOLUTION_FIELD_NUMBER: _ClassVar[int]
    SUBTICKS_PER_TICK_FIELD_NUMBER: _ClassVar[int]
    STEP_BASE_QUANTUMS_FIELD_NUMBER: _ClassVar[int]
    LIQUIDITY_TIER_FIELD_NUMBER: _ClassVar[int]
    id: int
    clob_pair_id: int
    ticker: str
    market_id: int
    status: _clob_pb2.ClobPairStatus
    quantum_conversion_exponent: int
    atomic_resolution: int
    subticks_per_tick: int
    step_base_quantums: int
    liquidity_tier: int
    def __init__(self, id: _Optional[int] = ..., clob_pair_id: _Optional[int] = ..., ticker: _Optional[str] = ..., market_id: _Optional[int] = ..., status: _Optional[_Union[_clob_pb2.ClobPairStatus, str]] = ..., quantum_conversion_exponent: _Optional[int] = ..., atomic_resolution: _Optional[int] = ..., subticks_per_tick: _Optional[int] = ..., step_base_quantums: _Optional[int] = ..., liquidity_tier: _Optional[int] = ...) -> None: ...

class PerpetualMarketCreateEventV2(_message.Message):
    __slots__ = ("id", "clob_pair_id", "ticker", "market_id", "status", "quantum_conversion_exponent", "atomic_resolution", "subticks_per_tick", "step_base_quantums", "liquidity_tier", "market_type")
    ID_FIELD_NUMBER: _ClassVar[int]
    CLOB_PAIR_ID_FIELD_NUMBER: _ClassVar[int]
    TICKER_FIELD_NUMBER: _ClassVar[int]
    MARKET_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    QUANTUM_CONVERSION_EXPONENT_FIELD_NUMBER: _ClassVar[int]
    ATOMIC_RESOLUTION_FIELD_NUMBER: _ClassVar[int]
    SUBTICKS_PER_TICK_FIELD_NUMBER: _ClassVar[int]
    STEP_BASE_QUANTUMS_FIELD_NUMBER: _ClassVar[int]
    LIQUIDITY_TIER_FIELD_NUMBER: _ClassVar[int]
    MARKET_TYPE_FIELD_NUMBER: _ClassVar[int]
    id: int
    clob_pair_id: int
    ticker: str
    market_id: int
    status: _clob_pb2.ClobPairStatus
    quantum_conversion_exponent: int
    atomic_resolution: int
    subticks_per_tick: int
    step_base_quantums: int
    liquidity_tier: int
    market_type: _perpetual_pb2.PerpetualMarketType
    def __init__(self, id: _Optional[int] = ..., clob_pair_id: _Optional[int] = ..., ticker: _Optional[str] = ..., market_id: _Optional[int] = ..., status: _Optional[_Union[_clob_pb2.ClobPairStatus, str]] = ..., quantum_conversion_exponent: _Optional[int] = ..., atomic_resolution: _Optional[int] = ..., subticks_per_tick: _Optional[int] = ..., step_base_quantums: _Optional[int] = ..., liquidity_tier: _Optional[int] = ..., market_type: _Optional[_Union[_perpetual_pb2.PerpetualMarketType, str]] = ...) -> None: ...

class PerpetualMarketCreateEventV3(_message.Message):
    __slots__ = ("id", "clob_pair_id", "ticker", "market_id", "status", "quantum_conversion_exponent", "atomic_resolution", "subticks_per_tick", "step_base_quantums", "liquidity_tier", "market_type", "default_funding8hr_ppm")
    ID_FIELD_NUMBER: _ClassVar[int]
    CLOB_PAIR_ID_FIELD_NUMBER: _ClassVar[int]
    TICKER_FIELD_NUMBER: _ClassVar[int]
    MARKET_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    QUANTUM_CONVERSION_EXPONENT_FIELD_NUMBER: _ClassVar[int]
    ATOMIC_RESOLUTION_FIELD_NUMBER: _ClassVar[int]
    SUBTICKS_PER_TICK_FIELD_NUMBER: _ClassVar[int]
    STEP_BASE_QUANTUMS_FIELD_NUMBER: _ClassVar[int]
    LIQUIDITY_TIER_FIELD_NUMBER: _ClassVar[int]
    MARKET_TYPE_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_FUNDING8HR_PPM_FIELD_NUMBER: _ClassVar[int]
    id: int
    clob_pair_id: int
    ticker: str
    market_id: int
    status: _clob_pb2.ClobPairStatus
    quantum_conversion_exponent: int
    atomic_resolution: int
    subticks_per_tick: int
    step_base_quantums: int
    liquidity_tier: int
    market_type: _perpetual_pb2.PerpetualMarketType
    default_funding8hr_ppm: int
    def __init__(self, id: _Optional[int] = ..., clob_pair_id: _Optional[int] = ..., ticker: _Optional[str] = ..., market_id: _Optional[int] = ..., status: _Optional[_Union[_clob_pb2.ClobPairStatus, str]] = ..., quantum_conversion_exponent: _Optional[int] = ..., atomic_resolution: _Optional[int] = ..., subticks_per_tick: _Optional[int] = ..., step_base_quantums: _Optional[int] = ..., liquidity_tier: _Optional[int] = ..., market_type: _Optional[_Union[_perpetual_pb2.PerpetualMarketType, str]] = ..., default_funding8hr_ppm: _Optional[int] = ...) -> None: ...

class LiquidityTierUpsertEventV1(_message.Message):
    __slots__ = ("id", "name", "initial_margin_ppm", "maintenance_fraction_ppm", "base_position_notional")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    INITIAL_MARGIN_PPM_FIELD_NUMBER: _ClassVar[int]
    MAINTENANCE_FRACTION_PPM_FIELD_NUMBER: _ClassVar[int]
    BASE_POSITION_NOTIONAL_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    initial_margin_ppm: int
    maintenance_fraction_ppm: int
    base_position_notional: int
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., initial_margin_ppm: _Optional[int] = ..., maintenance_fraction_ppm: _Optional[int] = ..., base_position_notional: _Optional[int] = ...) -> None: ...

class UpdateClobPairEventV1(_message.Message):
    __slots__ = ("clob_pair_id", "status", "quantum_conversion_exponent", "subticks_per_tick", "step_base_quantums")
    CLOB_PAIR_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    QUANTUM_CONVERSION_EXPONENT_FIELD_NUMBER: _ClassVar[int]
    SUBTICKS_PER_TICK_FIELD_NUMBER: _ClassVar[int]
    STEP_BASE_QUANTUMS_FIELD_NUMBER: _ClassVar[int]
    clob_pair_id: int
    status: _clob_pb2.ClobPairStatus
    quantum_conversion_exponent: int
    subticks_per_tick: int
    step_base_quantums: int
    def __init__(self, clob_pair_id: _Optional[int] = ..., status: _Optional[_Union[_clob_pb2.ClobPairStatus, str]] = ..., quantum_conversion_exponent: _Optional[int] = ..., subticks_per_tick: _Optional[int] = ..., step_base_quantums: _Optional[int] = ...) -> None: ...

class UpdatePerpetualEventV1(_message.Message):
    __slots__ = ("id", "ticker", "market_id", "atomic_resolution", "liquidity_tier")
    ID_FIELD_NUMBER: _ClassVar[int]
    TICKER_FIELD_NUMBER: _ClassVar[int]
    MARKET_ID_FIELD_NUMBER: _ClassVar[int]
    ATOMIC_RESOLUTION_FIELD_NUMBER: _ClassVar[int]
    LIQUIDITY_TIER_FIELD_NUMBER: _ClassVar[int]
    id: int
    ticker: str
    market_id: int
    atomic_resolution: int
    liquidity_tier: int
    def __init__(self, id: _Optional[int] = ..., ticker: _Optional[str] = ..., market_id: _Optional[int] = ..., atomic_resolution: _Optional[int] = ..., liquidity_tier: _Optional[int] = ...) -> None: ...

class UpdatePerpetualEventV2(_message.Message):
    __slots__ = ("id", "ticker", "market_id", "atomic_resolution", "liquidity_tier", "market_type")
    ID_FIELD_NUMBER: _ClassVar[int]
    TICKER_FIELD_NUMBER: _ClassVar[int]
    MARKET_ID_FIELD_NUMBER: _ClassVar[int]
    ATOMIC_RESOLUTION_FIELD_NUMBER: _ClassVar[int]
    LIQUIDITY_TIER_FIELD_NUMBER: _ClassVar[int]
    MARKET_TYPE_FIELD_NUMBER: _ClassVar[int]
    id: int
    ticker: str
    market_id: int
    atomic_resolution: int
    liquidity_tier: int
    market_type: _perpetual_pb2.PerpetualMarketType
    def __init__(self, id: _Optional[int] = ..., ticker: _Optional[str] = ..., market_id: _Optional[int] = ..., atomic_resolution: _Optional[int] = ..., liquidity_tier: _Optional[int] = ..., market_type: _Optional[_Union[_perpetual_pb2.PerpetualMarketType, str]] = ...) -> None: ...

class UpdatePerpetualEventV3(_message.Message):
    __slots__ = ("id", "ticker", "market_id", "atomic_resolution", "liquidity_tier", "market_type", "default_funding8hr_ppm")
    ID_FIELD_NUMBER: _ClassVar[int]
    TICKER_FIELD_NUMBER: _ClassVar[int]
    MARKET_ID_FIELD_NUMBER: _ClassVar[int]
    ATOMIC_RESOLUTION_FIELD_NUMBER: _ClassVar[int]
    LIQUIDITY_TIER_FIELD_NUMBER: _ClassVar[int]
    MARKET_TYPE_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_FUNDING8HR_PPM_FIELD_NUMBER: _ClassVar[int]
    id: int
    ticker: str
    market_id: int
    atomic_resolution: int
    liquidity_tier: int
    market_type: _perpetual_pb2.PerpetualMarketType
    default_funding8hr_ppm: int
    def __init__(self, id: _Optional[int] = ..., ticker: _Optional[str] = ..., market_id: _Optional[int] = ..., atomic_resolution: _Optional[int] = ..., liquidity_tier: _Optional[int] = ..., market_type: _Optional[_Union[_perpetual_pb2.PerpetualMarketType, str]] = ..., default_funding8hr_ppm: _Optional[int] = ...) -> None: ...

class TradingRewardsEventV1(_message.Message):
    __slots__ = ("trading_rewards",)
    TRADING_REWARDS_FIELD_NUMBER: _ClassVar[int]
    trading_rewards: _containers.RepeatedCompositeFieldContainer[AddressTradingReward]
    def __init__(self, trading_rewards: _Optional[_Iterable[_Union[AddressTradingReward, _Mapping]]] = ...) -> None: ...

class AddressTradingReward(_message.Message):
    __slots__ = ("owner", "denom_amount")
    OWNER_FIELD_NUMBER: _ClassVar[int]
    DENOM_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    owner: str
    denom_amount: bytes
    def __init__(self, owner: _Optional[str] = ..., denom_amount: _Optional[bytes] = ...) -> None: ...

class OpenInterestUpdateEventV1(_message.Message):
    __slots__ = ("open_interest_updates",)
    OPEN_INTEREST_UPDATES_FIELD_NUMBER: _ClassVar[int]
    open_interest_updates: _containers.RepeatedCompositeFieldContainer[OpenInterestUpdate]
    def __init__(self, open_interest_updates: _Optional[_Iterable[_Union[OpenInterestUpdate, _Mapping]]] = ...) -> None: ...

class OpenInterestUpdate(_message.Message):
    __slots__ = ("perpetual_id", "open_interest")
    PERPETUAL_ID_FIELD_NUMBER: _ClassVar[int]
    OPEN_INTEREST_FIELD_NUMBER: _ClassVar[int]
    perpetual_id: int
    open_interest: bytes
    def __init__(self, perpetual_id: _Optional[int] = ..., open_interest: _Optional[bytes] = ...) -> None: ...

class LiquidityTierUpsertEventV2(_message.Message):
    __slots__ = ("id", "name", "initial_margin_ppm", "maintenance_fraction_ppm", "base_position_notional", "open_interest_lower_cap", "open_interest_upper_cap")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    INITIAL_MARGIN_PPM_FIELD_NUMBER: _ClassVar[int]
    MAINTENANCE_FRACTION_PPM_FIELD_NUMBER: _ClassVar[int]
    BASE_POSITION_NOTIONAL_FIELD_NUMBER: _ClassVar[int]
    OPEN_INTEREST_LOWER_CAP_FIELD_NUMBER: _ClassVar[int]
    OPEN_INTEREST_UPPER_CAP_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    initial_margin_ppm: int
    maintenance_fraction_ppm: int
    base_position_notional: int
    open_interest_lower_cap: int
    open_interest_upper_cap: int
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., initial_margin_ppm: _Optional[int] = ..., maintenance_fraction_ppm: _Optional[int] = ..., base_position_notional: _Optional[int] = ..., open_interest_lower_cap: _Optional[int] = ..., open_interest_upper_cap: _Optional[int] = ...) -> None: ...

class RegisterAffiliateEventV1(_message.Message):
    __slots__ = ("referee", "affiliate")
    REFEREE_FIELD_NUMBER: _ClassVar[int]
    AFFILIATE_FIELD_NUMBER: _ClassVar[int]
    referee: str
    affiliate: str
    def __init__(self, referee: _Optional[str] = ..., affiliate: _Optional[str] = ...) -> None: ...

class UpsertVaultEventV1(_message.Message):
    __slots__ = ("address", "clob_pair_id", "status")
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    CLOB_PAIR_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    address: str
    clob_pair_id: int
    status: _vault_pb2.VaultStatus
    def __init__(self, address: _Optional[str] = ..., clob_pair_id: _Optional[int] = ..., status: _Optional[_Union[_vault_pb2.VaultStatus, str]] = ...) -> None: ...
