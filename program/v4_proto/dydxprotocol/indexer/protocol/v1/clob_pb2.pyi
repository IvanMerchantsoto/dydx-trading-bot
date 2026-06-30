from v4_proto.dydxprotocol.indexer.protocol.v1 import subaccount_pb2 as _subaccount_pb2
from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ClobPairStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CLOB_PAIR_STATUS_UNSPECIFIED: _ClassVar[ClobPairStatus]
    CLOB_PAIR_STATUS_ACTIVE: _ClassVar[ClobPairStatus]
    CLOB_PAIR_STATUS_PAUSED: _ClassVar[ClobPairStatus]
    CLOB_PAIR_STATUS_CANCEL_ONLY: _ClassVar[ClobPairStatus]
    CLOB_PAIR_STATUS_POST_ONLY: _ClassVar[ClobPairStatus]
    CLOB_PAIR_STATUS_INITIALIZING: _ClassVar[ClobPairStatus]
    CLOB_PAIR_STATUS_FINAL_SETTLEMENT: _ClassVar[ClobPairStatus]
CLOB_PAIR_STATUS_UNSPECIFIED: ClobPairStatus
CLOB_PAIR_STATUS_ACTIVE: ClobPairStatus
CLOB_PAIR_STATUS_PAUSED: ClobPairStatus
CLOB_PAIR_STATUS_CANCEL_ONLY: ClobPairStatus
CLOB_PAIR_STATUS_POST_ONLY: ClobPairStatus
CLOB_PAIR_STATUS_INITIALIZING: ClobPairStatus
CLOB_PAIR_STATUS_FINAL_SETTLEMENT: ClobPairStatus

class IndexerOrderId(_message.Message):
    __slots__ = ("subaccount_id", "client_id", "order_flags", "clob_pair_id")
    SUBACCOUNT_ID_FIELD_NUMBER: _ClassVar[int]
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    ORDER_FLAGS_FIELD_NUMBER: _ClassVar[int]
    CLOB_PAIR_ID_FIELD_NUMBER: _ClassVar[int]
    subaccount_id: _subaccount_pb2.IndexerSubaccountId
    client_id: int
    order_flags: int
    clob_pair_id: int
    def __init__(self, subaccount_id: _Optional[_Union[_subaccount_pb2.IndexerSubaccountId, _Mapping]] = ..., client_id: _Optional[int] = ..., order_flags: _Optional[int] = ..., clob_pair_id: _Optional[int] = ...) -> None: ...

class IndexerOrder(_message.Message):
    __slots__ = ("order_id", "side", "quantums", "subticks", "good_til_block", "good_til_block_time", "time_in_force", "reduce_only", "client_metadata", "condition_type", "conditional_order_trigger_subticks", "builder_code_params", "order_router_address", "twap_parameters")
    class Side(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        SIDE_UNSPECIFIED: _ClassVar[IndexerOrder.Side]
        SIDE_BUY: _ClassVar[IndexerOrder.Side]
        SIDE_SELL: _ClassVar[IndexerOrder.Side]
    SIDE_UNSPECIFIED: IndexerOrder.Side
    SIDE_BUY: IndexerOrder.Side
    SIDE_SELL: IndexerOrder.Side
    class TimeInForce(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        TIME_IN_FORCE_UNSPECIFIED: _ClassVar[IndexerOrder.TimeInForce]
        TIME_IN_FORCE_IOC: _ClassVar[IndexerOrder.TimeInForce]
        TIME_IN_FORCE_POST_ONLY: _ClassVar[IndexerOrder.TimeInForce]
        TIME_IN_FORCE_FILL_OR_KILL: _ClassVar[IndexerOrder.TimeInForce]
    TIME_IN_FORCE_UNSPECIFIED: IndexerOrder.TimeInForce
    TIME_IN_FORCE_IOC: IndexerOrder.TimeInForce
    TIME_IN_FORCE_POST_ONLY: IndexerOrder.TimeInForce
    TIME_IN_FORCE_FILL_OR_KILL: IndexerOrder.TimeInForce
    class ConditionType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        CONDITION_TYPE_UNSPECIFIED: _ClassVar[IndexerOrder.ConditionType]
        CONDITION_TYPE_STOP_LOSS: _ClassVar[IndexerOrder.ConditionType]
        CONDITION_TYPE_TAKE_PROFIT: _ClassVar[IndexerOrder.ConditionType]
    CONDITION_TYPE_UNSPECIFIED: IndexerOrder.ConditionType
    CONDITION_TYPE_STOP_LOSS: IndexerOrder.ConditionType
    CONDITION_TYPE_TAKE_PROFIT: IndexerOrder.ConditionType
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    SIDE_FIELD_NUMBER: _ClassVar[int]
    QUANTUMS_FIELD_NUMBER: _ClassVar[int]
    SUBTICKS_FIELD_NUMBER: _ClassVar[int]
    GOOD_TIL_BLOCK_FIELD_NUMBER: _ClassVar[int]
    GOOD_TIL_BLOCK_TIME_FIELD_NUMBER: _ClassVar[int]
    TIME_IN_FORCE_FIELD_NUMBER: _ClassVar[int]
    REDUCE_ONLY_FIELD_NUMBER: _ClassVar[int]
    CLIENT_METADATA_FIELD_NUMBER: _ClassVar[int]
    CONDITION_TYPE_FIELD_NUMBER: _ClassVar[int]
    CONDITIONAL_ORDER_TRIGGER_SUBTICKS_FIELD_NUMBER: _ClassVar[int]
    BUILDER_CODE_PARAMS_FIELD_NUMBER: _ClassVar[int]
    ORDER_ROUTER_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    TWAP_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    order_id: IndexerOrderId
    side: IndexerOrder.Side
    quantums: int
    subticks: int
    good_til_block: int
    good_til_block_time: int
    time_in_force: IndexerOrder.TimeInForce
    reduce_only: bool
    client_metadata: int
    condition_type: IndexerOrder.ConditionType
    conditional_order_trigger_subticks: int
    builder_code_params: BuilderCodeParameters
    order_router_address: str
    twap_parameters: TwapParameters
    def __init__(self, order_id: _Optional[_Union[IndexerOrderId, _Mapping]] = ..., side: _Optional[_Union[IndexerOrder.Side, str]] = ..., quantums: _Optional[int] = ..., subticks: _Optional[int] = ..., good_til_block: _Optional[int] = ..., good_til_block_time: _Optional[int] = ..., time_in_force: _Optional[_Union[IndexerOrder.TimeInForce, str]] = ..., reduce_only: bool = ..., client_metadata: _Optional[int] = ..., condition_type: _Optional[_Union[IndexerOrder.ConditionType, str]] = ..., conditional_order_trigger_subticks: _Optional[int] = ..., builder_code_params: _Optional[_Union[BuilderCodeParameters, _Mapping]] = ..., order_router_address: _Optional[str] = ..., twap_parameters: _Optional[_Union[TwapParameters, _Mapping]] = ...) -> None: ...

class TwapParameters(_message.Message):
    __slots__ = ("duration", "interval", "price_tolerance")
    DURATION_FIELD_NUMBER: _ClassVar[int]
    INTERVAL_FIELD_NUMBER: _ClassVar[int]
    PRICE_TOLERANCE_FIELD_NUMBER: _ClassVar[int]
    duration: int
    interval: int
    price_tolerance: int
    def __init__(self, duration: _Optional[int] = ..., interval: _Optional[int] = ..., price_tolerance: _Optional[int] = ...) -> None: ...

class BuilderCodeParameters(_message.Message):
    __slots__ = ("builder_address", "fee_ppm")
    BUILDER_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    FEE_PPM_FIELD_NUMBER: _ClassVar[int]
    builder_address: str
    fee_ppm: int
    def __init__(self, builder_address: _Optional[str] = ..., fee_ppm: _Optional[int] = ...) -> None: ...
