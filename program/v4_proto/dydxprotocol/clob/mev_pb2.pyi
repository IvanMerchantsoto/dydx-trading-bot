from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from v4_proto.dydxprotocol.subaccounts import subaccount_pb2 as _subaccount_pb2
from v4_proto.dydxprotocol.clob import clob_pair_pb2 as _clob_pair_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MEVMatch(_message.Message):
    __slots__ = ("taker_order_subaccount_id", "taker_fee_ppm", "maker_order_subaccount_id", "maker_order_subticks", "maker_order_is_buy", "maker_fee_ppm", "clob_pair_id", "fill_amount")
    TAKER_ORDER_SUBACCOUNT_ID_FIELD_NUMBER: _ClassVar[int]
    TAKER_FEE_PPM_FIELD_NUMBER: _ClassVar[int]
    MAKER_ORDER_SUBACCOUNT_ID_FIELD_NUMBER: _ClassVar[int]
    MAKER_ORDER_SUBTICKS_FIELD_NUMBER: _ClassVar[int]
    MAKER_ORDER_IS_BUY_FIELD_NUMBER: _ClassVar[int]
    MAKER_FEE_PPM_FIELD_NUMBER: _ClassVar[int]
    CLOB_PAIR_ID_FIELD_NUMBER: _ClassVar[int]
    FILL_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    taker_order_subaccount_id: _subaccount_pb2.SubaccountId
    taker_fee_ppm: int
    maker_order_subaccount_id: _subaccount_pb2.SubaccountId
    maker_order_subticks: int
    maker_order_is_buy: bool
    maker_fee_ppm: int
    clob_pair_id: int
    fill_amount: int
    def __init__(self, taker_order_subaccount_id: _Optional[_Union[_subaccount_pb2.SubaccountId, _Mapping]] = ..., taker_fee_ppm: _Optional[int] = ..., maker_order_subaccount_id: _Optional[_Union[_subaccount_pb2.SubaccountId, _Mapping]] = ..., maker_order_subticks: _Optional[int] = ..., maker_order_is_buy: bool = ..., maker_fee_ppm: _Optional[int] = ..., clob_pair_id: _Optional[int] = ..., fill_amount: _Optional[int] = ...) -> None: ...

class MEVLiquidationMatch(_message.Message):
    __slots__ = ("liquidated_subaccount_id", "insurance_fund_delta_quote_quantums", "maker_order_subaccount_id", "maker_order_subticks", "maker_order_is_buy", "maker_fee_ppm", "clob_pair_id", "fill_amount")
    LIQUIDATED_SUBACCOUNT_ID_FIELD_NUMBER: _ClassVar[int]
    INSURANCE_FUND_DELTA_QUOTE_QUANTUMS_FIELD_NUMBER: _ClassVar[int]
    MAKER_ORDER_SUBACCOUNT_ID_FIELD_NUMBER: _ClassVar[int]
    MAKER_ORDER_SUBTICKS_FIELD_NUMBER: _ClassVar[int]
    MAKER_ORDER_IS_BUY_FIELD_NUMBER: _ClassVar[int]
    MAKER_FEE_PPM_FIELD_NUMBER: _ClassVar[int]
    CLOB_PAIR_ID_FIELD_NUMBER: _ClassVar[int]
    FILL_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    liquidated_subaccount_id: _subaccount_pb2.SubaccountId
    insurance_fund_delta_quote_quantums: int
    maker_order_subaccount_id: _subaccount_pb2.SubaccountId
    maker_order_subticks: int
    maker_order_is_buy: bool
    maker_fee_ppm: int
    clob_pair_id: int
    fill_amount: int
    def __init__(self, liquidated_subaccount_id: _Optional[_Union[_subaccount_pb2.SubaccountId, _Mapping]] = ..., insurance_fund_delta_quote_quantums: _Optional[int] = ..., maker_order_subaccount_id: _Optional[_Union[_subaccount_pb2.SubaccountId, _Mapping]] = ..., maker_order_subticks: _Optional[int] = ..., maker_order_is_buy: bool = ..., maker_fee_ppm: _Optional[int] = ..., clob_pair_id: _Optional[int] = ..., fill_amount: _Optional[int] = ...) -> None: ...

class ClobMidPrice(_message.Message):
    __slots__ = ("clob_pair", "subticks")
    CLOB_PAIR_FIELD_NUMBER: _ClassVar[int]
    SUBTICKS_FIELD_NUMBER: _ClassVar[int]
    clob_pair: _clob_pair_pb2.ClobPair
    subticks: int
    def __init__(self, clob_pair: _Optional[_Union[_clob_pair_pb2.ClobPair, _Mapping]] = ..., subticks: _Optional[int] = ...) -> None: ...

class ValidatorMevMatches(_message.Message):
    __slots__ = ("matches", "liquidation_matches")
    MATCHES_FIELD_NUMBER: _ClassVar[int]
    LIQUIDATION_MATCHES_FIELD_NUMBER: _ClassVar[int]
    matches: _containers.RepeatedCompositeFieldContainer[MEVMatch]
    liquidation_matches: _containers.RepeatedCompositeFieldContainer[MEVLiquidationMatch]
    def __init__(self, matches: _Optional[_Iterable[_Union[MEVMatch, _Mapping]]] = ..., liquidation_matches: _Optional[_Iterable[_Union[MEVLiquidationMatch, _Mapping]]] = ...) -> None: ...

class MevNodeToNodeMetrics(_message.Message):
    __slots__ = ("validator_mev_matches", "clob_mid_prices", "bp_mev_matches", "proposal_receive_time")
    VALIDATOR_MEV_MATCHES_FIELD_NUMBER: _ClassVar[int]
    CLOB_MID_PRICES_FIELD_NUMBER: _ClassVar[int]
    BP_MEV_MATCHES_FIELD_NUMBER: _ClassVar[int]
    PROPOSAL_RECEIVE_TIME_FIELD_NUMBER: _ClassVar[int]
    validator_mev_matches: ValidatorMevMatches
    clob_mid_prices: _containers.RepeatedCompositeFieldContainer[ClobMidPrice]
    bp_mev_matches: ValidatorMevMatches
    proposal_receive_time: int
    def __init__(self, validator_mev_matches: _Optional[_Union[ValidatorMevMatches, _Mapping]] = ..., clob_mid_prices: _Optional[_Iterable[_Union[ClobMidPrice, _Mapping]]] = ..., bp_mev_matches: _Optional[_Union[ValidatorMevMatches, _Mapping]] = ..., proposal_receive_time: _Optional[int] = ...) -> None: ...
