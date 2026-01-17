from v4_proto.dydxprotocol.clob import matches_pb2 as _matches_pb2
from v4_proto.dydxprotocol.clob import order_pb2 as _order_pb2
from v4_proto.dydxprotocol.clob import order_removals_pb2 as _order_removals_pb2
from v4_proto.dydxprotocol.clob import tx_pb2 as _tx_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Operation(_message.Message):
    __slots__ = ("match", "short_term_order_placement", "short_term_order_cancellation", "preexisting_stateful_order")
    MATCH_FIELD_NUMBER: _ClassVar[int]
    SHORT_TERM_ORDER_PLACEMENT_FIELD_NUMBER: _ClassVar[int]
    SHORT_TERM_ORDER_CANCELLATION_FIELD_NUMBER: _ClassVar[int]
    PREEXISTING_STATEFUL_ORDER_FIELD_NUMBER: _ClassVar[int]
    match: _matches_pb2.ClobMatch
    short_term_order_placement: _tx_pb2.MsgPlaceOrder
    short_term_order_cancellation: _tx_pb2.MsgCancelOrder
    preexisting_stateful_order: _order_pb2.OrderId
    def __init__(self, match: _Optional[_Union[_matches_pb2.ClobMatch, _Mapping]] = ..., short_term_order_placement: _Optional[_Union[_tx_pb2.MsgPlaceOrder, _Mapping]] = ..., short_term_order_cancellation: _Optional[_Union[_tx_pb2.MsgCancelOrder, _Mapping]] = ..., preexisting_stateful_order: _Optional[_Union[_order_pb2.OrderId, _Mapping]] = ...) -> None: ...

class InternalOperation(_message.Message):
    __slots__ = ("match", "short_term_order_placement", "preexisting_stateful_order", "order_removal")
    MATCH_FIELD_NUMBER: _ClassVar[int]
    SHORT_TERM_ORDER_PLACEMENT_FIELD_NUMBER: _ClassVar[int]
    PREEXISTING_STATEFUL_ORDER_FIELD_NUMBER: _ClassVar[int]
    ORDER_REMOVAL_FIELD_NUMBER: _ClassVar[int]
    match: _matches_pb2.ClobMatch
    short_term_order_placement: _tx_pb2.MsgPlaceOrder
    preexisting_stateful_order: _order_pb2.OrderId
    order_removal: _order_removals_pb2.OrderRemoval
    def __init__(self, match: _Optional[_Union[_matches_pb2.ClobMatch, _Mapping]] = ..., short_term_order_placement: _Optional[_Union[_tx_pb2.MsgPlaceOrder, _Mapping]] = ..., preexisting_stateful_order: _Optional[_Union[_order_pb2.OrderId, _Mapping]] = ..., order_removal: _Optional[_Union[_order_removals_pb2.OrderRemoval, _Mapping]] = ...) -> None: ...
