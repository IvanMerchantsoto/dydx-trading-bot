from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from v4_proto.dydxprotocol.clob import order_pb2 as _order_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class OrderRemoval(_message.Message):
    __slots__ = ("order_id", "removal_reason")
    class RemovalReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        REMOVAL_REASON_UNSPECIFIED: _ClassVar[OrderRemoval.RemovalReason]
        REMOVAL_REASON_UNDERCOLLATERALIZED: _ClassVar[OrderRemoval.RemovalReason]
        REMOVAL_REASON_INVALID_REDUCE_ONLY: _ClassVar[OrderRemoval.RemovalReason]
        REMOVAL_REASON_POST_ONLY_WOULD_CROSS_MAKER_ORDER: _ClassVar[OrderRemoval.RemovalReason]
        REMOVAL_REASON_INVALID_SELF_TRADE: _ClassVar[OrderRemoval.RemovalReason]
        REMOVAL_REASON_CONDITIONAL_FOK_COULD_NOT_BE_FULLY_FILLED: _ClassVar[OrderRemoval.RemovalReason]
        REMOVAL_REASON_CONDITIONAL_IOC_WOULD_REST_ON_BOOK: _ClassVar[OrderRemoval.RemovalReason]
        REMOVAL_REASON_FULLY_FILLED: _ClassVar[OrderRemoval.RemovalReason]
        REMOVAL_REASON_VIOLATES_ISOLATED_SUBACCOUNT_CONSTRAINTS: _ClassVar[OrderRemoval.RemovalReason]
        REMOVAL_REASON_PERMISSIONED_KEY_EXPIRED: _ClassVar[OrderRemoval.RemovalReason]
    REMOVAL_REASON_UNSPECIFIED: OrderRemoval.RemovalReason
    REMOVAL_REASON_UNDERCOLLATERALIZED: OrderRemoval.RemovalReason
    REMOVAL_REASON_INVALID_REDUCE_ONLY: OrderRemoval.RemovalReason
    REMOVAL_REASON_POST_ONLY_WOULD_CROSS_MAKER_ORDER: OrderRemoval.RemovalReason
    REMOVAL_REASON_INVALID_SELF_TRADE: OrderRemoval.RemovalReason
    REMOVAL_REASON_CONDITIONAL_FOK_COULD_NOT_BE_FULLY_FILLED: OrderRemoval.RemovalReason
    REMOVAL_REASON_CONDITIONAL_IOC_WOULD_REST_ON_BOOK: OrderRemoval.RemovalReason
    REMOVAL_REASON_FULLY_FILLED: OrderRemoval.RemovalReason
    REMOVAL_REASON_VIOLATES_ISOLATED_SUBACCOUNT_CONSTRAINTS: OrderRemoval.RemovalReason
    REMOVAL_REASON_PERMISSIONED_KEY_EXPIRED: OrderRemoval.RemovalReason
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    REMOVAL_REASON_FIELD_NUMBER: _ClassVar[int]
    order_id: _order_pb2.OrderId
    removal_reason: OrderRemoval.RemovalReason
    def __init__(self, order_id: _Optional[_Union[_order_pb2.OrderId, _Mapping]] = ..., removal_reason: _Optional[_Union[OrderRemoval.RemovalReason, str]] = ...) -> None: ...
