from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EquityTierLimitConfiguration(_message.Message):
    __slots__ = ("short_term_order_equity_tiers", "stateful_order_equity_tiers")
    SHORT_TERM_ORDER_EQUITY_TIERS_FIELD_NUMBER: _ClassVar[int]
    STATEFUL_ORDER_EQUITY_TIERS_FIELD_NUMBER: _ClassVar[int]
    short_term_order_equity_tiers: _containers.RepeatedCompositeFieldContainer[EquityTierLimit]
    stateful_order_equity_tiers: _containers.RepeatedCompositeFieldContainer[EquityTierLimit]
    def __init__(self, short_term_order_equity_tiers: _Optional[_Iterable[_Union[EquityTierLimit, _Mapping]]] = ..., stateful_order_equity_tiers: _Optional[_Iterable[_Union[EquityTierLimit, _Mapping]]] = ...) -> None: ...

class EquityTierLimit(_message.Message):
    __slots__ = ("usd_tnc_required", "limit")
    USD_TNC_REQUIRED_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    usd_tnc_required: bytes
    limit: int
    def __init__(self, usd_tnc_required: _Optional[bytes] = ..., limit: _Optional[int] = ...) -> None: ...
