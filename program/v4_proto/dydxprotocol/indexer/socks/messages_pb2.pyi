from v4_proto.dydxprotocol.indexer.protocol.v1 import subaccount_pb2 as _subaccount_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class OrderbookMessage(_message.Message):
    __slots__ = ("contents", "clob_pair_id", "version")
    CONTENTS_FIELD_NUMBER: _ClassVar[int]
    CLOB_PAIR_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    contents: str
    clob_pair_id: str
    version: str
    def __init__(self, contents: _Optional[str] = ..., clob_pair_id: _Optional[str] = ..., version: _Optional[str] = ...) -> None: ...

class SubaccountMessage(_message.Message):
    __slots__ = ("block_height", "transaction_index", "event_index", "contents", "subaccount_id", "version")
    BLOCK_HEIGHT_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_INDEX_FIELD_NUMBER: _ClassVar[int]
    EVENT_INDEX_FIELD_NUMBER: _ClassVar[int]
    CONTENTS_FIELD_NUMBER: _ClassVar[int]
    SUBACCOUNT_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    block_height: str
    transaction_index: int
    event_index: int
    contents: str
    subaccount_id: _subaccount_pb2.IndexerSubaccountId
    version: str
    def __init__(self, block_height: _Optional[str] = ..., transaction_index: _Optional[int] = ..., event_index: _Optional[int] = ..., contents: _Optional[str] = ..., subaccount_id: _Optional[_Union[_subaccount_pb2.IndexerSubaccountId, _Mapping]] = ..., version: _Optional[str] = ...) -> None: ...

class TradeMessage(_message.Message):
    __slots__ = ("block_height", "contents", "clob_pair_id", "version")
    BLOCK_HEIGHT_FIELD_NUMBER: _ClassVar[int]
    CONTENTS_FIELD_NUMBER: _ClassVar[int]
    CLOB_PAIR_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    block_height: str
    contents: str
    clob_pair_id: str
    version: str
    def __init__(self, block_height: _Optional[str] = ..., contents: _Optional[str] = ..., clob_pair_id: _Optional[str] = ..., version: _Optional[str] = ...) -> None: ...

class MarketMessage(_message.Message):
    __slots__ = ("contents", "version")
    CONTENTS_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    contents: str
    version: str
    def __init__(self, contents: _Optional[str] = ..., version: _Optional[str] = ...) -> None: ...

class CandleMessage(_message.Message):
    __slots__ = ("contents", "clob_pair_id", "resolution", "version")
    class Resolution(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        ONE_MINUTE: _ClassVar[CandleMessage.Resolution]
        FIVE_MINUTES: _ClassVar[CandleMessage.Resolution]
        FIFTEEN_MINUTES: _ClassVar[CandleMessage.Resolution]
        THIRTY_MINUTES: _ClassVar[CandleMessage.Resolution]
        ONE_HOUR: _ClassVar[CandleMessage.Resolution]
        FOUR_HOURS: _ClassVar[CandleMessage.Resolution]
        ONE_DAY: _ClassVar[CandleMessage.Resolution]
    ONE_MINUTE: CandleMessage.Resolution
    FIVE_MINUTES: CandleMessage.Resolution
    FIFTEEN_MINUTES: CandleMessage.Resolution
    THIRTY_MINUTES: CandleMessage.Resolution
    ONE_HOUR: CandleMessage.Resolution
    FOUR_HOURS: CandleMessage.Resolution
    ONE_DAY: CandleMessage.Resolution
    CONTENTS_FIELD_NUMBER: _ClassVar[int]
    CLOB_PAIR_ID_FIELD_NUMBER: _ClassVar[int]
    RESOLUTION_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    contents: str
    clob_pair_id: str
    resolution: CandleMessage.Resolution
    version: str
    def __init__(self, contents: _Optional[str] = ..., clob_pair_id: _Optional[str] = ..., resolution: _Optional[_Union[CandleMessage.Resolution, str]] = ..., version: _Optional[str] = ...) -> None: ...

class BlockHeightMessage(_message.Message):
    __slots__ = ("block_height", "time", "version")
    BLOCK_HEIGHT_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    block_height: str
    time: str
    version: str
    def __init__(self, block_height: _Optional[str] = ..., time: _Optional[str] = ..., version: _Optional[str] = ...) -> None: ...
