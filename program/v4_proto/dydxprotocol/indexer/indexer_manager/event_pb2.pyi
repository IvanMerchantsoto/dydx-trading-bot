import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class IndexerTendermintEventWrapper(_message.Message):
    __slots__ = ("event", "txn_hash")
    EVENT_FIELD_NUMBER: _ClassVar[int]
    TXN_HASH_FIELD_NUMBER: _ClassVar[int]
    event: IndexerTendermintEvent
    txn_hash: str
    def __init__(self, event: _Optional[_Union[IndexerTendermintEvent, _Mapping]] = ..., txn_hash: _Optional[str] = ...) -> None: ...

class IndexerEventsStoreValue(_message.Message):
    __slots__ = ("events",)
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    events: _containers.RepeatedCompositeFieldContainer[IndexerTendermintEventWrapper]
    def __init__(self, events: _Optional[_Iterable[_Union[IndexerTendermintEventWrapper, _Mapping]]] = ...) -> None: ...

class IndexerTendermintEvent(_message.Message):
    __slots__ = ("subtype", "transaction_index", "block_event", "event_index", "version", "data_bytes")
    class BlockEvent(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        BLOCK_EVENT_UNSPECIFIED: _ClassVar[IndexerTendermintEvent.BlockEvent]
        BLOCK_EVENT_BEGIN_BLOCK: _ClassVar[IndexerTendermintEvent.BlockEvent]
        BLOCK_EVENT_END_BLOCK: _ClassVar[IndexerTendermintEvent.BlockEvent]
    BLOCK_EVENT_UNSPECIFIED: IndexerTendermintEvent.BlockEvent
    BLOCK_EVENT_BEGIN_BLOCK: IndexerTendermintEvent.BlockEvent
    BLOCK_EVENT_END_BLOCK: IndexerTendermintEvent.BlockEvent
    SUBTYPE_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_INDEX_FIELD_NUMBER: _ClassVar[int]
    BLOCK_EVENT_FIELD_NUMBER: _ClassVar[int]
    EVENT_INDEX_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    DATA_BYTES_FIELD_NUMBER: _ClassVar[int]
    subtype: str
    transaction_index: int
    block_event: IndexerTendermintEvent.BlockEvent
    event_index: int
    version: int
    data_bytes: bytes
    def __init__(self, subtype: _Optional[str] = ..., transaction_index: _Optional[int] = ..., block_event: _Optional[_Union[IndexerTendermintEvent.BlockEvent, str]] = ..., event_index: _Optional[int] = ..., version: _Optional[int] = ..., data_bytes: _Optional[bytes] = ...) -> None: ...

class IndexerTendermintBlock(_message.Message):
    __slots__ = ("height", "time", "events", "tx_hashes")
    HEIGHT_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_NUMBER: _ClassVar[int]
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    TX_HASHES_FIELD_NUMBER: _ClassVar[int]
    height: int
    time: _timestamp_pb2.Timestamp
    events: _containers.RepeatedCompositeFieldContainer[IndexerTendermintEvent]
    tx_hashes: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, height: _Optional[int] = ..., time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., events: _Optional[_Iterable[_Union[IndexerTendermintEvent, _Mapping]]] = ..., tx_hashes: _Optional[_Iterable[str]] = ...) -> None: ...
