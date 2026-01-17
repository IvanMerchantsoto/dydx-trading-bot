import datetime

from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EventParams(_message.Message):
    __slots__ = ("denom", "eth_chain_id", "eth_address")
    DENOM_FIELD_NUMBER: _ClassVar[int]
    ETH_CHAIN_ID_FIELD_NUMBER: _ClassVar[int]
    ETH_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    denom: str
    eth_chain_id: int
    eth_address: str
    def __init__(self, denom: _Optional[str] = ..., eth_chain_id: _Optional[int] = ..., eth_address: _Optional[str] = ...) -> None: ...

class ProposeParams(_message.Message):
    __slots__ = ("max_bridges_per_block", "propose_delay_duration", "skip_rate_ppm", "skip_if_block_delayed_by_duration")
    MAX_BRIDGES_PER_BLOCK_FIELD_NUMBER: _ClassVar[int]
    PROPOSE_DELAY_DURATION_FIELD_NUMBER: _ClassVar[int]
    SKIP_RATE_PPM_FIELD_NUMBER: _ClassVar[int]
    SKIP_IF_BLOCK_DELAYED_BY_DURATION_FIELD_NUMBER: _ClassVar[int]
    max_bridges_per_block: int
    propose_delay_duration: _duration_pb2.Duration
    skip_rate_ppm: int
    skip_if_block_delayed_by_duration: _duration_pb2.Duration
    def __init__(self, max_bridges_per_block: _Optional[int] = ..., propose_delay_duration: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., skip_rate_ppm: _Optional[int] = ..., skip_if_block_delayed_by_duration: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class SafetyParams(_message.Message):
    __slots__ = ("is_disabled", "delay_blocks")
    IS_DISABLED_FIELD_NUMBER: _ClassVar[int]
    DELAY_BLOCKS_FIELD_NUMBER: _ClassVar[int]
    is_disabled: bool
    delay_blocks: int
    def __init__(self, is_disabled: bool = ..., delay_blocks: _Optional[int] = ...) -> None: ...
