import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PerMarketFeeDiscountParams(_message.Message):
    __slots__ = ("clob_pair_id", "start_time", "end_time", "charge_ppm")
    CLOB_PAIR_ID_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    END_TIME_FIELD_NUMBER: _ClassVar[int]
    CHARGE_PPM_FIELD_NUMBER: _ClassVar[int]
    clob_pair_id: int
    start_time: _timestamp_pb2.Timestamp
    end_time: _timestamp_pb2.Timestamp
    charge_ppm: int
    def __init__(self, clob_pair_id: _Optional[int] = ..., start_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., end_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., charge_ppm: _Optional[int] = ...) -> None: ...
