from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from v4_proto.dydxprotocol.bridge import bridge_event_info_pb2 as _bridge_event_info_pb2
from v4_proto.dydxprotocol.bridge import params_pb2 as _params_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GenesisState(_message.Message):
    __slots__ = ("event_params", "propose_params", "safety_params", "acknowledged_event_info")
    EVENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    PROPOSE_PARAMS_FIELD_NUMBER: _ClassVar[int]
    SAFETY_PARAMS_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGED_EVENT_INFO_FIELD_NUMBER: _ClassVar[int]
    event_params: _params_pb2.EventParams
    propose_params: _params_pb2.ProposeParams
    safety_params: _params_pb2.SafetyParams
    acknowledged_event_info: _bridge_event_info_pb2.BridgeEventInfo
    def __init__(self, event_params: _Optional[_Union[_params_pb2.EventParams, _Mapping]] = ..., propose_params: _Optional[_Union[_params_pb2.ProposeParams, _Mapping]] = ..., safety_params: _Optional[_Union[_params_pb2.SafetyParams, _Mapping]] = ..., acknowledged_event_info: _Optional[_Union[_bridge_event_info_pb2.BridgeEventInfo, _Mapping]] = ...) -> None: ...
