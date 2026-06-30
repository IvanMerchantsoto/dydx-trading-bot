from v4_proto.cosmos_proto import cosmos_pb2 as _cosmos_pb2
from v4_proto.dydxprotocol.subaccounts import asset_position_pb2 as _asset_position_pb2
from v4_proto.dydxprotocol.subaccounts import perpetual_position_pb2 as _perpetual_position_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SubaccountId(_message.Message):
    __slots__ = ("owner", "number")
    OWNER_FIELD_NUMBER: _ClassVar[int]
    NUMBER_FIELD_NUMBER: _ClassVar[int]
    owner: str
    number: int
    def __init__(self, owner: _Optional[str] = ..., number: _Optional[int] = ...) -> None: ...

class Subaccount(_message.Message):
    __slots__ = ("id", "asset_positions", "perpetual_positions", "margin_enabled")
    ID_FIELD_NUMBER: _ClassVar[int]
    ASSET_POSITIONS_FIELD_NUMBER: _ClassVar[int]
    PERPETUAL_POSITIONS_FIELD_NUMBER: _ClassVar[int]
    MARGIN_ENABLED_FIELD_NUMBER: _ClassVar[int]
    id: SubaccountId
    asset_positions: _containers.RepeatedCompositeFieldContainer[_asset_position_pb2.AssetPosition]
    perpetual_positions: _containers.RepeatedCompositeFieldContainer[_perpetual_position_pb2.PerpetualPosition]
    margin_enabled: bool
    def __init__(self, id: _Optional[_Union[SubaccountId, _Mapping]] = ..., asset_positions: _Optional[_Iterable[_Union[_asset_position_pb2.AssetPosition, _Mapping]]] = ..., perpetual_positions: _Optional[_Iterable[_Union[_perpetual_position_pb2.PerpetualPosition, _Mapping]]] = ..., margin_enabled: bool = ...) -> None: ...
