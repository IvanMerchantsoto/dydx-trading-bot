from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from v4_proto.cosmos.base.v1beta1 import coin_pb2 as _coin_pb2
from v4_proto.cosmos.bank.v1beta1 import bank_pb2 as _bank_pb2
from v4_proto.cosmos_proto import cosmos_pb2 as _cosmos_pb2
from v4_proto.amino import amino_pb2 as _amino_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GenesisState(_message.Message):
    __slots__ = ("params", "balances", "supply", "denom_metadata", "send_enabled")
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    BALANCES_FIELD_NUMBER: _ClassVar[int]
    SUPPLY_FIELD_NUMBER: _ClassVar[int]
    DENOM_METADATA_FIELD_NUMBER: _ClassVar[int]
    SEND_ENABLED_FIELD_NUMBER: _ClassVar[int]
    params: _bank_pb2.Params
    balances: _containers.RepeatedCompositeFieldContainer[Balance]
    supply: _containers.RepeatedCompositeFieldContainer[_coin_pb2.Coin]
    denom_metadata: _containers.RepeatedCompositeFieldContainer[_bank_pb2.Metadata]
    send_enabled: _containers.RepeatedCompositeFieldContainer[_bank_pb2.SendEnabled]
    def __init__(self, params: _Optional[_Union[_bank_pb2.Params, _Mapping]] = ..., balances: _Optional[_Iterable[_Union[Balance, _Mapping]]] = ..., supply: _Optional[_Iterable[_Union[_coin_pb2.Coin, _Mapping]]] = ..., denom_metadata: _Optional[_Iterable[_Union[_bank_pb2.Metadata, _Mapping]]] = ..., send_enabled: _Optional[_Iterable[_Union[_bank_pb2.SendEnabled, _Mapping]]] = ...) -> None: ...

class Balance(_message.Message):
    __slots__ = ("address", "coins")
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    COINS_FIELD_NUMBER: _ClassVar[int]
    address: str
    coins: _containers.RepeatedCompositeFieldContainer[_coin_pb2.Coin]
    def __init__(self, address: _Optional[str] = ..., coins: _Optional[_Iterable[_Union[_coin_pb2.Coin, _Mapping]]] = ...) -> None: ...
