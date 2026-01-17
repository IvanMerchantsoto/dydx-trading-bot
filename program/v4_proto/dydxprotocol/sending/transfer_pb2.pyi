from v4_proto.cosmos_proto import cosmos_pb2 as _cosmos_pb2
from v4_proto.cosmos.base.v1beta1 import coin_pb2 as _coin_pb2
from v4_proto.cosmos.msg.v1 import msg_pb2 as _msg_pb2
from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from v4_proto.dydxprotocol.subaccounts import subaccount_pb2 as _subaccount_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Transfer(_message.Message):
    __slots__ = ("sender", "recipient", "asset_id", "amount")
    SENDER_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_FIELD_NUMBER: _ClassVar[int]
    ASSET_ID_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    sender: _subaccount_pb2.SubaccountId
    recipient: _subaccount_pb2.SubaccountId
    asset_id: int
    amount: int
    def __init__(self, sender: _Optional[_Union[_subaccount_pb2.SubaccountId, _Mapping]] = ..., recipient: _Optional[_Union[_subaccount_pb2.SubaccountId, _Mapping]] = ..., asset_id: _Optional[int] = ..., amount: _Optional[int] = ...) -> None: ...

class MsgDepositToSubaccount(_message.Message):
    __slots__ = ("sender", "recipient", "asset_id", "quantums")
    SENDER_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_FIELD_NUMBER: _ClassVar[int]
    ASSET_ID_FIELD_NUMBER: _ClassVar[int]
    QUANTUMS_FIELD_NUMBER: _ClassVar[int]
    sender: str
    recipient: _subaccount_pb2.SubaccountId
    asset_id: int
    quantums: int
    def __init__(self, sender: _Optional[str] = ..., recipient: _Optional[_Union[_subaccount_pb2.SubaccountId, _Mapping]] = ..., asset_id: _Optional[int] = ..., quantums: _Optional[int] = ...) -> None: ...

class MsgWithdrawFromSubaccount(_message.Message):
    __slots__ = ("sender", "recipient", "asset_id", "quantums")
    SENDER_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_FIELD_NUMBER: _ClassVar[int]
    ASSET_ID_FIELD_NUMBER: _ClassVar[int]
    QUANTUMS_FIELD_NUMBER: _ClassVar[int]
    sender: _subaccount_pb2.SubaccountId
    recipient: str
    asset_id: int
    quantums: int
    def __init__(self, sender: _Optional[_Union[_subaccount_pb2.SubaccountId, _Mapping]] = ..., recipient: _Optional[str] = ..., asset_id: _Optional[int] = ..., quantums: _Optional[int] = ...) -> None: ...

class MsgSendFromModuleToAccount(_message.Message):
    __slots__ = ("authority", "sender_module_name", "recipient", "coin")
    AUTHORITY_FIELD_NUMBER: _ClassVar[int]
    SENDER_MODULE_NAME_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_FIELD_NUMBER: _ClassVar[int]
    COIN_FIELD_NUMBER: _ClassVar[int]
    authority: str
    sender_module_name: str
    recipient: str
    coin: _coin_pb2.Coin
    def __init__(self, authority: _Optional[str] = ..., sender_module_name: _Optional[str] = ..., recipient: _Optional[str] = ..., coin: _Optional[_Union[_coin_pb2.Coin, _Mapping]] = ...) -> None: ...

class MsgSendFromAccountToAccount(_message.Message):
    __slots__ = ("authority", "sender", "recipient", "coin")
    AUTHORITY_FIELD_NUMBER: _ClassVar[int]
    SENDER_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_FIELD_NUMBER: _ClassVar[int]
    COIN_FIELD_NUMBER: _ClassVar[int]
    authority: str
    sender: str
    recipient: str
    coin: _coin_pb2.Coin
    def __init__(self, authority: _Optional[str] = ..., sender: _Optional[str] = ..., recipient: _Optional[str] = ..., coin: _Optional[_Union[_coin_pb2.Coin, _Mapping]] = ...) -> None: ...
