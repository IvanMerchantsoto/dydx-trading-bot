from v4_proto.cosmos_proto import cosmos_pb2 as _cosmos_pb2
from v4_proto.cosmos.msg.v1 import msg_pb2 as _msg_pb2
from v4_proto.amino import amino_pb2 as _amino_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class MsgAddAuthenticator(_message.Message):
    __slots__ = ("sender", "authenticator_type", "data")
    SENDER_FIELD_NUMBER: _ClassVar[int]
    AUTHENTICATOR_TYPE_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    sender: str
    authenticator_type: str
    data: bytes
    def __init__(self, sender: _Optional[str] = ..., authenticator_type: _Optional[str] = ..., data: _Optional[bytes] = ...) -> None: ...

class MsgAddAuthenticatorResponse(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class MsgRemoveAuthenticator(_message.Message):
    __slots__ = ("sender", "id")
    SENDER_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    sender: str
    id: int
    def __init__(self, sender: _Optional[str] = ..., id: _Optional[int] = ...) -> None: ...

class MsgRemoveAuthenticatorResponse(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class MsgSetActiveState(_message.Message):
    __slots__ = ("authority", "active")
    AUTHORITY_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    authority: str
    active: bool
    def __init__(self, authority: _Optional[str] = ..., active: bool = ...) -> None: ...

class MsgSetActiveStateResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class TxExtension(_message.Message):
    __slots__ = ("selected_authenticators",)
    SELECTED_AUTHENTICATORS_FIELD_NUMBER: _ClassVar[int]
    selected_authenticators: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, selected_authenticators: _Optional[_Iterable[int]] = ...) -> None: ...
