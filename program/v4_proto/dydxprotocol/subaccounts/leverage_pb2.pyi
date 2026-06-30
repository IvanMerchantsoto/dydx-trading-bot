from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PerpetualLeverageEntry(_message.Message):
    __slots__ = ("perpetual_id", "custom_imf_ppm")
    PERPETUAL_ID_FIELD_NUMBER: _ClassVar[int]
    CUSTOM_IMF_PPM_FIELD_NUMBER: _ClassVar[int]
    perpetual_id: int
    custom_imf_ppm: int
    def __init__(self, perpetual_id: _Optional[int] = ..., custom_imf_ppm: _Optional[int] = ...) -> None: ...

class LeverageData(_message.Message):
    __slots__ = ("entries",)
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[PerpetualLeverageEntry]
    def __init__(self, entries: _Optional[_Iterable[_Union[PerpetualLeverageEntry, _Mapping]]] = ...) -> None: ...
