from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Params(_message.Message):
    __slots__ = ("treasury_account", "denom", "denom_exponent", "market_id", "fee_multiplier_ppm")
    TREASURY_ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    DENOM_FIELD_NUMBER: _ClassVar[int]
    DENOM_EXPONENT_FIELD_NUMBER: _ClassVar[int]
    MARKET_ID_FIELD_NUMBER: _ClassVar[int]
    FEE_MULTIPLIER_PPM_FIELD_NUMBER: _ClassVar[int]
    treasury_account: str
    denom: str
    denom_exponent: int
    market_id: int
    fee_multiplier_ppm: int
    def __init__(self, treasury_account: _Optional[str] = ..., denom: _Optional[str] = ..., denom_exponent: _Optional[int] = ..., market_id: _Optional[int] = ..., fee_multiplier_ppm: _Optional[int] = ...) -> None: ...
