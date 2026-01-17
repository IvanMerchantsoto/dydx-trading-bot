from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class MarketParam(_message.Message):
    __slots__ = ("id", "pair", "exponent", "min_exchanges", "min_price_change_ppm", "exchange_config_json")
    ID_FIELD_NUMBER: _ClassVar[int]
    PAIR_FIELD_NUMBER: _ClassVar[int]
    EXPONENT_FIELD_NUMBER: _ClassVar[int]
    MIN_EXCHANGES_FIELD_NUMBER: _ClassVar[int]
    MIN_PRICE_CHANGE_PPM_FIELD_NUMBER: _ClassVar[int]
    EXCHANGE_CONFIG_JSON_FIELD_NUMBER: _ClassVar[int]
    id: int
    pair: str
    exponent: int
    min_exchanges: int
    min_price_change_ppm: int
    exchange_config_json: str
    def __init__(self, id: _Optional[int] = ..., pair: _Optional[str] = ..., exponent: _Optional[int] = ..., min_exchanges: _Optional[int] = ..., min_price_change_ppm: _Optional[int] = ..., exchange_config_json: _Optional[str] = ...) -> None: ...
