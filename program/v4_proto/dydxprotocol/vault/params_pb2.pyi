from v4_proto.cosmos_proto import cosmos_pb2 as _cosmos_pb2
from v4_proto.dydxprotocol.vault import vault_pb2 as _vault_pb2
from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class QuotingParams(_message.Message):
    __slots__ = ("layers", "spread_min_ppm", "spread_buffer_ppm", "skew_factor_ppm", "order_size_pct_ppm", "order_expiration_seconds", "activation_threshold_quote_quantums")
    LAYERS_FIELD_NUMBER: _ClassVar[int]
    SPREAD_MIN_PPM_FIELD_NUMBER: _ClassVar[int]
    SPREAD_BUFFER_PPM_FIELD_NUMBER: _ClassVar[int]
    SKEW_FACTOR_PPM_FIELD_NUMBER: _ClassVar[int]
    ORDER_SIZE_PCT_PPM_FIELD_NUMBER: _ClassVar[int]
    ORDER_EXPIRATION_SECONDS_FIELD_NUMBER: _ClassVar[int]
    ACTIVATION_THRESHOLD_QUOTE_QUANTUMS_FIELD_NUMBER: _ClassVar[int]
    layers: int
    spread_min_ppm: int
    spread_buffer_ppm: int
    skew_factor_ppm: int
    order_size_pct_ppm: int
    order_expiration_seconds: int
    activation_threshold_quote_quantums: bytes
    def __init__(self, layers: _Optional[int] = ..., spread_min_ppm: _Optional[int] = ..., spread_buffer_ppm: _Optional[int] = ..., skew_factor_ppm: _Optional[int] = ..., order_size_pct_ppm: _Optional[int] = ..., order_expiration_seconds: _Optional[int] = ..., activation_threshold_quote_quantums: _Optional[bytes] = ...) -> None: ...

class VaultParams(_message.Message):
    __slots__ = ("status", "quoting_params")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    QUOTING_PARAMS_FIELD_NUMBER: _ClassVar[int]
    status: _vault_pb2.VaultStatus
    quoting_params: QuotingParams
    def __init__(self, status: _Optional[_Union[_vault_pb2.VaultStatus, str]] = ..., quoting_params: _Optional[_Union[QuotingParams, _Mapping]] = ...) -> None: ...

class OperatorParams(_message.Message):
    __slots__ = ("operator", "metadata")
    OPERATOR_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    operator: str
    metadata: OperatorMetadata
    def __init__(self, operator: _Optional[str] = ..., metadata: _Optional[_Union[OperatorMetadata, _Mapping]] = ...) -> None: ...

class OperatorMetadata(_message.Message):
    __slots__ = ("name", "description")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    name: str
    description: str
    def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...

class Params(_message.Message):
    __slots__ = ("layers", "spread_min_ppm", "spread_buffer_ppm", "skew_factor_ppm", "order_size_pct_ppm", "order_expiration_seconds", "activation_threshold_quote_quantums")
    LAYERS_FIELD_NUMBER: _ClassVar[int]
    SPREAD_MIN_PPM_FIELD_NUMBER: _ClassVar[int]
    SPREAD_BUFFER_PPM_FIELD_NUMBER: _ClassVar[int]
    SKEW_FACTOR_PPM_FIELD_NUMBER: _ClassVar[int]
    ORDER_SIZE_PCT_PPM_FIELD_NUMBER: _ClassVar[int]
    ORDER_EXPIRATION_SECONDS_FIELD_NUMBER: _ClassVar[int]
    ACTIVATION_THRESHOLD_QUOTE_QUANTUMS_FIELD_NUMBER: _ClassVar[int]
    layers: int
    spread_min_ppm: int
    spread_buffer_ppm: int
    skew_factor_ppm: int
    order_size_pct_ppm: int
    order_expiration_seconds: int
    activation_threshold_quote_quantums: bytes
    def __init__(self, layers: _Optional[int] = ..., spread_min_ppm: _Optional[int] = ..., spread_buffer_ppm: _Optional[int] = ..., skew_factor_ppm: _Optional[int] = ..., order_size_pct_ppm: _Optional[int] = ..., order_expiration_seconds: _Optional[int] = ..., activation_threshold_quote_quantums: _Optional[bytes] = ...) -> None: ...
