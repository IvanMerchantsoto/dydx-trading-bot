from v4_proto.cosmos_proto import cosmos_pb2 as _cosmos_pb2
from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from v4_proto.dydxprotocol.affiliates import affiliates_pb2 as _affiliates_pb2
from v4_proto.google.api import annotations_pb2 as _annotations_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AffiliateInfoRequest(_message.Message):
    __slots__ = ("address",)
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    address: str
    def __init__(self, address: _Optional[str] = ...) -> None: ...

class AffiliateInfoResponse(_message.Message):
    __slots__ = ("is_whitelisted", "tier", "fee_share_ppm", "referred_volume", "staked_amount", "referred_volume_30d_rolling", "attributed_volume_30d_rolling")
    IS_WHITELISTED_FIELD_NUMBER: _ClassVar[int]
    TIER_FIELD_NUMBER: _ClassVar[int]
    FEE_SHARE_PPM_FIELD_NUMBER: _ClassVar[int]
    REFERRED_VOLUME_FIELD_NUMBER: _ClassVar[int]
    STAKED_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    REFERRED_VOLUME_30D_ROLLING_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTED_VOLUME_30D_ROLLING_FIELD_NUMBER: _ClassVar[int]
    is_whitelisted: bool
    tier: int
    fee_share_ppm: int
    referred_volume: bytes
    staked_amount: bytes
    referred_volume_30d_rolling: bytes
    attributed_volume_30d_rolling: bytes
    def __init__(self, is_whitelisted: bool = ..., tier: _Optional[int] = ..., fee_share_ppm: _Optional[int] = ..., referred_volume: _Optional[bytes] = ..., staked_amount: _Optional[bytes] = ..., referred_volume_30d_rolling: _Optional[bytes] = ..., attributed_volume_30d_rolling: _Optional[bytes] = ...) -> None: ...

class ReferredByRequest(_message.Message):
    __slots__ = ("address",)
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    address: str
    def __init__(self, address: _Optional[str] = ...) -> None: ...

class ReferredByResponse(_message.Message):
    __slots__ = ("affiliate_address",)
    AFFILIATE_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    affiliate_address: str
    def __init__(self, affiliate_address: _Optional[str] = ...) -> None: ...

class AllAffiliateTiersRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class AllAffiliateTiersResponse(_message.Message):
    __slots__ = ("tiers",)
    TIERS_FIELD_NUMBER: _ClassVar[int]
    tiers: _affiliates_pb2.AffiliateTiers
    def __init__(self, tiers: _Optional[_Union[_affiliates_pb2.AffiliateTiers, _Mapping]] = ...) -> None: ...

class AffiliateWhitelistRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class AffiliateWhitelistResponse(_message.Message):
    __slots__ = ("whitelist",)
    WHITELIST_FIELD_NUMBER: _ClassVar[int]
    whitelist: _affiliates_pb2.AffiliateWhitelist
    def __init__(self, whitelist: _Optional[_Union[_affiliates_pb2.AffiliateWhitelist, _Mapping]] = ...) -> None: ...

class AffiliateOverridesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class AffiliateOverridesResponse(_message.Message):
    __slots__ = ("overrides",)
    OVERRIDES_FIELD_NUMBER: _ClassVar[int]
    overrides: _affiliates_pb2.AffiliateOverrides
    def __init__(self, overrides: _Optional[_Union[_affiliates_pb2.AffiliateOverrides, _Mapping]] = ...) -> None: ...

class AffiliateParametersRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class AffiliateParametersResponse(_message.Message):
    __slots__ = ("parameters",)
    PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    parameters: _affiliates_pb2.AffiliateParameters
    def __init__(self, parameters: _Optional[_Union[_affiliates_pb2.AffiliateParameters, _Mapping]] = ...) -> None: ...
