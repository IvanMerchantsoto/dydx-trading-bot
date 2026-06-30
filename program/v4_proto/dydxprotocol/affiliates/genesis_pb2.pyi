from v4_proto.gogoproto import gogo_pb2 as _gogo_pb2
from v4_proto.dydxprotocol.affiliates import affiliates_pb2 as _affiliates_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GenesisState(_message.Message):
    __slots__ = ("affiliate_tiers", "affiliate_parameters")
    AFFILIATE_TIERS_FIELD_NUMBER: _ClassVar[int]
    AFFILIATE_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    affiliate_tiers: _affiliates_pb2.AffiliateTiers
    affiliate_parameters: _affiliates_pb2.AffiliateParameters
    def __init__(self, affiliate_tiers: _Optional[_Union[_affiliates_pb2.AffiliateTiers, _Mapping]] = ..., affiliate_parameters: _Optional[_Union[_affiliates_pb2.AffiliateParameters, _Mapping]] = ...) -> None: ...
