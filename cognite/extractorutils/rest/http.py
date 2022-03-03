#  Copyright 2022 Cognite AS
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Generic, List, Optional, Type, TypeVar, Union
from urllib.parse import urlparse
from uuid import uuid4

import arrow
from cognite.extractorutils.uploader_types import CdfTypes

ResponseType = TypeVar("ResponseType")

# Ignoring types here, since recursive types are not yet supported by mypy:  https://github.com/python/mypy/issues/731
JsonTypes = Union[str, int, float, bool]
RequestBody = Union[JsonTypes, List["RequestBody"], Dict[str, "RequestBody"]]  # type: ignore
RequestBodyTemplate = Union[  # type: ignore
    JsonTypes,
    Callable[[], JsonTypes],
    List["RequestBodyTemplate"],  # type: ignore
    Callable[[], List["RequestBodyTemplate"]],  # type: ignore
    Dict[str, "RequestBodyTemplate"],  # type: ignore
    Callable[[], Dict[str, "RequestBodyTemplate"]],  # type: ignore
]


class HttpMethod(Enum):
    GET = "GET"
    POST = "POST"


class HttpUrl:
    def __init__(self, url: str):
        parse_res = urlparse(url)
        self.scheme = parse_res.scheme
        self.netloc = parse_res.netloc
        self.path = parse_res.path
        if parse_res.query:
            self.query = {k: v for k, v in [i.split("=") for i in parse_res.query.split("&")]}
        else:
            self.query = {}
        self.fragment = parse_res.fragment

    def __str__(self) -> str:
        query = f"?{'&'.join([f'{k}={v}' for k, v in self.query.items()])}" if self.query else ""
        fragment = f"#{self.fragment}" if self.fragment else ""
        return f"{self.scheme}://{self.netloc}{self.path}{query}{fragment}"

    def __repr__(self) -> str:
        return self.__str__()


class HttpCall(Generic[ResponseType]):
    def __init__(self, url: HttpUrl, response: ResponseType):
        self.uuid = uuid4()
        self.url = url
        self.response = response
        self.time = arrow.get().float_timestamp


@dataclass
class Endpoint(Generic[ResponseType]):
    name: Optional[str]
    implementation: Callable[[ResponseType], CdfTypes]
    method: HttpMethod
    path: Union[str, Callable[[], str]]
    query: Dict[str, Any]
    headers: Dict[str, Union[str, Callable[[], str]]]
    body: Optional[RequestBody]
    response_type: Type[ResponseType]
    next_page: Optional[Callable[[HttpCall], Optional[HttpUrl]]]
    interval: Optional[int]
