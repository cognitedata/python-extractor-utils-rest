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

from enum import Enum
from typing import Generic, TypeVar
from urllib.parse import urlparse
from uuid import UUID

import arrow

RequestBody = TypeVar("RequestBody")
ResponseType = TypeVar("ResponseType")


class HttpMethod(Enum):
    GET = "GET"
    POST = "POST"


class HttpUrl:
    def __init__(self, url: str):
        parse_res = urlparse(url)
        self.scheme = parse_res.scheme
        self.netloc = parse_res.netloc
        self.path = parse_res.path
        self.query = {k: v for k, v in [i.split("=") for i in parse_res.query.split("&")]}
        self.fragment = parse_res.fragment

    def __str__(self) -> str:
        query = f"?{'&'.join([f'{k}={v}' for k, v in self.query.items()])}" if self.query else ""
        fragment = f"#{self.fragment}" if self.fragment else ""
        return f"{self.scheme}://{self.netloc}/{self.path}{query}{fragment}"

    def __repr__(self) -> str:
        return self.__str__()


class HttpCall(Generic[ResponseType]):
    def __init__(self, url: HttpUrl, response: ResponseType):
        self.uuid = UUID()
        self.url = url
        self.response = response
        self.time = arrow.get().float_timestamp
