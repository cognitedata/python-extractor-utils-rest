from enum import Enum
from typing import TypeVar, Generic
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
