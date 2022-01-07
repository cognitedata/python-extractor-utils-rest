import threading
from dataclasses import dataclass
from os import PathLike
from types import TracebackType
from typing import Any, Callable, Dict, Generic, Iterable, List, Optional, Tuple, Type, Union
from urllib.parse import urljoin

import dacite
import requests
from cognite.client.data_classes import Datapoint, Event, FileMetadata, Row
from cognite.extractorutils.authentication import Authenticator, AuthenticatorConfig
from cognite.extractorutils.configtools import BaseConfig
from cognite.extractorutils.uploader import EventUploadQueue
from more_itertools import peekable

from cognite.extractorutils import Extractor
from cognite.extractorutils.rest.http import HttpCall, HttpMethod, HttpUrl, RequestBody, ResponseType

FullFile = Union[Tuple[FileMetadata, PathLike], Tuple[FileMetadata, bytes]]

CdfTypes = Union[
    Event, Iterable[Event], Datapoint, Iterable[Datapoint], Row, Iterable[Row], FullFile, Iterable[FullFile]
]


@dataclass
class Endpoint(Generic[ResponseType]):
    implementation: Callable[[ResponseType], CdfTypes]
    method: HttpMethod
    path: str
    query: Dict[str, Any]
    headers: Dict[str, Union[str, Callable[[], str]]]
    body: Optional[RequestBody]
    response_type: Type[ResponseType]
    next_url: Optional[Callable[[str, ResponseType], Optional[str]]]
    interval: Optional[int]


@dataclass
class SourceConfig:
    idp_authentication: Optional[AuthenticatorConfig] = None
    headers: Optional[Dict[str, Any]] = None


@dataclass
class RestConfig(BaseConfig):
    source: SourceConfig = SourceConfig()


class RestExtractor(Extractor[RestConfig]):
    def __init__(
        self,
        *,
        name: str,
        description: str,
        version: Optional[str] = None,
        base_url: Optional[str],
        headers: Optional[Dict[str, Union[str, Callable[[], str]]]] = None,
        cancelation_token: Event = threading.Event(),
    ):
        super(RestExtractor, self).__init__(
            name=name,
            description=description,
            version=version,
            cancelation_token=cancelation_token,
            use_default_state_store=False,
            config_class=RestConfig,
        )
        self.base_url = base_url or ""
        self.headers: Dict[str, Union[str, Callable[[], str]]] = headers or {}
        self.endpoints: List[Endpoint] = []

        self.authenticator: Optional[Authenticator]
        if self.config.source.idp_authentication:
            self.authenticator = Authenticator(self.config.source.idp_authentication)
        else:
            self.authenticator = None

    def endpoint(
        self,
        *,
        method: HttpMethod,
        path: str,
        query: Dict[str, Any],
        headers: Dict[str, Union[str, Callable[[], str]]],
        body: Optional[RequestBody],
        response_type: Type[ResponseType],
        next_url: Optional[Callable[[str, ResponseType], Optional[str]]],
        interval: Optional[int],
    ):
        def decorator(func):
            self.endpoints.append(
                Endpoint(
                    implementation=func,
                    method=method,
                    path=path,
                    query=query,
                    headers=headers,
                    body=body,
                    response_type=response_type,
                    next_url=next_url,
                    interval=interval,
                )
            )
            return func

        return decorator

    def get(
        self,
        path: str,
        *,
        query: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Union[str, Callable[[], str]]]] = None,
        response_type: Type[ResponseType],
        next_url: Optional[Callable[[HttpCall], Optional[HttpUrl]]] = None,
        interval: Optional[int] = None,
    ):
        return self.endpoint(
            method=HttpMethod.GET,
            path=path,
            query=query,
            headers=headers or {},
            body=None,
            response_type=response_type,
            next_url=next_url,
            interval=interval,
        )

    def __enter__(self) -> "RestExtractor":
        super(RestExtractor, self).__enter__()
        # TODO: Add more uplaod queues
        self.event_queue = EventUploadQueue(
            self.cognite_client, max_queue_size=10_000, max_upload_interval=60, trigger_log_level="INFO"
        ).__enter__()
        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> bool:
        self.event_queue.__exit__(exc_type, exc_val, exc_tb)
        return super(RestExtractor, self).__exit__(exc_type, exc_val, exc_tb)

    def _handle_output(self, output: CdfTypes) -> None:
        if not isinstance(output, Iterable):
            output = [output]

        peekable_output = peekable(output)

        # TODO: Handle more resource types
        if isinstance(peekable_output.peek(), Event):
            for event in peekable_output:
                self.event_queue.add_to_upload_queue(event)
        else:
            raise ValueError(f"Unexpected type: {type(output[0])}")

    def _get_or_call(self, item: Union[str, Callable[[], str]]) -> str:
        return item() if isinstance(item, Callable) else item

    def _prepare_headers(self, endpoint: Endpoint) -> Dict[str, str]:
        headers = {k: self._get_or_call(v) for k, v in self.headers.items()}

        for k, v in endpoint.headers.items():
            headers[k] = self._get_or_call(v)

        for k, v in self.config.source.headers.items():
            headers[k] = v

        if self.authenticator:
            headers["Authentication"] = f"Bearer {self.authenticator.get_token()}"

        return headers

    def run(self) -> None:
        if not self.started:
            raise ValueError("You must run the extractor in a context manager")

        for endpoint in self.endpoints:
            url = urljoin(self.base_url, endpoint.path)
            self.logger.info(f"{endpoint.method.value} {url}")
            raw_response = requests.request(
                method=endpoint.method.value, url=url, headers=self._prepare_headers(endpoint)
            )
            data = raw_response.json()
            response = dacite.from_dict(endpoint.response_type, data)
            result = endpoint.implementation(response)
            self._handle_output(result)
