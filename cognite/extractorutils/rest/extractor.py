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
import json
import threading
from dataclasses import dataclass
from http import HTTPStatus
from logging import getLogger
from typing import Any, Callable, Dict, List, Optional, Set, Type, TypeVar, Union
from urllib.parse import urljoin

import dacite
import requests
from cognite.extractorutils.configtools import StateStoreConfig
from cognite.extractorutils.throttle import throttled_loop
from cognite.extractorutils.uploader_extractor import UploaderExtractor, UploaderExtractorConfig
from cognite.extractorutils.uploader_types import CdfTypes
from dacite import DaciteError
from requests.exceptions import JSONDecodeError

from cognite.extractorutils.rest.authentiaction import AuthConfig, AuthenticationProvider
from cognite.extractorutils.rest.http import (
    Endpoint,
    HttpCall,
    HttpMethod,
    HttpUrl,
    RequestBody,
    RequestBodyTemplate,
    ResponseType,
)


@dataclass
class SourceConfig:
    auth: Optional[AuthConfig] = None
    headers: Optional[Dict[str, str]] = None


@dataclass
class ExtractorConfig:
    state_store: StateStoreConfig = StateStoreConfig()


@dataclass
class RestConfig(UploaderExtractorConfig):
    source: SourceConfig = SourceConfig()
    extractor: ExtractorConfig = ExtractorConfig()


CustomRestConfig = TypeVar("CustomRestConfig", bound=RestConfig)


class RestExtractor(UploaderExtractor[CustomRestConfig]):
    def __init__(
        self,
        *,
        name: str,
        description: str,
        version: Optional[str] = None,
        base_url: Optional[str],
        headers: Optional[Dict[str, Union[str, Callable[[], str]]]] = None,
        cancelation_token: threading.Event = threading.Event(),
        config_class: Type[CustomRestConfig] = RestConfig,
        use_default_state_store: bool = True,
    ):
        super(RestExtractor, self).__init__(
            name=name,
            description=description,
            version=version,
            cancelation_token=cancelation_token,
            use_default_state_store=use_default_state_store,
            config_class=config_class,
        )
        self.base_url = base_url or ""
        self.headers: Dict[str, Union[str, Callable[[], str]]] = headers or {}
        self.endpoints: List[Endpoint] = []

    def endpoint(
        self,
        *,
        name: Optional[str] = None,
        method: HttpMethod,
        path: Union[str, Callable[[], str]],
        query: Dict[str, Any],
        headers: Dict[str, Union[str, Callable[[], str]]],
        body: Optional[RequestBodyTemplate],
        response_type: Type[ResponseType],
        next_page: Optional[Callable[[HttpCall], Optional[HttpUrl]]],
        interval: Optional[int],
    ) -> Callable[[Callable[[ResponseType], CdfTypes]], Callable[[ResponseType], CdfTypes]]:
        def decorator(func: Callable[[ResponseType], CdfTypes]) -> Callable[[ResponseType], CdfTypes]:
            self.endpoints.append(
                Endpoint(
                    name=name,
                    implementation=func,
                    method=method,
                    path=path,
                    query=query,
                    headers=headers,
                    body=body,
                    response_type=response_type,
                    next_page=next_page,
                    interval=interval,
                )
            )
            return func

        return decorator

    def get(
        self,
        path: Union[str, Callable[[], str]],
        *,
        name: Optional[str] = None,
        query: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Union[str, Callable[[], str]]]] = None,
        response_type: Type[ResponseType],
        next_page: Optional[Callable[[HttpCall], Optional[HttpUrl]]] = None,
        interval: Optional[int] = None,
    ) -> Callable[[Callable[[ResponseType], CdfTypes]], Callable[[ResponseType], CdfTypes]]:
        return self.endpoint(
            name=name,
            method=HttpMethod.GET,
            path=path,
            query=query or {},
            headers=headers or {},
            body=None,
            response_type=response_type,
            next_page=next_page,
            interval=interval,
        )

    def post(
        self,
        path: Union[str, Callable[[], str]],
        *,
        name: Optional[str] = None,
        query: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Union[str, Callable[[], str]]]] = None,
        body: Optional[RequestBodyTemplate],
        response_type: Type[ResponseType],
        next_page: Optional[Callable[[HttpCall], Optional[HttpUrl]]] = None,
        interval: Optional[int] = None,
    ) -> Callable[[Callable[[ResponseType], CdfTypes]], Callable[[ResponseType], CdfTypes]]:
        return self.endpoint(
            name=name,
            method=HttpMethod.POST,
            path=path,
            query=query or {},
            headers=headers or {},
            body=body,
            response_type=response_type,
            next_page=next_page,
            interval=interval,
        )

    def __enter__(self) -> "RestExtractor":
        super(RestExtractor, self).__enter__()

        self.authentication = AuthenticationProvider(self.config.source.auth)

        return self

    def prepare_headers(self, endpoint: Endpoint) -> Dict[str, str]:
        headers: Dict[str, str] = {k: _get_or_call(v) for k, v in self.headers.items()}

        for k, v in endpoint.headers.items():
            headers[k] = _get_or_call(v)

        if self.config.source.headers:
            for k2, v2 in self.config.source.headers.items():
                headers[k2] = v2

        if self.authentication.is_configured:
            headers["Authorization"] = self.authentication.auth_header

        if endpoint.body is not None:
            headers["Content-Type"] = "application/json"

        return headers

    def run(self) -> None:
        if not self.started:
            raise ValueError("You must run the extractor in a context manager")

        runners = []

        for endpoint in self.endpoints:
            runner = EndpointRunner(self, endpoint)
            runner.run()
            runners.append(runner)

        for runner in runners:
            runner.join()


class EndpointRunner:
    _threadnames: Set[str] = set()
    _threadname_counter = 0
    _threadname_lock = threading.RLock()

    def __init__(self, extractor: RestExtractor, endpoint: Endpoint):
        self.extractor = extractor
        self.endpoint = endpoint
        self.logger = getLogger(__name__)

        self.thread: Optional[threading.Thread] = None

    def get_threadname(self) -> str:
        with EndpointRunner._threadname_lock:
            if self.endpoint.name:
                name = self.endpoint.name
                if name in EndpointRunner._threadnames:
                    name += f"-{EndpointRunner._threadname_counter}"
                    EndpointRunner._threadname_counter += 1
            else:
                name = EndpointRunner._threadname_counter
                EndpointRunner._threadname_counter += 1
        return name

    def call(self, url: HttpUrl) -> HttpCall:
        self.logger.info(f"{self.endpoint.method.value} {url}")

        raw_response = requests.request(
            method=self.endpoint.method.value,
            url=str(url),
            data=_format_body(self.endpoint.body),
            headers=self.extractor.prepare_headers(self.endpoint),
        )
        if raw_response.status_code >= 400:
            status = HTTPStatus(raw_response.status_code)
            self.logger.error(f"Error from source. {raw_response.status_code}: {status.name} - {status.description}")

        try:
            data = raw_response.json()
            response = dacite.from_dict(self.endpoint.response_type, data)
        except (JSONDecodeError, DaciteError) as e:
            self.logger.error(f"Error while parsing response: {str(e)}")
            raise e

        result = self.endpoint.implementation(response)
        self.extractor.handle_output(result)

        return HttpCall(url=url, response=response)

    def _try_get_next_page(self, previous__call: HttpCall) -> Optional[HttpUrl]:
        if self.endpoint.next_page is None:
            return None
        return self.endpoint.next_page(previous__call)

    def exhaust_endpoint(self) -> None:
        next_url = HttpUrl(
            urljoin(
                self.extractor.base_url,
                self.endpoint.path() if callable(self.endpoint.path) else self.endpoint.path,
            )
        )

        while next_url is not None and not self.extractor.cancelation_token.is_set():
            call = self.call(next_url)
            next_url = self._try_get_next_page(call)

    def run(self) -> None:
        def loop() -> None:
            for _ in throttled_loop(
                target_time=self.endpoint.interval, cancelation_token=self.extractor.cancelation_token
            ):
                self.exhaust_endpoint()

        threadname = self.get_threadname()

        self.thread = threading.Thread(
            target=loop if self.endpoint.interval is not None else self.exhaust_endpoint,
            name=f"EndpointRunner-{threadname}",
        )
        self.thread.start()

    def join(self) -> None:
        if self.thread is not None:
            self.thread.join()


T = TypeVar("T")


def _get_or_call(item: Union[T, Callable[[], T]]) -> T:
    return item() if callable(item) else item


def _format_body(body: Optional[RequestBodyTemplate]) -> Optional[str]:
    if body is None:
        return None

    def recursive_get_or_call(item: RequestBodyTemplate) -> RequestBody:
        if isinstance(item, dict):
            return {k: recursive_get_or_call(v) for k, v in item.items()}
        elif isinstance(item, list):
            return [recursive_get_or_call(i) for i in item]
        else:
            res = _get_or_call(item)
            if isinstance(res, (list, dict)):
                return recursive_get_or_call(res)
            return res

    return json.dumps(recursive_get_or_call(body))
