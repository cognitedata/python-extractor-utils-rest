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
from types import TracebackType
from typing import Any, Callable, Dict, List, Optional, Set, Type, TypeVar, Union
from urllib.parse import urljoin

import dacite
import requests
from cognite.extractorutils.configtools import StateStoreConfig
from cognite.extractorutils.retry import retry
from cognite.extractorutils.throttle import throttled_loop
from cognite.extractorutils.uploader_extractor import UploaderExtractor, UploaderExtractorConfig
from cognite.extractorutils.uploader_types import CdfTypes
from dacite import DaciteError
from requests import Response
from requests.exceptions import HTTPError, JSONDecodeError

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
class RetryConfig:
    backoff_factor: float = 1.5
    max_delay: float = 60
    delay: float = 5
    number: int = 5
    jitter: float = 1


@dataclass
class SourceConfig:
    base_url: Optional[str] = None
    auth: Optional[AuthConfig] = None
    headers: Optional[Dict[str, str]] = None

    retries: RetryConfig = RetryConfig()


@dataclass
class ExtractorConfig:
    state_store: StateStoreConfig = StateStoreConfig()


@dataclass
class RestConfig(UploaderExtractorConfig):
    source: SourceConfig = SourceConfig()
    extractor: ExtractorConfig = ExtractorConfig()


CustomRestConfig = TypeVar("CustomRestConfig", bound=RestConfig)


class RestExtractor(UploaderExtractor[CustomRestConfig]):
    """
    Class for data extraction from RESTful systems.

    Args:
        name: Name of the extractor, how it's invoked from the command line.
        description: A short 1-2 sentence description of the extractor.
        version: Version number, following semantic versioning.
        base_url: Base URL for all calls. Will be ``urljoin``ed with relative paths provided in decorators. If no base
            URL is given, a full URL must be given in each decorator instead. Base URLs can also be given in
            configuration.
        headers: A dictionary of headers to add to every HTTP request.
        config_class: A class (based on the CustomRestConfig class) that defines the configuration schema for the
            extractor
        use_default_state_store: Create a simple instance of the LocalStateStore to provide to the run handle. If false
            a NoStateStore will be created in its place.
        cancelation_token: An event that will be set when the extractor should shut down, an empty one will be created
            if omitted.
    """

    def __init__(
        self,
        *,
        name: str,
        description: str,
        version: Optional[str] = None,
        base_url: Optional[str] = None,
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
        self._default_base_url = base_url or ""
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
        """
        A generic endpoint decorator. Not meant to be used directly, use ``get`` or ``post`` instead.
        """

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

    def endpoint_list(
        self,
        *,
        name: Optional[str] = None,
        method: HttpMethod,
        paths: List[Union[str, Callable[[], str]]],
        query: Dict[str, Any],
        headers: Dict[str, Union[str, Callable[[], str]]],
        body: Optional[RequestBodyTemplate],
        response_type: Type[ResponseType],
        next_page: Optional[Callable[[HttpCall], Optional[HttpUrl]]],
        interval: Optional[int],
    ) -> Callable[[Callable[[ResponseType], CdfTypes]], Callable[[ResponseType], CdfTypes]]:
        """
        A generic endpoint decorator. Not meant to be used directly, use ``get`` or ``post`` instead.
        """

        def decorator(func: Callable[[ResponseType], CdfTypes]) -> Callable[[ResponseType], CdfTypes]:
            endpoints = []
            for path in paths:
                endpoints.append(
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
            self.endpoints.extend(endpoints)
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
        """
        Perform a GET request and give the result to the decorated function. The output of the decorated function will
        be handled and uploaded to CDF.

        Args:
            path: Relative (if a base URL was given) or absolute (of no base URL) path to make request to
            name: A readable name for this request, used in logging.
            query: Query parameters. Values can either be values or callables giving values.
            headers: Headers. Values can either be values or callables giving values.
            response_type: Class to deserialize response JSON into
            next_page: A callable taking an HttpCall and returning the next HttpUrl to make a request to.
            interval: A target iteration time. If given, the extractor will make periodic requests instead of a single
                request.
        """
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
        """
        Perform a POST request and give the result to the decorated function. The output of the decorated function will
        be handled and uploaded to CDF.

        Args:
            path: Relative (if a base URL was given) or absolute (of no base URL) path to make request to
            name: A readable name for this request, used in e.g. logging.
            query: Query parameters. Values can either be values or callables giving values.
            headers: Headers. Values can either be values or callables giving values.
            body: Request body represented as a dictionary. Will be serialized into JSON. Values can either be values
                or callables giving values.
            response_type: Class to deserialize response JSON into
            next_page: A callable taking an HttpCall and returning the next HttpUrl to make a request to.
            interval: A target iteration time. If given, the extractor will make periodic requests instead of a single
                request.
        """
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

    def get_multiple(
        self,
        paths: List[Union[str, Callable[[], str]]],
        *,
        name: Optional[str] = None,
        query: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Union[str, Callable[[], str]]]] = None,
        response_type: Type[ResponseType],
        next_page: Optional[Callable[[HttpCall], Optional[HttpUrl]]] = None,
        interval: Optional[int] = None,
    ) -> Callable[[Callable[[ResponseType], CdfTypes]], Callable[[ResponseType], CdfTypes]]:
        """
        Perform a GET request and give the result to the decorated function. The output of the decorated function will
        be handled and uploaded to CDF.

        Args:
            paths: List of relative (if a base URL was given) or absolute (of no base URL) paths to make request to
            name: A readable name for this request, used in logging.
            query: Query parameters. Values can either be values or callables giving values.
            headers: Headers. Values can either be values or callables giving values.
            response_type: Class to deserialize response JSON into
            next_page: A callable taking an HttpCall and returning the next HttpUrl to make a request to.
            interval: A target iteration time. If given, the extractor will make periodic requests instead of a single
                request.
        """
        return self.endpoint_list(
            name=name,
            method=HttpMethod.GET,
            paths=paths,
            query=query or {},
            headers=headers or {},
            body=None,
            response_type=response_type,
            next_page=next_page,
            interval=interval,
        )

    def __enter__(self) -> "RestExtractor":
        super(RestExtractor, self).__enter__()

        self.authentication = AuthenticationProvider(self.config.source.auth)
        self.base_url = self.config.source.base_url or self._default_base_url

        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> bool:
        return super(RestExtractor, self).__exit__(exc_type, exc_val, exc_tb)

    def prepare_headers(self, endpoint: Endpoint) -> Dict[str, str]:
        """
        Return the set of headers for a given endpoint. On conflicting keys, the priority is (from highest to lowest):

        #. Automatic headers (such as ``Content-Type``)
        #. Auth headers (if configured)
        #. Headers from Config
        #. Headers specifiec in endpoints
        #. Global headers given in RestExtractor constructor

        Args:
            endpoint: endpoint to make headers for

        Returns:
            A dictionary of header keys/values
        """
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
        """
        Run extractor.

        If any of the configured endpoints have the ``interval`` argument set, the extractor will enter a loop until the
        ``cancelation_token`` is set (for example by an interrupt signal).
        """
        if not self.started:
            raise ValueError("You must run the extractor in a context manager")

        runners = []
        for endpoint in self.endpoints:
            runner = EndpointRunner(self, endpoint)
            runner.run()
            runners.append(runner)

        errors = []
        for runner in runners:
            runner.join()
            if runner.error:
                errors.append(runner)

        if errors:
            # Raise exception to finish uncleanly, and report a failed run
            raise RuntimeError(
                ", ".join([f"Error in thread '{runner.threadname}': {str(runner.error)}" for runner in errors])
            )


class EndpointRunner:
    """
    A runner class that takes an Endpoint object, and executes all the necessary HTTP requests for that endpoint.

    Args:
         extractor: The extractor class instantiating the runner.
         endpoint: The endpoint to run against
    """

    _threadnames: Set[str] = set()
    _threadname_counter = 0
    _threadname_lock = threading.RLock()

    def __init__(self, extractor: RestExtractor, endpoint: Endpoint):
        self.extractor = extractor
        self.endpoint = endpoint
        self.logger = getLogger(__name__)

        self.thread: Optional[threading.Thread] = None
        self.threadname: Optional[str] = None

        self.error: Optional[Exception] = None

    def _get_threadname(self) -> str:
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

    def _call(self, url: HttpUrl) -> HttpCall:
        self.logger.info(f"{self.endpoint.method.value} {url}")
        url.add_to_query(self.endpoint.query)

        @retry(
            cancelation_token=self.extractor.cancelation_token,
            exceptions=(HTTPError),
            max_delay=self.extractor.config.source.retries.max_delay,
            backoff=self.extractor.config.source.retries.backoff_factor,
            jitter=(0, self.extractor.config.source.retries.jitter),
            tries=self.extractor.config.source.retries.number,
            delay=self.extractor.config.source.retries.delay,
        )
        def inner_call() -> Response:
            resp = requests.request(
                method=self.endpoint.method.value,
                url=str(url),
                data=_format_body(self.endpoint.body),
                headers=self.extractor.prepare_headers(self.endpoint),
            )
            status = HTTPStatus(resp.status_code)
            self.logger.debug(f"Response {resp.status_code}: {status.name} {status.description} - {resp.reason}")
            resp.raise_for_status()
            return resp

        raw_response = inner_call()

        try:
            data = raw_response.json()

            if isinstance(data, list):
                data = {"items": data}

            response = dacite.from_dict(self.endpoint.response_type, data)

            result = self.endpoint.implementation(response)
            self.extractor.handle_output(result)
        except (JSONDecodeError, DaciteError) as e:
            self.logger.error(f"Error while parsing response: {str(e)}")
            raise e

        return HttpCall(url=url, response=response)

    def _try_get_next_page(self, previous_call: HttpCall) -> Optional[HttpUrl]:
        if self.endpoint.next_page is None:
            return None
        return self.endpoint.next_page(previous_call)

    def _exhaust_endpoint(self) -> None:
        try:
            next_url = HttpUrl(
                urljoin(
                    self.extractor.base_url,
                    self.endpoint.path() if callable(self.endpoint.path) else self.endpoint.path,
                )
            )

            while next_url is not None and not self.extractor.cancelation_token.is_set():
                call = self._call(next_url)
                next_url = self._try_get_next_page(call)

        except Exception as e:
            # Store exception details, so we can fetch them from the Extractor class later
            self.error = e
            raise e

    def run(self) -> None:
        """
        Perform all the requests for a given endpoint. Ie, perform initial request, then follow ``next_page`` until no
        more pages are returned from the callback.

        If the endpoint has an ``interval`` configured, the runner will enter a loop until the extractor's
        cancelation_token is set.

        ``run()`` spawns a new worker thread, and will return immediately.
        """

        def loop() -> None:
            for _ in throttled_loop(
                target_time=self.endpoint.interval, cancelation_token=self.extractor.cancelation_token
            ):
                self._exhaust_endpoint()

        self.threadname = self._get_threadname()

        self.thread = threading.Thread(
            target=loop if self.endpoint.interval is not None else self._exhaust_endpoint,
            name=f"EndpointRunner-{self.threadname}",
        )
        self.thread.start()

    def join(self) -> None:
        """
        Wait for runner to complete.
        """
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
