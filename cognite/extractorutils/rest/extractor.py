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
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from http import HTTPStatus
from queue import Empty, PriorityQueue
from types import TracebackType
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, TypeVar, Union
from urllib.parse import urljoin

import dacite
import requests
from cognite.extractorutils.configtools import StateStoreConfig
from cognite.extractorutils.exceptions import InvalidConfigError
from cognite.extractorutils.retry import retry
from cognite.extractorutils.uploader_extractor import UploaderExtractor, UploaderExtractorConfig
from cognite.extractorutils.uploader_types import CdfTypes
from dacite import DaciteError
from requests import Response
from requests.exceptions import HTTPError, JSONDecodeError

from cognite.extractorutils.rest.authentiaction import AuthConfig, AuthenticationProvider
from cognite.extractorutils.rest.http import (
    Endpoint,
    HttpCallResult,
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
    request_parallelism: int = 10


@dataclass
class RestConfig(UploaderExtractorConfig):
    source: SourceConfig = SourceConfig()
    extractor: ExtractorConfig = ExtractorConfig()


@dataclass
class HttpCall:
    """
    Class representing a single call to an HTTP endpoint at some point in the future.

    Args:
        endpoint: The endpoint this call is querying
        url: The url it will query
        call_when: Some timestamp in seconds since epoch when this should be called. Can be 0 to indicate
            that it should be called as soon as possible.
    """

    endpoint: Endpoint
    url: HttpUrl
    # When this endpoint should next be called
    call_when: float


@dataclass(order=True)
class PrioritizedHttpCall:
    priority: float
    call: HttpCall = field(compare=False)


CustomRestConfig = TypeVar("CustomRestConfig", bound=RestConfig)


class RestExtractor(UploaderExtractor[CustomRestConfig]):
    """
    Class for data extraction from RESTful systems.

    Args:
        name: Name of the extractor, how it's invoked from the command line.
        description: A short 1-2 sentence description of the extractor.
        version: Version number, following semantic versioning.
        default_base_url: Base URL for all calls. Will be ``urljoin``ed with relative paths provided in decorators. If no base
            URL is given, a full URL must be given in each decorator instead. Base URLs can also be given in
            configuration.
        headers: A dictionary of headers to add to every HTTP request.
        config_class: A class (based on the CustomRestConfig class) that defines the configuration schema for the
            extractor
        use_default_state_store: Create a simple instance of the LocalStateStore to provide to the run handle. If false
            a NoStateStore will be created in its place.
        cancelation_token: An event that will be set when the extractor should shut down, an empty one will be created
            if omitted.
        config_file_path: Optional override to configuration file path
        num_parallel_requests: Maximum number of requests to execute in parallel. Must be greater than 0.
    """

    def __init__(
        self,
        *,
        name: str,
        description: str,
        version: Optional[str] = None,
        default_base_url: Optional[str] = None,
        headers: Optional[Dict[str, Union[str, Callable[[], str]]]] = None,
        cancelation_token: threading.Event = threading.Event(),
        config_class: Type[CustomRestConfig] = RestConfig,
        use_default_state_store: bool = True,
        config_file_path: Optional[str] = None,
    ):
        super(RestExtractor, self).__init__(
            name=name,
            description=description,
            version=version,
            cancelation_token=cancelation_token,
            use_default_state_store=use_default_state_store,
            config_class=config_class,
            config_file_path=config_file_path,
        )
        self._default_base_url = default_base_url or ""
        self.headers: Dict[str, Union[str, Callable[[], str]]] = headers or {}
        # The list of pending endpoints while the extractor is starting
        # This is set to None when the extractor is running,
        # and new endpoints should be added to the call_queue instead.
        self._initial_endpoints: Optional[List[Endpoint]] = []
        self._call_queue: PriorityQueue[PrioritizedHttpCall] = PriorityQueue()
        self.n_executing = 0
        self._min_check_interval = 1

    def _add_endpoint(self, endpoint: Endpoint) -> None:
        """
        Add an endpoint to the list of active endpoints. Use this to create new endpoint
        definitions from inside of other endpoints.
        """
        if self._initial_endpoints is not None:
            self._initial_endpoints.append(endpoint)
        else:
            call = HttpCall(endpoint=endpoint, url=_get_initial_url(self.base_url, endpoint), call_when=0)
            self._call_queue.put(PrioritizedHttpCall(priority=call.call_when, call=call))

    def add_endpoint(
        self,
        *,
        implementation: Callable[[ResponseType], CdfTypes],
        name: Optional[str] = None,
        method: HttpMethod,
        path: Union[str, Callable[[], str]],
        query: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Union[str, Callable[[], str]]]] = None,
        body: Optional[RequestBodyTemplate] = None,
        response_type: Type[ResponseType],
        next_page: Optional[Callable[[HttpCallResult], Optional[HttpUrl]]] = None,
        interval: Optional[int] = None,
    ) -> None:
        """
        Add an endpoint to the list of active endpoints. Use this to create new endpoint
        definitions from inside of other endpoints.
        """
        self._add_endpoint(
            Endpoint(
                name=name,
                implementation=implementation,
                method=method,
                path=path,
                query=query or {},
                headers=headers or {},
                body=body,
                response_type=response_type,
                next_page=next_page,
                interval=interval,
            )
        )

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
        next_page: Optional[Callable[[HttpCallResult], Optional[HttpUrl]]],
        interval: Optional[int],
    ) -> Callable[[Callable[[ResponseType], CdfTypes]], Callable[[ResponseType], CdfTypes]]:
        """
        A generic endpoint decorator. Not meant to be used directly, use ``get`` or ``post`` instead.
        """

        def decorator(func: Callable[[ResponseType], CdfTypes]) -> Callable[[ResponseType], CdfTypes]:
            self._add_endpoint(
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
        next_page: Optional[Callable[[HttpCallResult], Optional[HttpUrl]]],
        interval: Optional[int],
    ) -> Callable[[Callable[[ResponseType], CdfTypes]], Callable[[ResponseType], CdfTypes]]:
        """
        A generic endpoint decorator. Not meant to be used directly, use ``get`` or ``post`` instead.
        """

        def decorator(func: Callable[[ResponseType], CdfTypes]) -> Callable[[ResponseType], CdfTypes]:
            for path in paths:
                self._add_endpoint(
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
        next_page: Optional[Callable[[HttpCallResult], Optional[HttpUrl]]] = None,
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
            next_page: A callable taking an HttpCallResult and returning the next HttpUrl to make a request to.
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
        next_page: Optional[Callable[[HttpCallResult], Optional[HttpUrl]]] = None,
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
            next_page: A callable taking an HttpCallResult and returning the next HttpUrl to make a request to.
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
        next_page: Optional[Callable[[HttpCallResult], Optional[HttpUrl]]] = None,
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
            next_page: A callable taking an HttpCallResult and returning the next HttpUrl to make a request to.
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

        endpoints = self._initial_endpoints
        self._initial_endpoints = None
        if endpoints is not None:
            for endpoint in endpoints:
                self._add_endpoint(endpoint)

        if self.config.extractor.request_parallelism <= 0:
            raise InvalidConfigError("request-parallelism must be a number greater than 0")

        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> bool:
        self._initial_endpoints = []
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

    def _get_next_call(self) -> Optional[HttpCall]:
        waiting: Optional[PrioritizedHttpCall] = None
        while not self.cancelation_token.is_set():
            # If n_executing is set to 0, and the queue is empty here, then we are done
            # This is race condition prone in theory, but it should work. n_executing is
            # only decremented after giving the call a chance to add to the call queue, so
            # either the call is unfinished, and n_executing is > 0, or the queue length is
            # accurate.
            if self.n_executing == 0 and self._call_queue.empty():
                return waiting.call if waiting is not None else None
            to_wait = self._min_check_interval if waiting is None else waiting.call.call_when - time.time()
            if waiting is not None and to_wait <= 0:
                return waiting.call
            try:
                next: PrioritizedHttpCall = self._call_queue.get(block=True, timeout=to_wait)
            except Empty:
                continue

            if waiting is not None and next.call.call_when < waiting.call.call_when:
                self._call_queue.put(waiting)
                waiting = next
            elif waiting is None:
                waiting = next
            else:
                self.cancelation_token.wait(min(self._min_check_interval, waiting.call.call_when - time.time()))

        return None

    def run(self) -> None:
        """
        Run extractor.

        If any of the configured endpoints have the ``interval`` argument set, the extractor will enter a loop until the
        ``cancelation_token`` is set (for example by an interrupt signal).
        """
        if not self.started:
            raise ValueError("You must run the extractor in a context manager")

        lock = threading.Lock()

        errors: List[Tuple[Exception, HttpCall]] = []

        def executor_call(endpoint: HttpCall) -> None:
            try:
                resp = self._call(endpoint)
                self._handle_call_response(endpoint.endpoint, resp)
            except Exception as e:
                errors.append((e, endpoint))
            with lock:
                self.n_executing -= 1

        with ThreadPoolExecutor(max_workers=self.config.extractor.request_parallelism) as executor:

            def producer_loop() -> None:
                try:
                    while not self.cancelation_token.is_set():
                        next = self._get_next_call()
                        if next is None:
                            break
                        with lock:
                            self.n_executing += 1
                        executor.submit(executor_call, next)
                except Exception as e:
                    self.logger.error(f"Failure in call producer thread: {str(e)}")

            producer_thread = threading.Thread(name="Producer", target=producer_loop)
            producer_thread.start()
            producer_thread.join()

        if errors:
            # Raise exception to finish uncleanly, and report a failed run
            raise RuntimeError(
                ", ".join(
                    [f"Error in endpoint '{endpoint.endpoint.name}': {str(error)}" for (error, endpoint) in errors]
                )
            )

    def _call(self, endpoint: HttpCall) -> HttpCallResult:
        self.logger.info(f"{endpoint.endpoint.method.value} {endpoint.url}")
        endpoint.url.add_to_query(endpoint.endpoint.query)

        @retry(
            cancelation_token=self.cancelation_token,
            exceptions=(HTTPError),
            max_delay=self.config.source.retries.max_delay,
            backoff=self.config.source.retries.backoff_factor,
            jitter=(0, self.config.source.retries.jitter),
            tries=self.config.source.retries.number,
            delay=self.config.source.retries.delay,
        )
        def inner_call() -> Response:
            resp = requests.request(
                method=endpoint.endpoint.method.value,
                url=str(endpoint.url),
                data=_format_body(endpoint.endpoint.body),
                headers=self.prepare_headers(endpoint.endpoint),
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

            response = dacite.from_dict(endpoint.endpoint.response_type, data)

            result = endpoint.endpoint.implementation(response)
            self.handle_output(result)
        except (JSONDecodeError, DaciteError) as e:
            self.logger.error(f"Error while parsing response: {str(e)}")
            raise e

        return HttpCallResult(url=endpoint.url, response=response)

    def _handle_call_response(self, endpoint: Endpoint, call: HttpCallResult) -> None:
        if endpoint.next_page is None:
            return
        next_url = endpoint.next_page(call)
        if next_url is not None:
            next = HttpCall(
                endpoint=endpoint,
                url=next_url,
                call_when=0 if endpoint.interval is None else time.time() + endpoint.interval,
            )
            self._call_queue.put(PrioritizedHttpCall(priority=next.call_when, call=next))


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


def _get_initial_url(base_url: str, endpoint: Endpoint) -> HttpUrl:
    return HttpUrl(
        urljoin(
            base_url,
            endpoint.path() if callable(endpoint.path) else endpoint.path,
        )
    )
