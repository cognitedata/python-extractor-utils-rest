from dataclasses import dataclass
from typing import Any, Generator, List, Optional

import requests
from cognite.client.data_classes import Row
from cognite.extractorutils.uploader_types import RawRow
from requests_mock import Mocker

from cognite.extractorutils.rest import RestExtractor
from cognite.extractorutils.rest.http import HttpCallResult, HttpMethod, HttpUrl, JsonBody


@dataclass
class MyResponseType:
    it: int
    cursor: Optional[str]


@dataclass
class ResponseTypeList:
    items: List[MyResponseType]


def get_extractor(idx: int) -> RestExtractor:
    extractor = RestExtractor(
        name=f"Test extractor {idx}",
        description="test",
        version="1.0.0",
        default_base_url="http://mybaseurl.foo/",
        config_file_path="tests/unit/test_config.yml",
    )
    extractor.cancelation_token.clear()
    extractor._min_check_interval = 0.1
    return extractor


class RawMocker:
    def __init__(self, mock: Mocker):
        self.calls = 0

        def raw_req_callback(request: requests.Request, context: Any) -> dict:
            self.calls += 1
            return {}

        self.mock = mock
        self.mock.post(
            url="https://api.cognitedata.com/api/v1/projects/test/raw/dbs/mydb/tables/mytable/rows",
            json=raw_req_callback,
        )


class TestRequests:
    def test_simple_get(self, requests_mock: Mocker) -> None:
        requests_mock.get(url="http://mybaseurl.foo/path", json={"it": 1, "cursor": None})
        raw = RawMocker(requests_mock)
        extractor = get_extractor(0)

        @extractor.get("path", response_type=MyResponseType)
        def get_test_resp(data: MyResponseType) -> Generator[RawRow, None, None]:
            yield RawRow("mydb", "mytable", Row(key=data.it, columns={"test": "test"}))

        with extractor:
            extractor.run()

        assert raw.calls == 1

    def test_get_list(self, requests_mock: Mocker) -> None:
        requests_mock.get(url="http://mybaseurl.foo/path", json=[{"it": 1, "cursor": None}, {"it": 2, "cursor": None}])
        raw = RawMocker(requests_mock)
        extractor = get_extractor(1)

        @extractor.get("path", response_type=ResponseTypeList)
        def get_test_resp(data: ResponseTypeList) -> Generator[RawRow, None, None]:
            for item in data.items:
                yield RawRow("mydb", "mytable", Row(key=item.it, columns={"test": "test"}))

        with extractor:
            extractor.run()

        assert raw.calls == 1

    def test_follow_cursor(self, requests_mock: Mocker) -> None:
        def mock_response(request: requests.Request, context: Any) -> dict:
            print(request.url)
            if request.url.endswith("?cursor=some"):
                return {"it": 2, "cursor": "some2"}
            elif request.url.endswith("?cursor=some2"):
                return {"it": 3, "cursor": None}
            return {"it": 1, "cursor": "some"}

        requests_mock.get(url="http://mybaseurl.foo/path", json=mock_response)
        raw = RawMocker(requests_mock)
        extractor = get_extractor(2)
        num_page = 0
        resps = []

        def test_next_page(call: HttpCallResult) -> Optional[HttpUrl]:
            nonlocal num_page
            num_page += 1
            if call.response.cursor is not None:
                call.url.query["cursor"] = call.response.cursor
                return call.url
            return None

        @extractor.get("path", response_type=MyResponseType, next_page=test_next_page)
        def get_test_resp(data: MyResponseType) -> Generator[RawRow, None, None]:
            nonlocal resps
            resps.append(data)
            yield RawRow("mydb", "mytable", Row(key=data.it, columns={"test": "test"}))

        with extractor:
            extractor.run()

        assert raw.calls == 1
        assert num_page == 3
        assert len(resps) == 3
        assert resps[0].it == 1
        assert resps[1].it == 2

    def test_nested(self, requests_mock: Mocker) -> None:
        call1 = requests_mock.get(
            url="http://mybaseurl.foo/path", json=[{"it": 1, "cursor": None}, {"it": 2, "cursor": None}]
        )
        call2 = requests_mock.get(url="http://mybaseurl.foo/path/two", json=[{"it": 2, "cursor": None}])
        RawMocker(requests_mock)
        extractor = get_extractor(3)

        def get_test_resp_alt(data: ResponseTypeList) -> Generator[RawRow, None, None]:
            for item in data.items:
                yield RawRow("mydb", "mytable", Row(key=item.it, columns={"test": "test"}))

        @extractor.get("path", response_type=ResponseTypeList)
        def get_test_resp(data: ResponseTypeList) -> Generator[RawRow, None, None]:
            for item in data.items:
                extractor.add_endpoint(
                    method=HttpMethod.GET,
                    implementation=get_test_resp_alt,
                    path="path/two",
                    response_type=ResponseTypeList,
                )
                yield RawRow("mydb", "mytable", Row(key=item.it, columns={"test": "test"}))

        with extractor:
            extractor.run()

        assert call1.call_count == 1
        assert call2.call_count == 2

    def test_arbitrary_response(self, requests_mock: Mocker) -> None:
        requests_mock.get(url="http://mybaseurl.foo/path", json=[{"it": 1, "cursor": None}, {"it": 2, "cursor": None}])
        RawMocker(requests_mock)
        extractor = get_extractor(5)

        @extractor.get("path", response_type=JsonBody)
        def get_test_resp(data: JsonBody) -> Generator[RawRow, None, None]:
            for item in data:
                yield RawRow("mydb", "mytable", Row(key=item["it"], columns={"test": "test"}))

        with extractor:
            extractor.run()

    def test_nested_alt(self, requests_mock: Mocker) -> None:
        call1 = requests_mock.get(
            url="http://mybaseurl.foo/path", json=[{"it": 1, "cursor": None}, {"it": 2, "cursor": None}]
        )
        call2 = requests_mock.get(url="http://mybaseurl.foo/path/two", json=[{"it": 3, "cursor": None}])
        RawMocker(requests_mock)
        extractor = get_extractor(4)

        @extractor.get("path", response_type=ResponseTypeList)
        def get_test_resp(data: ResponseTypeList) -> Generator[RawRow, None, None]:
            for item in data.items:

                @extractor.get("path/two", response_type=ResponseTypeList)
                def get_test_resp_alt(data: ResponseTypeList) -> Generator[RawRow, None, None]:
                    for item in data.items:
                        yield RawRow("mydb", "mytable", Row(key=item.it, columns={"test": "test"}))

                yield RawRow("mydb", "mytable", Row(key=item.it, columns={"test": "test"}))

        with extractor:
            extractor.run()

        assert call1.call_count == 1
        assert call2.call_count == 2

    def test_raw_response(self, requests_mock: Mocker) -> None:
        requests_mock.get(url="http://mybaseurl.foo/path", json=[{"it": 1, "cursor": None}, {"it": 2, "cursor": None}])
        RawMocker(requests_mock)
        extractor = get_extractor(6)

        @extractor.get("path", response_type=requests.Response)
        def get_test_resp(data: requests.Response) -> Generator[RawRow, None, None]:
            for item in data.json():
                yield RawRow("mydb", "mytable", Row(key=item["it"], columns={"test": "test"}))

        with extractor:
            extractor.run()
