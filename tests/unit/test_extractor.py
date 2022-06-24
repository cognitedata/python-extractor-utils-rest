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

import unittest

from cognite.extractorutils.rest.extractor import _format_body, _get_or_call, _incompatible_lists_to_dict


class Iterator:
    def __init__(self) -> None:
        self.num = 0

    def __call__(self) -> int:
        self.num += 1
        return self.num


class TestFormatBody(unittest.TestCase):
    def test_empty_body(self) -> None:
        self.assertIsNone(_format_body(None))

    def test_dict_values(self) -> None:
        self.assertEqual(
            _format_body({"key1": "val", "key2": 42, "key3": None}), '{"key1": "val", "key2": 42, "key3": null}'
        )

    def test_recursive_dict_values(self) -> None:
        self.assertEqual(
            _format_body({"key1": "val", "key2": {"subkey": "subval"}, "key3": ["item1", "item2", "item3"]}),
            '{"key1": "val", "key2": {"subkey": "subval"}, "key3": ["item1", "item2", "item3"]}',
        )

    def test_callable(self) -> None:
        self.assertEqual(_format_body({"key1": lambda: "val"}), '{"key1": "val"}')

    def test_recursive_callable(self) -> None:
        self.assertEqual(_format_body({"key1": lambda: {"subkey": lambda: "subval"}}), '{"key1": {"subkey": "subval"}}')

    def test_callable_in_list(self) -> None:
        self.assertEqual(_format_body({"key": lambda: ["item1", lambda: "item2"]}), '{"key": ["item1", "item2"]}')

    def test_empty_object(self) -> None:
        self.assertEqual(_format_body({}), "{}")
        self.assertEqual(_format_body(lambda: {}), "{}")

    def test_callable_class(self) -> None:
        it = Iterator()
        self.assertEqual(_format_body({"key": lambda: [it, it, it, it]}), '{"key": [1, 2, 3, 4]}')
        self.assertEqual(_format_body({"key": lambda: [it, it, it, it]}), '{"key": [5, 6, 7, 8]}')


class TestGetOrCall(unittest.TestCase):
    def test_none(self) -> None:
        self.assertIsNone(_get_or_call(None))

    def test_value(self) -> None:
        self.assertEqual(_get_or_call(42), 42)
        self.assertEqual(_get_or_call("hey"), "hey")

    def test_callable(self) -> None:
        self.assertEqual(_get_or_call(lambda: 42), 42)
        self.assertEqual(_get_or_call(lambda: 1 + 2 + 3), 6)
        self.assertEqual(_get_or_call(lambda: "hey"), "hey")


class TestIncompatibleListsToDict(unittest.TestCase):
    def test_plain_dict(self) -> None:
        self.assertEqual(
            _incompatible_lists_to_dict({"key1": "val1", "key2": "val2", "key3": "val3"}),
            {"key1": "val1", "key2": "val2", "key3": "val3"},
        )

    def test_nested_dict(self) -> None:
        self.assertEqual(
            _incompatible_lists_to_dict({"key": {"inner_key": "inner_val"}}), {"key": {"inner_key": "inner_val"}}
        )

    def test_simple_list(self) -> None:
        self.assertEqual(
            _incompatible_lists_to_dict([{"key1": "val1"}, {"key2": "val2"}, {"key3": "val3"}]),
            {"items": [{"key1": "val1"}, {"key2": "val2"}, {"key3": "val3"}]},
        )

    def test_nested_list(self) -> None:
        self.assertEqual(
            _incompatible_lists_to_dict([[{"key1": "val1"}, {"key2": "val2"}], [{"key3": "val3"}]]),
            {"items": [{"items": [{"key1": "val1"}, {"key2": "val2"}]}, {"items": [{"key3": "val3"}]}]},
        )

    def test_nested_list_within_dict(self) -> None:
        self.assertEqual(
            _incompatible_lists_to_dict({"key": [[{"key1": "val1"}, {"key2": "val2"}]]}),
            {"key": [{"items": [{"key1": "val1"}, {"key2": "val2"}]}]},
        )


if __name__ == "__main__":
    unittest.main()
