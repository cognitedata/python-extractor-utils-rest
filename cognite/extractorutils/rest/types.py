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
from datetime import datetime
from typing import Iterable, List, Optional, Tuple, Union

from cognite.client.data_classes import Event as _Event
from cognite.client.data_classes import Row as _Row

try:
    from typing import TypeAlias  # type: ignore
except ImportError:
    # Backport for python < 3.10
    from typing_extensions import TypeAlias


class RawRow:
    def __init__(self, db_name: str, table_name: str, row: Union[_Row, Iterable[_Row]]):
        self.db_name = db_name
        self.table_name = table_name
        if isinstance(row, Iterable):
            self.rows = row
        else:
            self.rows = [row]


TimeStamp = Union[int, datetime]


class InsertDatapoints:
    def __init__(
        self,
        *,
        id: Optional[int] = None,
        external_id: Optional[str] = None,
        datapoints: Union[List[Tuple[TimeStamp, float]], Tuple[TimeStamp, str]],
    ):
        self.id = id
        self.external_id = external_id
        self.datapoints = datapoints


Event: TypeAlias = _Event

CdfTypes = Union[Event, Iterable[Event], RawRow, Iterable[RawRow], InsertDatapoints, Iterable[InsertDatapoints]]
