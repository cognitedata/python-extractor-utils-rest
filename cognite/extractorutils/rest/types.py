from typing import Iterable, Union

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


Event: TypeAlias = _Event

CdfTypes = Union[
    Event,
    Iterable[Event],
    RawRow,
    Iterable[RawRow],
]
