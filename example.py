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

import os
from dataclasses import dataclass
from typing import Dict, Generator, List, Optional

from cognite.client.data_classes import Row

from cognite.extractorutils.rest import RestExtractor
from cognite.extractorutils.rest.types import Event, RawRow


@dataclass
class RawEvent:
    externalId: Optional[str]
    dataSetId: Optional[int]
    startTime: Optional[int]
    endTime: Optional[int]
    type: Optional[str]
    subtype: Optional[str]
    description: Optional[str]
    metadata: Optional[Dict[str, str]]
    assetIds: Optional[List[Optional[int]]]
    source: Optional[str]
    id: Optional[int]
    lastUpdatedTime: Optional[int]
    createdTime: Optional[int]


@dataclass
class EventsList:
    items: List[RawEvent]
    nextCursor: Optional[str]


extractor = RestExtractor(
    name="Event extractor",
    description="Testytesty",
    version="1.0.0",
    base_url="https://api.cognitedata.com/api/v1/projects/jetfiretest/",
    headers={"api-key": os.environ["SOURCE_API_KEY"]},
)


@extractor.get("events", response_type=EventsList)
def get_events(events: EventsList) -> Generator[Event, None, None]:
    for event in events.items:
        yield Event(
            external_id=f"testy-{event.id}",
            description=event.description,
            start_time=event.startTime,
            end_time=event.endTime,
            type=event.type,
            subtype=event.subtype,
            metadata=event.metadata,
            source=event.source,
        )


@extractor.post("events/list", body={"filter": {}, "limit": 1000}, response_type=EventsList)
def get_events_post(events: EventsList) -> Generator[Event, None, None]:
    for event in events.items:
        yield Event(
            external_id=f"testy2-{event.id}",
            description=event.description,
            start_time=event.startTime,
            end_time=event.endTime,
            type=event.type,
            subtype=event.subtype,
            metadata=event.metadata,
            source=event.source,
        )


@extractor.get("events", response_type=EventsList)
def get_events_as_raw(events: EventsList) -> Generator[RawRow, None, None]:
    for event in events.items:
        yield RawRow(
            "db1",
            "table1",
            Row(
                key=event.id,
                columns={
                    "description": event.description,
                    "start_time": event.startTime,
                    "end_time": event.endTime,
                    "type": event.type,
                    "subtype": event.subtype,
                    "metadata": event.metadata,
                    "source": event.source,
                },
            ),
        )


with extractor:
    extractor.run()
