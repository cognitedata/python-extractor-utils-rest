import os
from dataclasses import dataclass
from typing import Dict, List, Optional

from cognite.client.data_classes import Event

from cognite.extractorutils.rest import RestExtractor


@dataclass
class RawEvent:
    external_id: Optional[str]
    data_set_id: Optional[int]
    start_time: Optional[int]
    end_time: Optional[int]
    type: Optional[str]
    subtype: Optional[str]
    description: Optional[str]
    metadata: Optional[Dict[str, str]]
    asset_ids: Optional[List[Optional[int]]]
    source: Optional[str]
    id: Optional[int]
    last_updated_time: Optional[int]
    created_time: Optional[int]


@dataclass
class EventsList:
    items: List[RawEvent]
    nextCursor: Optional[str]


extractor = RestExtractor(
    name="Event extractor",
    description="Testytesty",
    version="1.0.0",
    base_url="https://api.cognitedata.com/api/v1/projects/jetfiretest/",
    headers={"api-key": os.environ["COGNITE_API_KEY"]},
)


@extractor.get("events", response_type=EventsList)
def get_events(events: EventsList) -> List[Event]:
    for event in events.items:
        yield Event(external_id=f"testy-{event.id}", start_time=event.start_time, end_time=event.end_time)


with extractor:
    extractor.run()
