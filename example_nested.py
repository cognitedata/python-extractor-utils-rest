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

from dataclasses import dataclass
from typing import Generator, List, Optional

from cognite.client.data_classes import Row
from cognite.extractorutils.uploader_types import RawRow

from cognite.extractorutils.rest import RestExtractor
from cognite.extractorutils.rest.http import HttpMethod

# Paginate timeseries belonging to a specific list of assets

@dataclass
class RawAsset:
    externalId: Optional[str]
    name: str
    id: Optional[int]


@dataclass
class RawTimeSeries:
    externalId: Optional[str]
    name: Optional[str]
    id: Optional[int]

@dataclass
class AssetsList:
    items: List[RawAsset]
    nextCursor: Optional[str]

@dataclass
class TimeSeriesList:
    items: List[RawTimeSeries]
    nextCursor: Optional[str]


extractor = RestExtractor(
    name="Time series by asset name extractor",
    description="Testytesty",
    version="1.0.0",
)

def get_timeseries_alt(tss: TimeSeriesList) -> Generator[RawRow, None, None]:
    for ts in tss.items:
        yield RawRow(
            db_name="ts_byasset",
            table_name="tss",
            row=Row(
                key=ts.id,
                columns={
                    "name": ts.name,
                    "id": ts.id,
                    "externalId": ts.externalId
                }
            )
        )

@extractor.post("assets/list", body={"filter": {"name": "PubSubGroupType"}, "limit": 1000}, response_type=AssetsList)
def get_assets_alt(assets: AssetsList) -> Generator[RawRow, None, None]:
    ids = []
    for asset in assets.items:
        ids.append(asset.id)
        yield RawRow(
            db_name="ts_byasset",
            table_name="assets",
            row=Row(
                key=asset.id,
                columns={
                    "name": asset.name,
                    "id": asset.id,
                    "externalId": asset.externalId
                }
            )
        )
    
    if len(ids) > 0:
        extractor.add_endpoint(
            method=HttpMethod.POST,
            implementation=get_timeseries_alt,
            path="timeseries/list",
            body={"filter": {"assetSubtreeIds": [{"id": id} for id in ids]}},
            response_type=TimeSeriesList
        )


with extractor:
    extractor.run()
