# Cognite `extractor-utils` REST extension

The REST extension for [Cognite `extractor-utils`](https://github.com/cognitedata/python-extractor-utils) provides a way
to easily write your own extractors for RESTful source systems.

The library is currently under development, and should not be used in production environments yet.


## Overview

The REST extension for extractor utils templetizes how the extractor will make HTTP requests to the source,
automatically serializes the response into user-defined DTO classes, and handles uploading of data to CDF.

The only part of the extractor necessary to for a user to implement are

 * Describing how HTTP requests should be constructed using pre-built function decorators
 * Describing the response schema using Python `dataclass`es
 * Implementing a mapping from the source data model to the CDF data model

For example, consider [CDF's Events API](https://docs.cognite.com/api/v1/#operation/listEvents) as a source. We could
describe the response schema as an `EventsList` dataclass:

``` python
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
```

We can then write a handler that takes in one of these `EventList`s, and returns CDF Events, as represented by instances
of the `Event` class from the `cognite.extractorutils.rest.typing` module.


``` python
extractor = RestExtractor(
    name="Event extractor",
    description="Extractor from CDF events to CDF events",
    version="1.0.0",
    base_url=f"https://api.cognitedata.com/api/v1/projects/{os.environ['COGNITE_PROJECT']}/",
    headers={"api-key": os.environ["COGNITE_API_KEY"]},
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

with extractor:
    extractor.run()

```

A full example is provided in the [`example.py`](./example.py) file.

### Lists at the root
Using Python dataclasses we're not able to express JSON structures where the root element 
is a list. To get around that responses of this nature will be automatically converted to something which can be modeled with Python dataclasses. 

A JSON structure containing a list as it's root element will be converted to an object containing a single key, "items", which has the original JSON list as it's value, as in the example below.

```
[{"object_id": 1}, {"object_id": 2}, {"object_id": 3}]
```

will be converted to 

```
{
    "items": [{"object_id": 1}, {"object_id": 2}, {"object_id": 3}]
}
```

## Contributing

We use [poetry](https://python-poetry.org) to manage dependencies and to administrate virtual environments. To develop
`extractor-utils`, follow the following steps to set up your local environment:

 1. Install poetry: (add `--user` if desirable)
    ```
    $ pip install poetry
    ```
 2. Clone repository:
    ```
    $ git clone git@github.com:cognitedata/python-extractor-utils-rest.git
    ```
 3. Move into the newly created local repository:
    ```
    $ cd python-extractor-utils-rest
    ```
 4. Create virtual environment and install dependencies:
    ```
    $ poetry install
    ```

All code must pass typing and style checks to be merged. It is recommended to install pre-commit hooks to ensure that
these checks pass before commiting code:

```
$ poetry run pre-commit install
```

This project adheres to the [Contributor Covenant v2.0](https://www.contributor-covenant.org/version/2/0/code_of_conduct/)
as a code of conduct.

