import copy, datetime, json, time
import dateutil.parser
from decimal import Decimal

from os import environ
import singer
import singer.metrics as metrics

from google.cloud import bigquery

from . import utils

from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema


LOGGER = utils.get_logger(__name__)

# StitchData compatible timestamp meta data
#  https://www.stitchdata.com/docs/data-structure/system-tables-and-columns
# The timestamp of the record extracted from the source
EXTRACT_TIMESTAMP = "_sdc_extracted_at"
# The timestamp of the record submit to the destination
# (kept null at extraction)
BATCH_TIMESTAMP = "_sdc_batched_at"
# Legacy timestamp field
LEGACY_TIMESTAMP = "_etl_tstamp"

BOOKMARK_KEY_NAME = "last_update"

SERVICE_ACCOUNT_INFO_ENV_VAR = "GOOGLE_APPLICATION_CREDENTIALS_STRING"

def get_bigquery_client():
    """Initialize a bigquery client from credentials file JSON,
    if in environment, else credentials file.

    Returns:
        Initialized BigQuery client.
    """
    credentials_json = environ.get(SERVICE_ACCOUNT_INFO_ENV_VAR)
    if credentials_json:
        return bigquery.Client.from_service_account_info(json.loads(credentials_json))
    return bigquery.Client()

def _build_query(keys, filters=[], inclusive_start=True, limit=None):
    columns = ",".join(keys["columns"])
    if "*" not in columns and keys["datetime_key"] not in columns:
        columns = columns + "," + keys["datetime_key"]
    keys["columns"] = columns

    query = "SELECT {columns} FROM {table} WHERE 1=1".format(**keys)

    if filters:
        for f in filters:
            query = query + " AND " + f

    if keys.get("datetime_key") and keys.get("start_date"):
        if inclusive_start:
            query = (query +
                     (" AND '{start_date}' <= " +
                      "{datetime_key}").format(**keys))
        else:
            query = (query +
                     (" AND '{start_date}' < " +
                      "{datetime_key}").format(**keys))

    if keys.get("datetime_key") and keys.get("end_datetime"):
        query = (query +
                 (" AND {datetime_key} < " +
                  "'{end_datetime}'").format(**keys))
    if keys.get("datetime_key"):
        query = (query + " ORDER BY {datetime_key}".format(**keys))

    if limit is not None:
        query = query + " LIMIT %d" % limit

    return query

TYPE_MAP = {
    'STRING': { 'type': 'string'},
    'BOOLEAN':  { 'type': 'boolean' },
    'BOOL':  { 'type': 'boolean' },
    'INTEGER': { 'type': 'integer' },
    'INT64': { 'type': 'integer' },
    'FLOAT': { 'type': 'number', 'format': 'float' },
    'FLOAT64': { 'type': 'number', 'format': 'float' },
    'NUMERIC': { 'type': 'number', 'format': 'float' },
    'TIMESTAMP': { 'type': 'string', 'format': 'date-time' },
    'DATETIME': { 'type': 'string', 'format': 'date-time' },
    'DATE': { 'type': 'string', 'format': 'date' },
    'TIME': { 'type': 'string', 'format': 'time' },
    # TODO: 'BYTES' - I'm not sure how this comes, maybe a list of ints?
}

def convert_schemafield_to_jsonschema(schemafields):
    jsonschema = { 'type': 'object', 'properties': {} }

    for schemafield in schemafields:
        if schemafield.field_type in TYPE_MAP:
            jsonschema['properties'][schemafield.name] = TYPE_MAP[schemafield.field_type].copy()
        elif schemafield.field_type == 'RECORD' or schemafield.field_type == 'STRUCT':
            jsonschema['properties'][schemafield.name] = convert_schemafield_to_jsonschema(schemafield.fields)
        else:
            raise NotImplementedError(f"Field type not supported: {schemafield.field_type}")

        if schemafield.mode == 'NULLABLE':
            type = jsonschema['properties'][schemafield.name]['type']
            jsonschema['properties'][schemafield.name]['type'] = ['null', type]
        elif schemafield.mode == 'REPEATED':
            jsonschema['properties'][schemafield.name] = {
                'type': 'array',
                'items': jsonschema['properties'][schemafield.name]
            }
        jsonschema['properties'][schemafield.name]['description'] = schemafield.description
    return jsonschema

def do_discover(config, output_schema_file=None,
                add_timestamp=True):
    client = get_bigquery_client()
    project = config["project"]
    streams = []

    for dataset in client.list_datasets(project):
        for t in client.list_tables(dataset.dataset_id):
            full_table_id = f"{project}.{dataset.dataset_id}.{t.table_id}"

            # Load the full table details to get the schema
            table = client.get_table(full_table_id)
            replication_key = None

            try:
                schema = Schema.from_dict(convert_schemafield_to_jsonschema(table.schema))
            except:
                LOGGER.exception(f"Skipping table {t.full_table_id}: Error:")
                continue

            # Try and guess required properties by looking for required fields that end in "id"
            # if this doesn't work, users can always specify their own key-properties with catalog
            key_properties = [s.name for s in table.schema if s.mode == 'REQUIRED' and s.name.lower().endswith("id")]
            metadata = {
                'inclusion': 'available',
                'selected': True,
                'table-name': full_table_id,
                'table-created-at': utils.strftime(table.created),
                'table-labels': table.labels,
            }

            if replication_key is not None:
                metadata['replication-key'] = replication_key

            stream_metadata = [{
                'metadata': metadata,
                'breadcrumb': []
            }]

            stream_name = full_table_id.replace(".", "__").replace('-', '_')
            if replication_key is not None:
                streams.append(
                    CatalogEntry(
                        tap_stream_id=stream_name,
                        stream=f"{dataset.dataset_id}_{table.table_id}",
                        schema=schema,
                        key_properties=key_properties,
                        metadata=stream_metadata,
                        replication_key=replication_key,
                        is_view=None,
                        database=None,
                        table=None,
                        row_count=None,
                        stream_alias=None,
                        replication_method='INCREMENTAL',
                    )
                )
            else:
                streams.append(
                    CatalogEntry(
                        tap_stream_id=stream_name,
                        stream=f"{dataset.dataset_id}_{table.table_id}",
                        schema=schema,
                        key_properties=key_properties,
                        metadata=stream_metadata,
                        is_view=None,
                        database=None,
                        table=None,
                        row_count=None,
                        stream_alias=None,
                        replication_method='FULL_TABLE',
                    )
                )

    return Catalog(streams)


def deep_convert_datetimes(value):
    if isinstance(value, list):
        return [deep_convert_datetimes(child) for child in value]
    elif isinstance(value, dict):
        return {k: deep_convert_datetimes(v) for k, v in value.items()}
    elif isinstance(value, datetime.date) or isinstance(value, datetime.datetime):
        return value.isoformat()
    return value


def do_sync(config, state, stream):
    singer.set_currently_syncing(state, stream.tap_stream_id)
    singer.write_state(state)

    client = get_bigquery_client()
    metadata = stream.metadata[0]["metadata"]
    tap_stream_id = stream.tap_stream_id

    inclusive_start = True
    start_date = singer.get_bookmark(state, tap_stream_id,
                                         BOOKMARK_KEY_NAME)
    if start_date:
        if not config.get("start_always_inclusive"):
            inclusive_start = False
    else:
        start_date = config.get("start_date")
    start_date = dateutil.parser.parse(start_date).strftime(
            "%Y-%m-%d %H:%M:%S.%f")

    if config.get("end_datetime"):
        end_datetime = dateutil.parser.parse(
            config.get("end_datetime")).strftime("%Y-%m-%d %H:%M:%S.%f")

    singer.write_schema(tap_stream_id, stream.schema.to_dict(),
                        stream.key_properties)

    keys = {"table": metadata["table-name"],
            "columns": "*",
            "datetime_key": metadata.get("replication-key"),
            "start_date": start_date,
            "end_datetime": end_datetime
            }

    limit = config.get("limit", None)
    query = _build_query(keys, metadata.get("filters", []), inclusive_start,
                         limit=limit)
    query_job = client.query(query)

    properties = stream.schema.properties
    last_update = start_date

    LOGGER.info("Running query:\n    %s" % query)

    extract_tstamp = datetime.datetime.utcnow()
    extract_tstamp = extract_tstamp.replace(tzinfo=datetime.timezone.utc)

    with metrics.record_counter(tap_stream_id) as counter:
        for row in query_job:
            record = {}
            for key in properties.keys():
                prop = properties[key]

                if key in [LEGACY_TIMESTAMP,
                           EXTRACT_TIMESTAMP,
                           BATCH_TIMESTAMP]:
                    continue

                if row[key] is None:
                    if prop.type[0] != "null":
                        raise ValueError(
                            "NULL value not allowed by the schema"
                        )
                    else:
                        record[key] = None
                elif prop.format == "date-time":
                    if type(row[key]) == str:
                        r = dateutil.parser.parse(row[key])
                    elif type(row[key]) == datetime.date:
                        r = datetime.datetime(
                            year=row[key].year,
                            month=row[key].month,
                            day=row[key].day)
                    elif type(row[key]) == datetime.datetime:
                        r = row[key]
                    record[key] = r.isoformat()
                elif prop.type[1] == "string":
                    record[key] = str(row[key])
                elif prop.type[1] == "number":
                    record[key] = Decimal(row[key])
                elif prop.type[1] == "integer":
                    record[key] = int(row[key])
                else:
                    record[key] = row[key]

            if LEGACY_TIMESTAMP in properties.keys():
                record[LEGACY_TIMESTAMP] = int(round(time.time() * 1000))
            if EXTRACT_TIMESTAMP in properties.keys():
                record[EXTRACT_TIMESTAMP] = extract_tstamp.isoformat()

            # NOTE: Needed to handle nested objects/lists with datetime
            record = deep_convert_datetimes(record)
            singer.write_record(tap_stream_id, record)

            last_update = record[keys["datetime_key"]]
            counter.increment()

    state = singer.write_bookmark(state, tap_stream_id, BOOKMARK_KEY_NAME,
                                  last_update)

    singer.write_state(state)
