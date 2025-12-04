# =================================================================
# Authors: Sander Schaminee <sander.schaminee@geocat.net>
#
# Copyright (c) 2025 Sander Schaminee
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import csv
import json
import logging
import re
import tempfile
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from io import TextIOWrapper
from operator import itemgetter
from pathlib import Path
from typing import Any

from pygeoapi import util
from pygeoapi.provider.base import BaseProvider

# Join source file name pattern: table-{uuid}.json
_SOURCE_FILE_PATTERN = re.compile(
    r'^table-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.json$',  # noqa
    re.IGNORECASE
)

# Stores references to join source files for quick lookup
_REF_CACHE: dict[str, dict[str, dict[str, Any]]] = {}

LOGGER = logging.getLogger(__name__)

# Join configuration (set at end)
_CONFIG: 'JoinConfig'


def _delete_source(path: Path, silent: bool = False) -> bool:
    """
    Removes the given file from disk.
    If it does not exist, True is immediately returned.

    :param path: file path to remove
    :param silent: if True, no exception is raised and False is returned

    :returns: bool indicating if file is removed
    """

    if not path or not path.exists():
        LOGGER.debug(f'file {path} already removed')
        return True

    try:
        path.unlink()
    except Exception as e:
        LOGGER.error(f'cannot remove {path}',
                     exc_info=e)
        if not silent:
            raise
        else:
            return False

    return True


def _cleanup_sources():
    """
    Removes stale join source files, if a max age or limit was set.
    This method has no effect if REF_CACHE is empty.
    """
    if not _CONFIG.enabled or (_CONFIG.max_days == 0 and
                               _CONFIG.max_files == 0):
        # pygeoapi has not been configured with OGC API - Joins,
        # or auto-cleanup is not configured
        return

    now = datetime.now(timezone.utc)
    max_age = timedelta(days=_CONFIG.max_days)
    max_files = _CONFIG.max_files
    for collection, sources in _REF_CACHE.items():
        # sort sources by timestamp in ascending order
        # and output as tuple (timestamp, source_id, ref)
        source_items = sorted(
            [(datetime.strptime(info['timeStamp'], util.DATETIME_FORMAT).replace(tzinfo=timezone.utc),  # noqa
              source_id, info['ref'])
             for source_id, info in sources.items()],
            key=itemgetter(0)
        )

        # pass 1: limit by max_days (if configured)
        if max_age.days > 0:
            for timestamp, source_id, ref in source_items:
                if now - timestamp <= max_age:
                    continue
                if _delete_source(ref, True):
                    sources.pop(source_id, None)

        # pass 2: limit by max_files (if configured)
        if 0 < max_files < len(sources):
            for _, source_id, ref in list(reversed(source_items))[:max_files]:
                if _delete_source(ref, True):
                    sources.pop(source_id, None)

    # Check for and remove empty collections or join sources
    for collection in list(_REF_CACHE.keys()):
        # Remove empty/non-existent references
        for source_id in list(_REF_CACHE[collection].keys()):
            source_ref = _REF_CACHE[collection][source_id].get('ref')
            if not isinstance(source_ref, Path) or not source_ref.is_file():
                del _REF_CACHE[collection][source_id]
        # Remove empty collection
        if not _REF_CACHE[collection]:
            del _REF_CACHE[collection]


def _make_source_path(join_id: str) -> Path:
    """
    Makes a join source path for storage.
    Note that the file is not actually created.

    :param join_id: ID of the join data to retrieve

    returns: a `Path` instance for a JSON file
    """

    json_file = _CONFIG.source_dir / f'table-{join_id}.json'
    return json_file


def _find_source_path(collection_name: str, join_id: str) -> Path:
    """
    Returns the join source file path for a specific feature collection
    and join ID. Raises KeyError if the reference wasn't found.
    """
    if not _CONFIG.enabled:
        LOGGER.debug(f'OGC API - Joins is disabled, cannot find join source '
                     f'{join_id} for collection {collection_name}')
        raise Exception('OGC API - Joins is disabled')

    try:
        ref = _REF_CACHE[collection_name][join_id]['ref']
    except KeyError:
        LOGGER.debug(f'join source {join_id} not found for '
                     f'collection {collection_name}', exc_info=True)
        raise
    return ref


def _valid_id(join_id: Any) -> bool:
    """
    Returns True if join_id is a valid UUID string.
    This method can be used for input sanitization purposes.
    """
    try:
        uuid.UUID(join_id)
    except (TypeError, ValueError):
        LOGGER.debug(f'invalid join_id: {join_id}', exc_info=True)
        return False
    return True


def enabled(config: dict) -> bool:
    """
    Returns True if the OGC API - Joins extension is enabled (from config).
    Note that init() does not have to be called to yet.
    This method can be used in OpenAPI generation.

    :param config: pygeoapi configuration dictionary

    :returns: True if OGC API - Joins was configured, False otherwise
    """
    global _CONFIG

    _CONFIG = JoinConfig.from_dict(config)
    if not _CONFIG.enabled:
        # pygeoapi has not been configured with OGC API - Joins
        LOGGER.debug('OGC API - Joins has not been configured')
        return False

    LOGGER.debug('OGC API - Joins has been configured')
    return True


def init(config: dict) -> bool:
    """
    Builds the initial join source reference cache if OGC API - Joins was
    configured and performs a cleanup to remove stale sources.

    Should be called when the API initializes.
    Immediately returns False if OGC API - Joins was not configured.

    :param config: pygeoapi configuration dictionary

    :returns: True if OGC API - Joins was configured, False otherwise
    """
    if not enabled(config):
        return False

    # Read all files from dir that match 'table-{uuid}.json'
    # and build REF_CACHE
    for file in _CONFIG.source_dir.iterdir():
        if file.is_file() and _SOURCE_FILE_PATTERN.match(file.name):
            with open(file, 'r') as f:
                source_dict = json.load(f)
                source_id = source_dict['id']
                collection_name = source_dict['collectionName']
                collection_dict = _REF_CACHE.setdefault(collection_name, {})
                collection_dict[source_id] = {
                    'timeStamp': source_dict['timeStamp'],
                    'ref': file
                }

    # remove stale sources
    _cleanup_sources()

    LOGGER.info('OGC API - Joins initialized successfully')
    return True


def collection_keys(provider: dict,
                    collection_name: str) -> list[dict[str, str]]:
    """
    Retrieves the key field configuration for the given feature provider.

    :param provider: feature provider configuration
    :param collection_name: name of feature collection

    :returns: list of key fields
    """
    id_field = provider['id_field']
    key_fields = deepcopy(provider.get('key_fields', []))
    default_key = None
    id_field_found = False

    for key in key_fields:
        if key.get('default', False):
            if default_key:
                raise ValueError(f'multiple default key fields configured for '
                                 f'feature collection \'{collection_name}\'')
            default_key = key['id']
        if key['id'] == id_field:
            id_field_found = True

    if not id_field_found:
        key_fields.append({
            'id': id_field,
            'default': True if default_key in (None, id_field) else False,
        })

    return key_fields


def process_csv(collection_name: str, collection_provider: BaseProvider,
                form_data: dict) -> dict:
    """
    Processes the CSV form data and stores the result as a JSON file
    in a temporary directory.

    :param collection_name: collection name to apply join source to
    :param collection_provider: feature collection provider
    :param form_data: parameters dict (from request form data)

    :returns: dictionary containing the processed join data
    """

    # Make sure again that we really have a feature provider here
    if collection_provider.type != 'feature':
        raise ValueError(f'join source must be linked to a feature provider '
                         f'(got {collection_provider.type})')

    # Extract required parameters (raises KeyError if missing)
    left_dataset_key = form_data['collectionKey']
    right_dataset_key = form_data['joinKey']
    csv_data = form_data['joinFile']

    if not (left_dataset_key == collection_provider.id_field or
            left_dataset_key in (k['id'] for k in collection_provider.key_fields)):  # noqa
        raise ValueError(f'collectionKey \'{left_dataset_key}\' not found '
                         f'in feature collection \'{collection_name}\'')

    if not isinstance(csv_data, util.FileObject):
        raise ValueError(f'join source data must be a '
                         f'{util.FileObject.__class__.__name__}, '
                         f'got {type(csv_data)}')

    if csv_data.content_type not in ('text/csv', 'application/csv'):
        raise ValueError(f'join source data must be of type "text/csv", '
                         f'got "{csv_data.content_type}"')

    # Make sure the buffer pointer is at the beginning
    csv_data.buffer.seek(0)

    # CSV processing parameters (optional)
    csv_delimiter = form_data.get('csvDelimiter', ',')
    csv_header_row = int(form_data.get('csvHeaderRow', 1))
    csv_data_start_row = int(form_data.get('csvDataStartRow', 2))

    # Basic CSV parameter validation
    if len(csv_delimiter) != 1:
        raise ValueError('csvDelimiter must be a single character')

    if csv_header_row < 1:
        raise ValueError('csvHeaderRow must be at least 1')

    if csv_data_start_row <= csv_header_row:
        raise ValueError(
            f'csvDataStartRow ({csv_data_start_row}) must be greater than '
            f'csvHeaderRow ({csv_header_row})'
        )

    join_data = {}

    LOGGER.debug('Reading CSV data from stream')
    try:
        # Wrap binary stream in TextIOWrapper for reading
        # TODO: support other encodings
        text_stream = TextIOWrapper(csv_data.buffer, encoding='utf-8')
        all_lines = text_stream.readlines()
        num_lines = len(all_lines)

        LOGGER.debug(f'CSV file contains {num_lines} lines')

        if csv_header_row > num_lines:
            raise ValueError(
                f'csvHeaderRow ({csv_header_row}) exceeds '
                f'number of CSV rows ({num_lines})'
            )

        if csv_data_start_row > num_lines:
            raise ValueError(
                f'csvDataStartRow ({csv_data_start_row}) exceeds '
                f'number of CSV rows ({num_lines})'
            )

        # Now read lines again to process CSV:
        # start reading from header row
        reader = csv.DictReader(all_lines[csv_header_row-1:],
                                delimiter=csv_delimiter)

        LOGGER.debug(f'CSV header fields {reader.fieldnames}')

        if right_dataset_key not in reader.fieldnames:
            raise ValueError(
                f'key field "{right_dataset_key}" not found '
                f'in CSV fields: {reader.fieldnames}'
            )

        # Parse fields to include and validate
        user_fields = [f.strip() for f in
                       form_data.get('joinFields', '').split(',')
                       if f.strip()]
        collection_fields = frozenset(collection_provider.get_fields())
        if user_fields:
            # User specified fields to include:
            # include all fields that exist in the CSV, do not conflict
            # with provider fields, and aren't the right dataset key
            join_fields = [f for f in user_fields
                           if f not in collection_fields
                           and f in reader.fieldnames
                           and f != right_dataset_key]
        else:
            # User did not specify fields to include:
            # include all fields that do not conflict with provider fields
            # and aren't the right dataset key
            join_fields = [f for f in reader.fieldnames
                           if f not in collection_fields
                           and f != right_dataset_key]

        # Skip rows between header and data start (if any)
        rows_to_skip = csv_data_start_row - csv_header_row - 1
        for _ in range(rows_to_skip):
            next(reader)  # should not raise as we already validated row nums

        LOGGER.debug(f'collecting data from CSV fields: {join_fields}')

        # Process data rows now
        row_count = 0
        for row_dict in reader:
            # Skip empty rows
            if not any(v.strip() for v in row_dict.values() if v):
                continue

            # Get key value
            # NOTE: we keep this a string!
            key_value = row_dict.get(right_dataset_key, '').strip()

            if not key_value:
                # Don't be smart: user needs to fix this
                raise ValueError('found empty or missing key')

            if key_value in join_data:
                # Don't be smart: user needs to fix this too
                raise ValueError(f'found duplicate key ({key_value})')

            # Add data row to output for key and fields of interest
            join_data[key_value] = [row_dict[f] for f in join_fields]
            row_count += 1

        LOGGER.debug(f'Processed {row_count} CSV records')

    except (UnicodeDecodeError, ValueError) as e:
        LOGGER.error('failed to process CSV', exc_info=True)
        raise ValueError(f'failed to process CSV: {e}')

    num_keys = len(join_data)
    LOGGER.debug(f'CSV lookup contains {num_keys} unique keys')

    created = util.get_current_datetime()
    source_id = str(uuid.uuid4())
    output = {
        "id": source_id,
        "timeStamp": created,
        "collectionName": collection_name,
        "collectionKey": left_dataset_key,
        "joinSource": csv_data.name,
        "joinKey": right_dataset_key,
        "joinFields": join_fields,
        "numberOfRows": num_keys,
        "data": join_data
    }

    # Lazily clean up stale sources
    _cleanup_sources()

    # Store the output as JSON file named 'table-{uuid}.json'
    json_file = _make_source_path(source_id)
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=4)

    # Update REF_CACHE (adding collection if not present)
    _REF_CACHE.setdefault(collection_name, {})[source_id] = {
        "timeStamp": created,
        "ref": json_file
    }

    return output


def list_sources(collection_name: str) -> dict:
    """
    Retrieve all available join sources for a given feature collection.

    :param collection_name: name of feature collection

    :returns: list of dict with source references
    """
    if not _CONFIG.enabled:
        LOGGER.debug(f'OGC API - Joins is disabled, cannot list sources '
                     f'for collection {collection_name}')
        raise Exception('OGC API - Joins is disabled')

    return _REF_CACHE.get(collection_name, {})


@lru_cache(maxsize=8)  # cache up to 8 sources for speed
def read_join_source(collection_name: str,
                     join_id: str) -> dict:
    """
    Tries to read the join source file for a specific collection.
    The resulting JSON dict is cached for reuse.

    :param collection_name: name of feature collection
    :param join_id: ID of the join data to retrieve

    :returns: dict with join source data on success,
              otherwise an empty dict
    """
    if not _valid_id(join_id):
        raise ValueError('invalid join source ID')

    try:
        ref = _find_source_path(collection_name, join_id)
    except KeyError:
        # If the join source was not found, we should answer with a 404
        return {}

    if not isinstance(ref, Path) or not ref.is_file():
        # Unusual scenario: Path is set to empty or non-existing value
        LOGGER.debug(f'empty or non-existing join source: {join_id}')
        _cleanup_sources()
        return {}

    with open(ref, 'r') as f:
        source_dict = json.load(f)

    return source_dict


def perform_join(feature_collection: dict, collection_name: str,
                 join_id: str):
    """
    On-the-fly join of a join source table to a feature collection.
    The join will be in-place, so this method returns nothing.

    :param feature_collection: feature collection to apply join to
    :param collection_name: name of feature collection
    :param join_id: ID of the join data source to retrieve

    :raises: ValueError if the join_id is invalid,
             KeyError if the join source does not exist
    """

    source_dict = read_join_source(collection_name, join_id)

    # First perform join on each GeoJSON Feature
    join_count = 0
    collection_key = source_dict['collectionKey']
    join_data = source_dict['data']
    join_fields = source_dict['joinFields']
    for feature in feature_collection['features']:
        # NOTE: look up key as string!
        key = str(feature['properties'].get(collection_key, ''))
        join_row = join_data.get(key, [])
        for field, value in zip(join_fields, join_row):
            feature['properties'][field] = value
        # Add foreign member to GeoJSON Feature for join status
        if join_row:
            feature['joined'] = True
            join_count += 1
        else:
            feature['joined'] = False

    # Now add join count as foreign member on FeatureCollection
    feature_collection['numberJoined'] = join_count


def remove_source(collection_name: str, join_id: str) -> bool:
    """
    Remove the join source data for a feature collection.

    :param collection_name: name of feature collection
    :param join_id: ID of the join data source to remove

    :returns: True on success, False otherwise
    """

    if not _valid_id(join_id):
        raise ValueError('invalid join source ID')

    collection = _REF_CACHE.get(collection_name, {})
    join_ref = collection.pop(join_id, None)
    if not collection:
        # No more sources for collection: delete key
        del _REF_CACHE[collection_name]
    if not join_ref:
        # If the join was not found, we should answer with a 404
        return False

    return _delete_source(join_ref.get('ref'))


@dataclass
class JoinConfig:
    """
    Configuration object for OGC API - Joins utility.
    """
    source_dir: Path
    max_days: int = 0
    max_files: int = 0
    enabled: bool = False

    @classmethod
    def from_dict(cls, config: dict) -> 'JoinConfig':
        source_dir = Path(tempfile.gettempdir())
        try:
            join_config = config.get('server', {})['joins']
        except KeyError:
            # pygeoapi was configured without OGC API - Joins:
            # enabled will be False
            LOGGER.debug('no pygeoapi server.joins configuration found')
            return cls(source_dir)

        # Check if 'joins' key was set, but without configuration
        if join_config is None:
            LOGGER.debug('pygeoapi server.joins configured with defaults')
            join_config = {}

        conf_source_dir = join_config.get('source_dir')
        if conf_source_dir:
            conf_source_dir = Path(conf_source_dir).resolve()
            # Make sure that the directory exists (recursively)
            # NOTE: if user configured 'temp_dir' with a file path
            #       and the file exists, this will raise an error
            conf_source_dir.mkdir(parents=True, exist_ok=True)
            source_dir = conf_source_dir

        return cls(source_dir,
                   max_days=join_config.get('max_days', 0),
                   max_files=join_config.get('max_files', 0),
                   enabled=True)


# Make sure JOIN_CONFIG is always initialized (default: disabled state)
_CONFIG = JoinConfig(Path(tempfile.gettempdir()))
