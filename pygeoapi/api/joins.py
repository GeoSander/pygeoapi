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

import io
import csv
import json
import os
import threading
from copy import deepcopy
import logging
from datetime import datetime
from pathlib import Path
from typing import Tuple, Any, Optional, BinaryIO, re
import urllib

from pygeoapi import l10n
from pygeoapi.plugin import load_plugin, PLUGINS
from pygeoapi.provider.base import (
    ProviderGenericError, ProviderTypeError, SchemaType)
from pygeoapi.util import (
    get_provider_by_type, to_json, filter_providers_by_type,
    filter_dict_by_key_value, transform_bbox
)

from . import (
    APIRequest, API, validate_datetime, SYSTEM_LOCALE,
    FORMAT_TYPES, F_JSON, F_JSONLD, F_HTML, HTTPStatus, evaluate_limit
)

LOGGER = logging.getLogger(__name__)

API_NAME = 'joins'
CONFORMANCE_CLASSES = [
    # Endpoints
    'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/core',
    'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/data-joining',
    'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/join-delete',
    'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/file-joining',
    # Input
    'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/input-file-upload',
    'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/input-http-ref',
    'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/input-csv',
    'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/input-geojson',
    # Output
    'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/output-geojson',
    'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/output-geojson-direct',
    # Encodings
    'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/html',
    'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/json',
    'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/geojson',
    # OpenAPI
    'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/oas30',
]


DEFAULT_CRS = 'http://www.opengis.net/def/crs/EPSG/0/4326'


def _get_key_fields(provider: dict,
                    collection_name: str) -> list[dict[str, str]]:
    """ Retrieves the key field configuration
    for the given feature provider. """
    id_field = provider['id_field']
    key_fields = provider.get('key_fields', [])
    default_key = None
    id_field_found = False

    for key in key_fields:
        if key.get('default', False):
            if default_key:
                raise ValueError(f"Multiple default join keys configured "
                                 f"for feature collection '{collection_name}'")
            default_key = key['id']
        if key['id'] == id_field:
            id_field_found = True

    if not id_field_found:
        key_fields.append({
            'id': id_field,
            'default': True if default_key in (None, id_field) else False,
        })

    return key_fields


def get_oas_30(cfg: dict, locale: str) -> tuple[list[dict[str, str]], dict[str, dict]]:  # noqa
    """
    Get OpenAPI fragments

    :param cfg: `dict` of configuration
    :param locale: `str` of locale

    :returns: `tuple` of `list` of tag objects, and `dict` of path objects
    """

    from pygeoapi.openapi import OPENAPI_YAML, get_visible_collections, get_oas_30_parameters

    LOGGER.debug('setting up joins endpoints')

    paths = {}
    collections = filter_dict_by_key_value(cfg['resources'],
                                           'type', 'collection')

    parameters = get_oas_30_parameters(cfg, locale)

    # Joins endpoints (list and create)
    paths[f'/{API_NAME}'] = {
        'get': {
            'summary': f'Get all available joins',
            'description': f'Lists all available joins on the server',
            'tags': [API_NAME],
            'operationId': 'getJoins',
            'parameters': [
                {'$ref': '#/components/parameters/f'},
                # {'$ref': '#/components/parameters/lang'},
            ],
            'responses': {
                '200': {
                    'description': 'Response',
                    'content': {
                        'application/json': {}
                    }
                }
            }
        },
        'post': {
            'summary': 'Create a new join',
            'description': 'Creates a new join based on the provided parameters',
            'tags': [API_NAME],
            'operationId': 'createJoin',
            'parameters': [
                {'$ref': '#/components/parameters/f'},
                # {'$ref': '#/components/parameters/lang'},
            ],
            # 'requestBody': {
            #     'required': True,
            #     'content': {
            #         'application/json': {
            #             'schema': {
            #                 'type': 'object',
            #                 'properties': {
            #                     'leftCollection': {'type': 'string'},
            #                     'rightCollection': {'type': 'string'},
            #                     'joinType': {'type': 'string', 'enum': ['inner', 'outer', 'left', 'right']},
            #                     'on': {'type': 'array', 'items': {'type': 'string'}}
            #                 }
            #             }
            #         }
            #     }
            # },
            'responses': {
                '201': {
                    'description': 'Response',
                    'content': {
                        'application/json': {}
                    }
                }
            }
        }
    }

    # Joins endpoint (join metadata and delete)
    paths[f'/{API_NAME}/{{joinId}}'] = {
        'get': {
            'summary': f'Get metadata for a join with the given id',
            'description': f'Returns the metadata for an established join',
            'tags': [API_NAME],
            'operationId': 'getJoinMetadata',
            'parameters': [
                {'$ref': '#/components/parameters/f'},
                # {'$ref': '#/components/parameters/lang'},
            ],
            'responses': {
                '200': {
                    'description': 'Response',
                    'content': {
                        'application/json': {}
                    }
                }
            }
        },
        'delete': {
            'summary': f'Delete a join with the given id',
            'description': f'Deletes a join with the given id',
            'tags': [API_NAME],
            'operationId': 'deleteJoin',
            'parameters': [
                {'$ref': '#/components/parameters/f'},
                # {'$ref': '#/components/parameters/lang'},
            ],
            'responses': {
                '200': {
                    'description': 'Response',
                    'content': {
                        'application/json': {}
                    }
                }
            }
        }
    }

    # Join results endpoint
    paths[f'/{API_NAME}/{{joinId}}/results'] = {
        'get': {
            'summary': f'Get results for a join with the given id',
            'description': f'Returns the output data of an established join',
            'tags': [API_NAME],
            'operationId': 'getJoinResults',
            'parameters': [
                {'$ref': '#/components/parameters/f'},
                # {'$ref': '#/components/parameters/lang'},
            ],
            'responses': {
                '200': {
                    'description': 'Response',
                    'content': {
                        'application/json': {}
                    }
                }
            }
        }
    }

    for k, v in get_visible_collections(cfg).items():
        feature_provider = filter_providers_by_type(collections[k]['providers'], 'feature')
        if not feature_provider:
            continue

        title = l10n.translate(v['title'], locale)

        # Keys endpoint
        paths[f'/collections/{k}/keys'] = {
            'get': {
                'summary': f'Get {title} join key fields',
                'description': f'Lists all available {title} join key fields',
                'tags': [k, API_NAME],
                'operationId': 'getKeys',
                'parameters': [
                    {'$ref': '#/components/parameters/f'},
                    # {'$ref': '#/components/parameters/lang'},
                ],
                'responses': {
                    '200': {
                        'description': 'Response',
                        'content': {
                            'application/json': {}
                        }
                    }
                }
            }
        }

        # Key value endpoint
        # See https://github.com/opengeospatial/ogcapi-joins/blob/master/sources/core/openapi/schemas/collectionKeyField.yaml
        key_field_param = {
            'name': 'keyFieldId',
            'in': 'path',
            'description': 'identifier of a key field',
            'required': True,
            'schema': {
                'type': 'string'
            }
        }
        description = f'Return all available {title} join key values'
        paths[f'/collections/{k}/keys/{{keyFieldId}}'] = {
            'get': {
                'summary': f'Get {title} values for a given join key field id',
                'description': description,
                'tags': [k, API_NAME],
                'operationId': 'getKeyValues',
                'parameters': [
                    key_field_param,
                    {'$ref': '#/components/parameters/f'},
                    # {'$ref': '#/components/parameters/lang'},
                ],
                'responses': {
                    '200': {
                        'description': 'Response',
                        'content': {
                            'application/json': {}
                        }
                    }
                },
            },
            # 'options': {
            #     'summary': f'Options for {title} key values by id',
            #     'description': description,
            #     'tags': [k],
            #     'operationId': f'options{k.capitalize()}KeyValues',
            #     'parameters': [
            #         {'$ref': f"{OPENAPI_YAML['oajoins']}/collectionKeyField.yaml"}  # noqa
            #     ],
            #     'responses': {
            #         '200': {'description': 'options response'}
            #     }
            # }
        }

    return [{'name': API_NAME}], {'paths': paths}


# Thread lock for joins.json file operations
_joins_lock = threading.Lock()


def get_joins_file_path(working_dir: Optional[str] = None) -> Path:
    """
    Get the path to the joins.json file.

    :param working_dir: Working directory path (default: current directory)

    :returns: Path object for joins.json
    """
    if working_dir is None:
        working_dir = os.getcwd()

    return Path(working_dir) / 'joins.json'


def load_joins_metadata(working_dir: Optional[str] = None) -> dict[str, Any]:
    """
    Load joins metadata from joins.json file.

    :param working_dir: Working directory path (default: current directory)

    :returns: Dictionary of join metadata indexed by join name
    """
    joins_file = get_joins_file_path(working_dir)

    with _joins_lock:
        if not joins_file.exists():
            LOGGER.debug(f'Joins file does not exist: {joins_file}')
            return {}

        try:
            with open(joins_file, 'r', encoding='utf-8') as f:
                joins_data = json.load(f)
                LOGGER.debug(f'Loaded {len(joins_data)} joins from {joins_file}')
                return joins_data
        except (json.JSONDecodeError, IOError) as e:
            LOGGER.error(f'Failed to load joins file: {e}')
            return {}


def save_joins_metadata(
        joins_data: dict[str, Any],
        working_dir: Optional[str] = None
) -> None:
    """
    Save joins metadata to joins.json file.

    :param joins_data: Dictionary of join metadata indexed by join name
    :param working_dir: Working directory path (default: current directory)
    """
    joins_file = get_joins_file_path(working_dir)

    with _joins_lock:
        try:
            # Ensure directory exists
            joins_file.parent.mkdir(parents=True, exist_ok=True)

            # Write with pretty formatting
            with open(joins_file, 'w', encoding='utf-8') as f:
                json.dump(joins_data, f, ensure_ascii=False, indent=2)

            LOGGER.debug(f'Saved {len(joins_data)} joins to {joins_file}')
        except IOError as e:
            LOGGER.error(f'Failed to save joins file: {e}')
            raise ValueError(f'Failed to save joins metadata: {e}')


def generate_unique_join_name(
        left_dataset_name: str,
        right_dataset_name: str,
        working_dir: Optional[str] = None
) -> str:
    """
    Generate a unique join name with auto-incrementing number.

    Format: {leftDatasetName}-{rightDatasetName}-{num}
    The num is 1-based and increments if there's a naming conflict.

    :param left_dataset_name: Name of the left dataset
    :param right_dataset_name: Name of the right dataset (without extension)
    :param working_dir: Working directory path (default: current directory)

    :returns: Unique join name
    """
    joins_data = load_joins_metadata(working_dir)

    # Sanitize dataset names (remove special characters)
    left_clean = re.sub(r'[^a-zA-Z0-9_-]', '_', left_dataset_name)
    right_clean = re.sub(r'[^a-zA-Z0-9_-]', '_', right_dataset_name)

    # Find the next available number
    num = 1
    while True:
        join_name = f"{left_clean}-{right_clean}-{num}"
        if join_name not in joins_data:
            LOGGER.debug(f'Generated unique join name: {join_name}')
            return join_name
        num += 1


def save_join_result(
        join_name: str,
        joined_geojson: dict[str, Any],
        output_path: str,
        left_dataset_name: str,
        right_dataset_name: str,
        left_dataset_key: str,
        right_dataset_key: str,
        statistics: dict[str, int],
        csv_delimiter: str = ',',
        csv_header_row: int = 1,
        csv_data_start_row: int = 2,
        include_fields: Optional[list] = None,
        working_dir: Optional[str] = None
) -> dict[str, Any]:
    """
    Save join result to disk and persist metadata to joins.json.

    :param join_name: Unique name for this join
    :param joined_geojson: The joined GeoJSON data
    :param output_path: Path where the joined GeoJSON will be saved
    :param left_dataset_name: Name of the left dataset
    :param right_dataset_name: Name of the right dataset
    :param left_dataset_key: Join key from left dataset
    :param right_dataset_key: Join key from right dataset
    :param statistics: Join statistics dictionary
    :param csv_delimiter: CSV delimiter used
    :param csv_header_row: CSV header row number
    :param csv_data_start_row: CSV data start row number
    :param include_fields: List of fields included from right dataset
    :param working_dir: Working directory path (default: current directory)

    :returns: Join metadata dictionary
    """

    # Step 1: Save the joined GeoJSON to disk
    try:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(joined_geojson, f, ensure_ascii=False, indent=2)

        LOGGER.info(f'Saved join result to: {output_path}')
    except IOError as e:
        raise ValueError(f'Failed to save join result: {e}')

    # Step 2: Create join metadata
    join_metadata = {
        'id': join_name,
        'created': datetime.utcnow().isoformat() + 'Z',
        'leftDataset': {
            'name': left_dataset_name,
            'key': left_dataset_key
        },
        'rightDataset': {
            'name': right_dataset_name,
            'key': right_dataset_key,
            'format': 'text/csv',
            'delimiter': csv_delimiter,
            'headerRow': csv_header_row,
            'dataStartRow': csv_data_start_row
        },
        'parameters': {
            'includeFields': include_fields or []
        },
        'result': {
            'path': str(output_path),
            'format': 'application/geo+json'
        },
        'statistics': statistics
    }

    # Step 3: Load existing joins metadata
    joins_data = load_joins_metadata(working_dir)

    # Step 4: Add new join metadata
    joins_data[join_name] = join_metadata

    # Step 5: Save updated joins metadata
    save_joins_metadata(joins_data, working_dir)

    LOGGER.info(f'Persisted join metadata for: {join_name}')

    return join_metadata


def _remove_join(
        join_name: str,
        delete_file: bool = True,
        working_dir: Optional[str] = None
) -> bool:
    """
    Delete a join and optionally its result file.

    :param join_name: Name of the join to delete
    :param delete_file: Whether to delete the result file (default: True)
    :param working_dir: Working directory path (default: current directory)

    :returns: True if deleted, False if not found
    """
    joins_data = load_joins_metadata(working_dir)

    if join_name not in joins_data:
        LOGGER.warning(f'Join not found: {join_name}')
        return False

    join_metadata = joins_data[join_name]

    # Delete the result file if requested
    if delete_file:
        result_path = join_metadata.get('result', {}).get('path')
        if result_path:
            try:
                result_file = Path(result_path)
                if result_file.exists():
                    result_file.unlink()
                    LOGGER.info(f'Deleted join result file: {result_path}')
            except OSError as e:
                LOGGER.error(f'Failed to delete join result file: {e}')

    # Remove from metadata
    del joins_data[join_name]
    save_joins_metadata(joins_data, working_dir)

    LOGGER.info(f'Deleted join metadata: {join_name}')
    return True


def _parse_multipart_form_data(request) -> dict:
    """
    Parse multipart form data from Flask request.

    :param request: Flask request object

    :returns: Dictionary with form fields and files
    """
    form_data = {}

    # Parse form fields
    if hasattr(request, 'form'):
        for key, value in request.form.items():
            form_data[key] = value

    # Parse files
    if hasattr(request, 'files'):
        for key, file in request.files.items():
            form_data[key] = file

    return form_data


def _validate_join_parameters(form_data: dict) -> Tuple[bool, Optional[str]]:
    """
    Validate required join parameters from form data.

    :param form_data: Dictionary with form fields

    :returns: Tuple of (is_valid, error_message)
    """
    required_fields = [
        'left-dataset-url',
        'left-dataset-key',
        'right-dataset-format',
        'right-dataset-file',
        'right-dataset-key'
    ]

    for field in required_fields:
        if field not in form_data or not form_data[field]:
            return False, f'Missing required field: {field}'

    # Validate right dataset format
    right_format = form_data.get('right-dataset-format', '').lower()
    if right_format not in ['text/csv', 'csv']:
        return False, f'Unsupported right dataset format: {right_format}'

    return True, None


def list_joins(api: API, request: APIRequest) -> Tuple[dict, int, str]:
    """
    Returns all available joins from the server

    :param request: A request object

    :returns: tuple of headers, status code, content
    """
    headers = request.get_response_headers(**api.api_headers) if hasattr(request, 'get_response_headers') else {}

    try:
        working_dir = api.config.get('server', {}).get('workingdir', os.getcwd())
        all_joins = load_joins_metadata(working_dir)

        # # Filter joins for this dataset
        # dataset_joins = {
        #     name: metadata
        #     for name, metadata in all_joins.items()
        #     if metadata.get('leftDataset', {}).get('name') == dataset
        # }

        response = {
            'joins': all_joins,  # dataset_joins,
            'count': len(all_joins),  # len(dataset_joins)
        }

        return headers, HTTPStatus.OK, to_json(response, api.pretty_print)

    except Exception as e:
        LOGGER.error(f'Failed to list joins: {e}', exc_info=True)
        msg = f'Failed to list joins: {str(e)}'
        return api.get_exception(
            HTTPStatus.INTERNAL_SERVER_ERROR, headers, F_JSON,
            'NoApplicableCode', msg
        )


def get_join_metadata(api: API, request: APIRequest,
                      join_id: str) -> Tuple[dict, int, str]:
    """
    Returns the metadata for a specific join on the server

    :param request: A request object
    :param join_id: The id of the join to retrieve metadata for

    :returns: tuple of headers, status code, content
    """
    return {}, 200, "{}"


def perform_geojson_csv_join_from_stream(
        geojson_data: dict[str, Any],
        csv_stream: BinaryIO,
        geojson_key: str,
        csv_key: str,
        csv_delimiter: str = ',',
        csv_encoding: str = 'utf-8',
        csv_header_row: int = 1,
        csv_data_start_row: int = 2,
        include_fields: Optional[list[str]] = None
) -> Tuple[dict[str, Any], dict[str, int]]:
    """
    Performs a left join between an in-memory GeoJSON dict and a CSV stream.

    This function joins GeoJSON features with CSV records based on matching key fields.
    The CSV is read from a stream (e.g., from multipart form-data) and loaded into
    memory for fast lookups. The join is performed in-memory using dictionaries
    for O(n) performance.

    :param geojson_data: GeoJSON dict with 'features' array (left dataset)
    :param csv_stream: File-like object containing CSV data (e.g., from request.files)
    :param geojson_key: Property name in GeoJSON features to use as join key
    :param csv_key: Column name in CSV to use as join key
    :param csv_delimiter: CSV delimiter character (default: ',')
    :param csv_encoding: CSV file encoding (default: 'utf-8')
    :param csv_header_row: 1-based row number of the CSV header (default: 1)
    :param csv_data_start_row: 1-based row number where data starts (default: 2)
    :param include_fields: List of CSV field names to include in join (None = all fields)

    :returns: Tuple of (joined_geojson_dict, statistics_dict)
        - joined_geojson_dict: Modified GeoJSON with joined properties
        - statistics_dict: Dictionary with join statistics:
            - numberMatched: GeoJSON records matched with CSV
            - numberOfUnmatchedLeftItems: GeoJSON records without CSV match
            - numberOfUnmatchedRightItems: CSV records without GeoJSON match
    """

    LOGGER.debug('Starting GeoJSON-CSV join from stream')

    # Step 1: Validate GeoJSON structure
    if not isinstance(geojson_data, dict):
        raise ValueError('geojson_data must be a dictionary')

    if 'features' not in geojson_data:
        raise ValueError('Invalid GeoJSON: missing "features" array')

    features = geojson_data['features']
    total_geojson_records = len(features)

    LOGGER.debug(f'Processing {total_geojson_records} GeoJSON features')

    # Step 2: Read CSV stream into a dictionary for O(1) lookup
    csv_lookup: dict[str, dict[str, Any]] = {}
    csv_fieldnames: list[str] = []

    try:
        # Wrap binary stream in TextIOWrapper for reading
        text_stream = io.TextIOWrapper(csv_stream, encoding=csv_encoding, newline='')

        # Read all lines
        lines = text_stream.readlines()

        if len(lines) < csv_header_row:
            raise ValueError(
                f'CSV file has fewer lines ({len(lines)}) than header row number ({csv_header_row})'
            )

        # Extract header from specified row (convert to 0-based index)
        header_line = lines[csv_header_row - 1]
        csv_fieldnames = [field.strip() for field in header_line.strip().split(csv_delimiter)]

        LOGGER.debug(f'CSV header fields: {csv_fieldnames}')

        if csv_key not in csv_fieldnames:
            raise ValueError(
                f'CSV key field "{csv_key}" not found in CSV columns: {csv_fieldnames}'
            )

        # Validate include_fields if specified
        if include_fields:
            invalid_fields = [f for f in include_fields if f not in csv_fieldnames]
            if invalid_fields:
                raise ValueError(
                    f'Requested fields not found in CSV: {invalid_fields}'
                )
            fields_to_include = set(include_fields)
            # Always include the key field for joining
            fields_to_include.add(csv_key)
        else:
            fields_to_include = set(csv_fieldnames)

        LOGGER.debug(f'Including CSV fields: {fields_to_include}')

        # Process data rows starting from specified row
        if csv_data_start_row > len(lines):
            LOGGER.warning(
                f'Data start row ({csv_data_start_row}) exceeds file length ({len(lines)})'
            )

        row_count = 0
        for line_num in range(csv_data_start_row - 1, len(lines)):
            line = lines[line_num].strip()
            if not line:
                continue  # Skip empty lines

            # Parse CSV row
            values = [v.strip() for v in line.split(csv_delimiter)]

            # Create dict from header and values
            if len(values) != len(csv_fieldnames):
                LOGGER.warning(
                    f'Row {line_num + 1} has {len(values)} values but header has '
                    f'{len(csv_fieldnames)} fields. Skipping row.'
                )
                continue

            row_dict = dict(zip(csv_fieldnames, values))

            # Get key value
            key_value = row_dict.get(csv_key)
            if key_value is not None and key_value != '':
                # Convert to string for consistent comparison
                key_str = str(key_value).strip()

                # Filter to only include requested fields
                filtered_row = {
                    field: value
                    for field, value in row_dict.items()
                    if field in fields_to_include
                }

                # Store CSV row by key value
                # If duplicate keys exist, last one wins (standard left join behavior)
                csv_lookup[key_str] = filtered_row
                row_count += 1

        LOGGER.debug(f'Loaded {row_count} CSV records into lookup table')

    except (UnicodeDecodeError, ValueError) as e:
        raise ValueError(f'Failed to read CSV stream: {e}')

    total_csv_records = len(csv_lookup)
    LOGGER.debug(f'CSV lookup contains {total_csv_records} unique keys')

    # Step 3: Perform left join and collect statistics
    matched_count = 0
    unmatched_left_count = 0
    matched_csv_keys = set()

    for feature in features:
        if 'properties' not in feature:
            feature['properties'] = {}

        properties = feature['properties']
        geojson_key_value = properties.get(geojson_key)

        if geojson_key_value is not None:
            # Convert to string for consistent comparison
            geojson_key_str = str(geojson_key_value).strip()

            # Look up matching CSV record
            csv_record = csv_lookup.get(geojson_key_str)

            if csv_record is not None:
                # Match found - merge CSV properties into GeoJSON feature
                matched_count += 1
                matched_csv_keys.add(geojson_key_str)

                # Add CSV columns to properties (except the join key to avoid duplication)
                for csv_col, csv_val in csv_record.items():
                    if csv_col != csv_key:
                        properties[csv_col] = csv_val
            else:
                # No match found in CSV
                unmatched_left_count += 1
                LOGGER.debug(f'No CSV match for GeoJSON key: {geojson_key_str}')
        else:
            # GeoJSON feature missing the join key
            unmatched_left_count += 1
            LOGGER.debug(f'GeoJSON feature missing join key "{geojson_key}"')

    # Step 4: Calculate unmatched right items
    unmatched_right_count = total_csv_records - len(matched_csv_keys)

    # Step 5: Build statistics
    statistics = {
        'numberMatched': matched_count,
        'numberOfUnmatchedLeftItems': unmatched_left_count,
        'numberOfUnmatchedRightItems': unmatched_right_count
    }

    LOGGER.info(
        f'Join completed: {matched_count} matched, '
        f'{unmatched_left_count} unmatched left, '
        f'{unmatched_right_count} unmatched right'
    )

    return geojson_data, statistics


def create_join(api: API, request: APIRequest) -> Tuple[dict, int, str]:
    """
    Creates a new join on the server

    :param request: A request object

    :returns: tuple of headers, status code, content
    """
    if not request.is_valid(PLUGINS['formatter'].keys()):
        return api.get_format_exception(request)

    headers = request.get_response_headers(SYSTEM_LOCALE, **api.api_headers)

    collections = filter_dict_by_key_value(api.config['resources'],
                                           'type', 'collection')

    try:
        # Step 1: Parse multipart form data
        form_data = _parse_multipart_form_data(request)

        # Step 2: Validate parameters
        is_valid, error_msg = _validate_join_parameters(form_data)
        if not is_valid:
            LOGGER.error(error_msg)
            return api.get_exception(
                HTTPStatus.BAD_REQUEST, headers, F_JSON,
                'InvalidParameterValue', error_msg
            )

        # Step 3: Extract parameters
        dataset = form_data['left-dataset-url'].lstrip(api.get_collections_url()).strip('/')
        if dataset not in collections:
            msg = f'Collection (left dataset) not found: {dataset}'
            return api.get_exception(
                HTTPStatus.NOT_FOUND, headers, F_JSON,
                'NotFound', msg
            )
        left_dataset_key = form_data['left-dataset-key']
        right_dataset_key = form_data['right-dataset-key']
        csv_file = form_data['right-dataset-file']

        # Extract right dataset name from filename
        right_dataset_name = getattr(csv_file, 'filename', 'unknown')
        if right_dataset_name:
            # Remove file extension
            right_dataset_name = Path(right_dataset_name).stem

        # Optional parameters
        csv_delimiter = form_data.get('csv-file-delimiter', ',')
        csv_header_row = int(form_data.get('csv-file-header-row-number', 1))
        csv_data_start_row = int(form_data.get('csv-file-data-start-row-number', 2))

        # Parse include fields
        include_fields = None
        if 'right-dataset-data-value-list' in form_data:
            fields_str = form_data['right-dataset-data-value-list']
            if fields_str:
                include_fields = [f.strip() for f in fields_str.split(',') if f.strip()]

        LOGGER.info(f'Executing join for collection: {dataset}')
        LOGGER.debug(f'Left key: {left_dataset_key}, Right key: {right_dataset_key}')

        # Step 4: Get provider and fetch data
        try:
            provider_def = get_provider_by_type(
                collections[dataset]['providers']['providers'], 'feature'
            )
            provider = load_plugin('provider', provider_def)
        except ProviderTypeError:
            msg = f'Feature provider not found for collection: {dataset}'
            return api.get_exception(
                HTTPStatus.BAD_REQUEST, headers, F_JSON,
                'NoApplicableCode', msg
            )

        # Fetch all features from the provider
        try:
            result = provider.query(limit=10000)  # TODO: make this limitless
            if not result or 'features' not in result:
                msg = 'Failed to retrieve features from provider'
                return api.get_exception(
                    HTTPStatus.INTERNAL_SERVER_ERROR, headers, F_JSON,
                    'NoApplicableCode', msg
                )
        except Exception as e:
            msg = f'Provider query failed: {str(e)}'
            LOGGER.error(msg, exc_info=True)
            return api.get_exception(
                HTTPStatus.INTERNAL_SERVER_ERROR, headers, F_JSON,
                'NoApplicableCode', msg
            )

        # Step 5: Reset CSV file stream to beginning
        if hasattr(csv_file, 'seek'):
            csv_file.seek(0)

        # Step 6: Perform the join
        from pygeoapi.util import perform_geojson_csv_join_from_stream

        joined_geojson, statistics = perform_geojson_csv_join_from_stream(
            geojson_data=result,
            csv_stream=csv_file.stream if hasattr(csv_file, 'stream') else csv_file,
            geojson_key=left_dataset_key,
            csv_key=right_dataset_key,
            csv_delimiter=csv_delimiter,
            csv_encoding='utf-8',
            csv_header_row=csv_header_row,
            csv_data_start_row=csv_data_start_row,
            include_fields=include_fields
        )

        # Step 7: Generate unique join name
        working_dir = api.config.get('server', {}).get('workingdir', os.getcwd())
        join_name = generate_unique_join_name(
            left_dataset_name=dataset,
            right_dataset_name=right_dataset_name,
            working_dir=working_dir
        )

        LOGGER.info(f'Generated join name: {join_name}')

        # Step 8: Save join result to disk
        join_metadata = save_join_result(
            join_name=join_name,
            joined_geojson=joined_geojson,
            left_dataset_name=dataset,
            left_dataset_key=left_dataset_key,
            right_dataset_name=right_dataset_name,
            right_dataset_key=right_dataset_key,
            statistics=statistics,
            csv_delimiter=csv_delimiter,
            csv_header_row=csv_header_row,
            csv_data_start_row=csv_data_start_row,
            include_fields=include_fields,
            working_dir=working_dir
        )

        LOGGER.info(f'Join completed successfully: {join_name}')

        # Step 9: Return response with join metadata
        response = {
            'joinName': join_name,
            'metadata': join_metadata
        }

        return headers, HTTPStatus.CREATED, to_json(response, api.pretty_print)

    except ProviderGenericError as err:
        return api.get_exception(
            err.http_status_code, headers, F_JSON,
            err.ogc_exception_code, err.message
        )

    except ValueError as e:
        msg = str(e)
        return api.get_exception(
            HTTPStatus.BAD_REQUEST, headers, F_JSON,
            'InvalidParameterValue', msg
        )

    except Exception as e:
        LOGGER.error(f'Join execution failed: {e}', exc_info=True)
        msg = f'Join execution failed: {str(e)}'
        return api.get_exception(
            HTTPStatus.INTERNAL_SERVER_ERROR, headers, F_JSON,
            'NoApplicableCode', msg
        )












    if dataset not in collections.keys():
        msg = 'Collection not found'
        return api.get_exception(
            HTTPStatus.NOT_FOUND, headers, request.format, 'NotFound', msg)

    try:
        # Get the provider
        provider_def = get_provider_by_type(
            collections[dataset]['providers'], 'feature')
        p = load_plugin('provider', provider_def)

        # Query all features from provider
        LOGGER.debug(f'Fetching features from collection: {dataset}')
        result = p.query(limit=10000)  # Adjust limit as needed

        if 'features' not in result:
            msg = 'Provider did not return features'
            return api.get_exception(
                HTTPStatus.INTERNAL_SERVER_ERROR, headers, request.format,
                'NoApplicableCode', msg)

        # Perform the join
        from pygeoapi.util import perform_geojson_csv_join_from_stream

        joined_geojson, statistics = perform_geojson_csv_join_from_stream(
            geojson_data=result,
            csv_stream=csv_stream,
            geojson_key=geojson_key,
            csv_key=csv_key,
            csv_delimiter=csv_delimiter,
            csv_encoding=csv_encoding
        )

        # Build response
        content = {
            'type': 'FeatureCollection',
            'features': joined_geojson.get('features', []),
            'statistics': statistics,
            'numberMatched': statistics['numberMatched'],
            'numberOfUnmatchedLeftItems': statistics['numberOfUnmatchedLeftItems'],
            'numberOfUnmatchedRightItems': statistics['numberOfUnmatchedRightItems']
        }

        return headers, HTTPStatus.CREATED, to_json(content, api.pretty_print)

    except ProviderTypeError:
        msg = 'Invalid provider type'
        return api.get_exception(
            HTTPStatus.BAD_REQUEST, headers, request.format,
            'NoApplicableCode', msg)

    except ProviderGenericError as err:
        return api.get_exception(
            err.http_status_code, headers, request.format,
            err.ogc_exception_code, err.message)

    except ValueError as e:
        msg = str(e)
        return api.get_exception(
            HTTPStatus.BAD_REQUEST, headers, request.format,
            'InvalidParameterValue', msg)

    except Exception as e:
        LOGGER.error(f'Join execution failed: {e}', exc_info=True)
        msg = f'Join execution failed: {str(e)}'
        return api.get_exception(
            HTTPStatus.INTERNAL_SERVER_ERROR, headers, request.format,
            'NoApplicableCode', msg)


def delete_join(api: API, request: APIRequest,
                join_id: str) -> Tuple[dict, int, str]:
    """
    Deletes a specific join on the server

    :param request: A request object
    :param join_id: The id of the join to remove

    :returns: tuple of headers, status code, content
    """
    headers = request.get_response_headers(**api.api_headers) if hasattr(request, 'get_response_headers') else {}

    try:
        working_dir = api.config.get('server', {}).get('workingdir', os.getcwd())

        # Verify the join exists
        join_metadata = load_joins_metadata(working_dir).get(join_id)

        if not join_metadata:
            msg = f'Join not found: {join_id}'
            return api.get_exception(
                HTTPStatus.NOT_FOUND, headers, F_JSON,
                'NotFound', msg
            )

        # Delete the join
        success = _remove_join(join_id, delete_file=True, working_dir=working_dir)

        if success:
            return headers, HTTPStatus.NO_CONTENT, ''
        else:
            msg = f'Failed to delete join: {join_id}'
            return api.get_exception(
                HTTPStatus.INTERNAL_SERVER_ERROR, headers, F_JSON,
                'NoApplicableCode', msg
            )

    except Exception as e:
        LOGGER.error(f'Failed to delete join: {e}', exc_info=True)
        msg = f'Failed to delete join: {str(e)}'
        return api.get_exception(
            HTTPStatus.INTERNAL_SERVER_ERROR, headers, F_JSON,
            'NoApplicableCode', msg
        )


def get_collection_key_values(api: API, request: APIRequest, dataset: str,
                              field_id: str) -> Tuple[dict, int, str]:
    """
    Returns all the values "as-is" for a given key field in a collection

    :param request: A request object
    :param dataset: dataset name
    :param field_id: field identifier (name) for which to list the values

    :returns: tuple of headers, status code, content
    """
    if not request.is_valid(PLUGINS['formatter'].keys()):
        return api.get_format_exception(request)

    headers = request.get_response_headers(SYSTEM_LOCALE, **api.api_headers)

    collections = filter_dict_by_key_value(api.config['resources'],
                                           'type', 'collection')
    if dataset not in collections.keys():
        msg = 'Collection not found'
        return api.get_exception(
            HTTPStatus.NOT_FOUND, headers, request.format, 'NotFound', msg)

    LOGGER.debug(f"Retrieving key field configuration for collection '{dataset}'")

    try:
        provider_def = get_provider_by_type(
            collections[dataset]['providers'], 'feature')
        key_fields = _get_key_fields(provider_def, dataset)
        p = load_plugin('provider', provider_def)
    except ProviderTypeError:
        msg = 'Invalid provider type'
        return api.get_exception(
            HTTPStatus.BAD_REQUEST, headers, request.format,
            'NoApplicableCode', msg)
    except ProviderGenericError as err:
        return api.get_exception(
            err.http_status_code, headers, request.format,
            err.ogc_exception_code, err.message)

    LOGGER.debug('Processing offset parameter')
    try:
        offset = int(request.params.get('offset'))
        if offset < 0:
            msg = 'offset value should be positive or zero'
            return api.get_exception(
                HTTPStatus.BAD_REQUEST, headers, request.format,
                'InvalidParameterValue', msg)
    except ValueError:
        msg = 'offset value should be an integer'
        return api.get_exception(
            HTTPStatus.BAD_REQUEST, headers, request.format,
            'InvalidParameterValue', msg)
    except TypeError as err:
        LOGGER.warning(err)
        offset = 0

    LOGGER.debug('Processing limit parameter')
    if api.config['server'].get('limit') is not None:
        msg = ('server.limit is no longer supported! '
               'Please use limits at the server or collection '
               'level (RFC5)')
        LOGGER.warning(msg)
    try:
        limit = evaluate_limit(request.params.get('limit'),
                               api.config['server'].get('limits', {}),
                               collections[dataset].get('limits', {}))
    except ValueError as err:
        return api.get_exception(
            HTTPStatus.BAD_REQUEST, headers, request.format,
            'InvalidParameterValue', str(err))

    if field_id not in set(k['id'] for k in key_fields):
        msg = 'Key field not found'
        return api.get_exception(
            HTTPStatus.NOT_FOUND, headers, request.format, 'NotFound', msg)

    # Get provider locale (if any)
    prv_locale = l10n.get_plugin_locale(provider_def, request.raw_locale)

    content = {}
    try:
        result = p.query(offset=offset, limit=limit, language=prv_locale,
                         skip_geometry=True, select_properties=[field_id])
    except ProviderGenericError as err:
        return api.get_exception(
            err.http_status_code, headers, request.format,
            err.ogc_exception_code, err.message)

    # Process features to extract values for the specified field
    content['keys'] = [
        {
            'value': f['properties'][field_id],
            'title': str(f['properties'][field_id])
        }
        for f in result.get('features', [])
    ]
    content['numberMatched'] = result['numberMatched']
    content['numberReturned'] = result['numberReturned']

    serialized_query_params = ''
    for k, v in request.params.items():
        if k not in ('f', 'offset'):
            serialized_query_params += '&'
            serialized_query_params += urllib.parse.quote(k, safe='')
            serialized_query_params += '='
            serialized_query_params += urllib.parse.quote(str(v), safe=',')

    uri = f'{api.get_collections_url()}/{dataset}/keys/{field_id}'
    content['links'] = [
        {
            'type': FORMAT_TYPES[F_JSON],
            'rel': request.get_linkrel(F_JSON),
            'title': l10n.translate('This document as JSON', request.locale),  # noqa
            'href': f'{uri}?f={F_JSON}'
        }, {
            'type': FORMAT_TYPES[F_JSONLD],
            'rel': request.get_linkrel(F_JSONLD),
            'title': l10n.translate('This document as RDF (JSON-LD)', request.locale),  # noqa
            'href': f'{uri}?f={F_JSONLD}'
        }, {
            'type': FORMAT_TYPES[F_HTML],
            'rel': request.get_linkrel(F_HTML),
            'title': l10n.translate('This document as HTML', request.locale),  # noqa
            'href': f'{uri}?f={F_HTML}'
        }
    ]

    next_link = False
    prev_link = False

    if 'next' in [link['rel'] for link in content['links']]:
        LOGGER.debug('Using next link from provider')
    else:
        if content.get('numberMatched', -1) > (limit + offset):
            next_link = True
        elif len(content['keys']) == limit:
            next_link = True

        if offset > 0:
            prev_link = True

    if prev_link:
        prev = max(0, offset - limit)
        content['links'].append(
            {
                'type': 'application/geo+json',
                'rel': 'prev',
                'title': l10n.translate('Items (prev)', request.locale),
                'href': f'{uri}?offset={prev}{serialized_query_params}'
            })

    if next_link:
        next_ = offset + limit
        next_href = f'{uri}?offset={next_}{serialized_query_params}'
        content['links'].append(
            {
                'type': 'application/geo+json',
                'rel': 'next',
                'title': l10n.translate('Items (next)', request.locale),
                'href': next_href
            })

    content['links'].append(
        {
            'type': FORMAT_TYPES[F_JSON],
            'title': l10n.translate(
                collections[dataset]['title'], request.locale),
            'rel': 'collection',
            'href': '/'.join(uri.split('/')[:-1])
        })

    # Set response language to requested provider locale
    # (if it supports language) and/or otherwise the requested pygeoapi
    # locale (or fallback default locale)
    l10n.set_response_language(headers, prv_locale, request.locale)

    return headers, HTTPStatus.OK, to_json(content, api.pretty_print)


def get_collection_key_fields(api: API, request: APIRequest,
                              dataset: str) -> Tuple[dict, int, str]:
    """
    Returns all available join key fields for a collection

    :param request: A request object
    :param dataset: dataset name

    :returns: tuple of headers, status code, content
    """
    headers = request.get_response_headers(SYSTEM_LOCALE, **api.api_headers)

    collections = filter_dict_by_key_value(api.config['resources'],
                                           'type', 'collection')
    if dataset not in collections.keys():
        msg = 'Collection not found'
        return api.get_exception(
            HTTPStatus.NOT_FOUND, headers, request.format, 'NotFound', msg)

    LOGGER.debug(f"Retrieving key field configuration for collection '{dataset}'")

    provider_def = get_provider_by_type(
        collections[dataset]['providers'], 'feature')
    key_fields = _get_key_fields(provider_def, dataset)

    content = {
        'links': [
            {
                'type': FORMAT_TYPES[F_JSON],
                'rel': request.get_linkrel(F_JSON),
                'title': l10n.translate('This document as JSON', request.locale),  # noqa
                'href': f'{api.get_collections_url()}/keys?f={F_JSON}'
            }, {
                'type': FORMAT_TYPES[F_JSONLD],
                'rel': request.get_linkrel(F_JSONLD),
                'title': l10n.translate('This document as RDF (JSON-LD)', request.locale),  # noqa
                'href': f'{api.get_collections_url()}/keys?f={F_JSONLD}'
            }, {
                'type': FORMAT_TYPES[F_HTML],
                'rel': request.get_linkrel(F_HTML),
                'title': l10n.translate('This document as HTML', request.locale),  # noqa
                'href': f'{api.get_collections_url()}/keys?f={F_HTML}'
            }
        ],
        'keys': []
    }

    for field in key_fields:
        field_id = field['id']
        content['keys'].append({
            'id': field['id'],
            'isDefault': field.get('default', False),
            'language': SYSTEM_LOCALE.language,
            'links': [
                {
                    'type': FORMAT_TYPES[F_JSON],
                    'rel': request.get_linkrel(F_JSON),
                    'title': l10n.translate('The key values as JSON', request.locale),  # noqa
                    'href': f'{api.get_collections_url()}/keys/{field_id}?f={F_JSON}'
                }, {
                    'type': FORMAT_TYPES[F_JSONLD],
                    'rel': request.get_linkrel(F_JSONLD),
                    'title': l10n.translate('The key values as RDF (JSON-LD)', request.locale),  # noqa
                    'href': f'{api.get_collections_url()}/keys/{field_id}?f={F_JSONLD}'
                }, {
                    'type': FORMAT_TYPES[F_HTML],
                    'rel': request.get_linkrel(F_HTML),
                    'title': l10n.translate('The key values as HTML', request.locale),  # noqa
                    'href': f'{api.get_collections_url()}/keys/{field_id}?f={F_HTML}'
                }
            ]
        })

    return headers, HTTPStatus.OK, to_json(content, api.pretty_print)
