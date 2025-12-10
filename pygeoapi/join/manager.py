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
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from io import TextIOWrapper
from operator import itemgetter
from tinydb import TinyDB, Query
from typing import Optional, Any
from filelock import FileLock

from pygeoapi import util
from pygeoapi.provider.base import BaseProvider

# Join source file name pattern: table-{uuid}.json
_SOURCE_FILE_PATTERN = re.compile(
    r'^table-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.json$',  # noqa
    re.IGNORECASE
)

LOGGER = logging.getLogger(__name__)


class JoinManager:
    """Manager for OGC API - Joins functionality."""

    def __init__(self, source_dir: Path, **kwargs):
        """
        Initialize a JoinManager for OGC API - Joins.

        :param source_dir: source directory to persist join sources in
        :param kwargs: additional configuration parameters for join sources
        """
        self._source_dir = source_dir
        self._max_days = kwargs.get('max_days', 0)
        self._max_files = kwargs.get('max_files', 0)

        # Initialize database and lock
        self._db_path = self._source_dir / 'join_sources.tinydb'
        self._db_lock = FileLock(self._db_path.with_suffix('.tinydb.lock'))

        # (Re)populate TinyDB with existing join source references on disk
        self._build_refs()

        # Perform cleanup if needed
        self._cleanup_sources()

        LOGGER.debug(
            f'JoinManager initialized with source_dir: {self._source_dir}')

    @property
    def source_dir(self) -> Path:
        """Source directory in which to persist the join sources."""
        return self._source_dir

    @property
    def max_days(self) -> int:
        """Maximum number of days to keep join sources."""
        return self._max_days

    @property
    def max_files(self) -> int:
        """Maximum number of join source files to keep at any time."""
        return self._max_files

    @classmethod
    def from_config(cls, config: dict) -> Optional['JoinManager']:
        """
        Factory method to create JoinManager from configuration.

        :param config: Full pygeoapi configuration dict
        :returns: JoinManager instance if valid config, None otherwise
        """
        # Check if joins configuration exists
        try:
            joins_config = config.get('server', {})['joins']
        except KeyError:
            # pygeoapi was configured without OGC API - Joins:
            return None

        # Check if 'joins' key was set, but without further configuration
        if joins_config is None:
            LOGGER.debug('pygeoapi server.joins configured with defaults')
            joins_config = {}

        source_dir = Path(tempfile.gettempdir())
        conf_source_dir = joins_config.get('source_dir')
        try:
            if conf_source_dir:
                conf_source_dir = Path(conf_source_dir).resolve()
                # Make sure that the directory exists (recursively)
                # NOTE: if user configured 'temp_dir' with a file path
                #       and the file exists, this will raise an error
                conf_source_dir.mkdir(parents=True, exist_ok=True)
                source_dir = conf_source_dir

            # Test write permissions
            test_file = source_dir / '.write_test'
            test_file.touch()
            test_file.unlink()

        except (OSError, PermissionError, FileExistsError) as e:
            LOGGER.error(
                f'Cannot access or write to source_dir {source_dir}: {e}',
                exc_info=True
            )
            LOGGER.debug('OGC API - Joins will be unavailable')
            return None

        # Validate numeric settings
        max_days = joins_config.get('max_days', 0)
        max_files = joins_config.get('max_files', 0)

        if not isinstance(max_days, int) or max_days < 0:
            LOGGER.warning(
                f'Invalid max_days value: {max_days}, defaulting to 0 (disabled)')  # noqa
            max_days = 0

        if not isinstance(max_files, int) or max_files < 0:
            LOGGER.warning(
                f'Invalid max_files value: {max_files}, defaulting to 0 (disabled)')  # noqa
            max_files = 0

        # Create and return manager
        try:
            manager = cls(source_dir, max_days=max_days, max_files=max_files)
            LOGGER.debug('JoinManager successfully created')
            return manager
        except Exception as e:
            LOGGER.error(f'Failed to create JoinManager: {e}')
            return None

    @contextmanager
    def _db(self):
        """Context manager for locked TinyDB access."""
        with self._db_lock:
            with TinyDB(self._db_path) as db:
                yield db

    def _build_refs(self):
        """Load join source files from disk and upsert refs into TinyDB."""
        result = {}
        with self._db() as db:
            for file in self.source_dir.iterdir():
                if file.is_file() and _SOURCE_FILE_PATTERN.match(file.name):
                    with FileLock(file.with_suffix(file.suffix + '.lock')):
                        with open(file, 'r') as f:
                            try:
                                data = json.load(f)
                                doc = {
                                    'id': data['id'],
                                    'collectionId': data['collectionId'],
                                    'timeStamp': data['timeStamp'],
                                    'joinSource': data['joinSource'],
                                    'ref': str(file)
                                }
                            except Exception as e:
                                # Ignore file if not valid JSON
                                LOGGER.debug(str(e), exc_info=True)
                                continue
                            q = Query()
                            db.upsert(doc, (q.id == data['id']) & (q.collectionId == data['collectionId']))  # noqa
                            result.setdefault(data['collectionId'], {})[data['id']] = doc  # noqa
        return result

    @staticmethod
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

        with FileLock(path.with_suffix(path.suffix + '.lock')):
            # Remove file
            try:
                path.unlink(missing_ok=True)
                LOGGER.debug(f'removed join source file: {path}')
            except Exception as e:
                LOGGER.warning(f'failed to remove join source {path}: {e}')
                if not silent:
                    raise
                else:
                    return False

        return True

    def _cleanup_sources(self):
        """
        Removes stale join source files, if a max age or limit was set.
        This method has no effect if there aren't any source references.

        This method will also build the source reference cache, if empty.
        """
        now = datetime.now(timezone.utc)
        max_age = timedelta(days=self.max_days)

        with self._db() as db:
            q = Query()
            for collection_id, sources in self._build_refs().items():
                # sort sources by timestamp in ascending order
                # and output as tuple (timestamp, id, ref)
                source_items = sorted(
                    [(util.str_to_datetime(info['timeStamp']), info['id'],
                      Path(info['ref'])) for info in sources.values()],
                    key=itemgetter(0)
                )

                # pass 1: limit by max_days (if configured)
                if max_age.days > 0:
                    for timestamp, source_id, ref in source_items:
                        if now - timestamp <= max_age:
                            continue
                        if self._delete_source(ref, True):
                            db.remove((q.collectionId == collection_id) & (q.id == source_id))  # noqa
                            LOGGER.debug(f'removed stale source: {ref}')
                        else:
                            LOGGER.warning(f'could not remove stale source: {ref}')  # noqa

                # pass 2: limit by max_files (if configured)
                if 0 < self.max_files < len(sources):
                    for _, source_id, ref in list(reversed(source_items))[:self.max_files]:  # noqa
                        if self._delete_source(ref, True):
                            db.remove((q.collectionId == collection_id) & (q.id == source_id))  # noqa
                            LOGGER.debug(f'removed stale source: {ref}')
                        else:
                            LOGGER.warning(f'could not remove stale source: {ref}')  # noqa

    def _make_source_path(self, join_id: str) -> Path:
        """
        Makes a join source path for storage.
        Note that the file is not actually created.

        :param join_id: ID of the join data to retrieve

        returns: a `Path` instance for a JSON file
        """
        json_file = self.source_dir / f'table-{join_id}.json'
        return json_file

    def _find_source_path(self, collection_id: str, join_id: str) -> Path:
        """
        Finds the path to a join source file on disk.
        Raises a JoinSourceNotFoundError if the source is not found.
        Raises a JoinSourceMissingError if the source reference is missing.

        :param collection_id: Collection identifier
        :param join_id: Join source identifier

        :returns: `Path` instance to the join source file
        """
        with self._db() as db:
            q = Query()
            result = db.get((q.id == join_id) &
                            (q.collectionId == collection_id))
            if not result:
                raise JoinSourceNotFoundError(
                    f'join source {join_id} not found for collection {collection_id}')  # noqa

            file_path = Path(result['ref'])

            # Verify file still exists
            if not file_path.is_file():
                # Clean up orphaned database entry
                LOGGER.warning(f'Join source file missing: {file_path}')
                db.remove((q.id == join_id) &
                          (q.collectionId == collection_id))
                raise JoinSourceMissingError(
                    f'join source {join_id} for collection {collection_id} was removed')  # noqa

            return file_path

    @staticmethod
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

    def process_csv(self, collection_id: str,
                    collection_provider: BaseProvider,
                    form_data: dict) -> dict:
        """
        Processes the CSV form data and stores the result as a JSON file
        in a temporary directory.

        Example response:
        {
            "id": "13b40adb-aef3-4f6b-8d32-acf3ab082d2d",
            "timeStamp": "2025-12-10T12:26:17.542928Z",
            "collectionId": "cities",
            "collectionKey": "id",
            "joinSource": "city_data.csv",
            "joinKey": "city_id",
            "joinFields": ["city_name", "population"],
            "numberOfRows": 50,
            "data": {
                "12345": ["Amsterdam", "1100000"],
                "67890": ["Rotterdam", "650000"]
            }
        }


        :param collection_id: collection name to apply join source to
        :param collection_provider: feature collection provider
        :param form_data: parameters dict (from request form data)

        :returns: dictionary containing the processed join data
        """
        # Ensure that we really have a feature provider here
        if collection_provider.type != 'feature':
            raise ValueError(
                f'join source must be linked to a feature provider '
                f'(got {collection_provider.type})')

        # Extract required parameters (raises KeyError if missing)
        left_dataset_key = form_data['collectionKey']
        right_dataset_key = form_data['joinKey']
        csv_data = form_data['joinFile']

        if left_dataset_key not in collection_provider.get_key_fields():
            raise ValueError(f'collectionKey \'{left_dataset_key}\' not found '
                             f'in feature collection \'{collection_id}\'')

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
            # TODO: support other encodings (in OGC API - Joins spec)
            text_stream = TextIOWrapper(csv_data.buffer,
                                        encoding='utf-8', errors='replace')
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
            reader = csv.DictReader(all_lines[csv_header_row - 1:],
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
                # should not raise as we already validated row nums
                next(reader)

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
            "collectionId": collection_id,
            "collectionKey": left_dataset_key,
            "joinSource": csv_data.name,
            "joinKey": right_dataset_key,
            "joinFields": join_fields,
            "numberOfRows": num_keys,
            "data": join_data
        }

        # Lazily clean up any stale sources
        self._cleanup_sources()

        # Store the output as JSON file named 'table-{uuid}.json'
        json_file = self._make_source_path(source_id)
        with FileLock(json_file.with_suffix(json_file.suffix + '.lock')):
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=4)

        # Write source file reference to TinyDB for lookup
        with self._db() as db:
            doc = {
                'id': source_id,
                'collectionId': collection_id,
                'timeStamp': created,
                'joinSource': csv_data.name,
                'ref': str(json_file)
            }
            db.insert(doc)

        return output

    def list_sources(self, collection_id: str) -> dict:
        """
        Retrieve all available join sources for a given feature collection.
        Does not return entire join source metadata objects, only references.

        :param collection_id: name of feature collection

        :returns: dict[str, dict] with references for each source ID
        """
        with self._db() as db:
            q = Query()
            sources = db.search(q.collectionId == collection_id)

            # Only return sources that exist on disk
            valid_sources = {}
            for source in sources:
                file_path = Path(source['ref'])
                if file_path.is_file():
                    valid_sources[source['id']] = {
                        'timeStamp': source['timeStamp'],
                        'joinSource': source['joinSource'],
                        'ref': file_path
                    }
                else:
                    # Clean up orphaned entry
                    LOGGER.warning(f'Removing orphaned join source: {source["id"]}')  # noqa
                    db.remove((q.id == source['id']) &
                              (q.collectionId == collection_id))

            return valid_sources

    def read_join_source(self, collection_id: str, join_id: str) -> dict:
        """
        Read specific join source metadata.
        Raises a JoinSourceNotFoundError if the source is not found.
        Raises a JoinSourceMissingError if the source reference is missing.

        :param collection_id: Collection identifier
        :param join_id: Join source identifier
        :returns: Join source metadata dict
        """
        if not self._valid_id(join_id):
            raise ValueError('invalid join source ID')

        # This may raise JoinSourceNotFoundError or JoinSourceMissingError
        source_path = self._find_source_path(collection_id, join_id)

        # Read full JSON source and return document
        with FileLock(source_path.with_suffix(source_path.suffix + '.lock')):
            with open(source_path, 'r', encoding='utf-8') as f:
                source_dict = json.load(f)

        return source_dict

    def perform_join(self, features: dict, collection_id: str, join_id: str):
        """
        On-the-fly join of a join source table to a feature collection.
        The join will be in-place, so this method returns nothing.

        :param features: feature collection or single feature to apply join to
        :param collection_id: name of feature collection
        :param join_id: ID of the join data source to retrieve

        :raises: ValueError if the join_id is invalid,
                 KeyError if the join source does not exist
        """
        if not isinstance(features, dict):
            raise ValueError('provide single feature or collection object')

        type_ = features['type']
        source_dict = self.read_join_source(collection_id, join_id)

        # First perform join on each GeoJSON Feature
        join_count = 0
        collection_key = source_dict['collectionKey']
        join_data = source_dict['data']
        join_fields = source_dict['joinFields']
        is_collection = 'features' in features and type_ == 'FeatureCollection'
        items = features['features'] if is_collection else [features]
        for feature in items:
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

        # For FeatureCollections, add join count as foreign member
        if is_collection:
            features['numberJoined'] = join_count

    def remove_source(self, collection_id: str, join_id: str) -> bool:
        """
        Remove the join source data for a feature collection.

        :param collection_id: name of feature collection
        :param join_id: ID of the join data source to remove

        :returns: True on success, False otherwise
        """
        if not self._valid_id(join_id):
            raise ValueError('invalid join source ID')

        try:
            source_path = self._find_source_path(collection_id, join_id)
        except JoinSourceNotFoundError:
            # If the join source was not found, we should respond with a 404
            return False
        except JoinSourceMissingError:
            # Source file was removed, but reference still existed: 200
            return True

        # Remove the JSON file from disk
        deleted = self._delete_source(source_path)

        # Remove reference (clean up orphan)
        if deleted:
            with self._db() as db:
                q = Query()
                db.remove((q.id == join_id) &
                          (q.collectionId == collection_id))

        return deleted


class JoinSourceNotFoundError(Exception):
    """Join source is not found (by ID and/or collection)."""
    pass


class JoinSourceMissingError(FileNotFoundError):
    """Join source is missing but still referenced (orphan)."""
    pass
