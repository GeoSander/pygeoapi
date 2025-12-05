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

import json
import pytest
import shutil
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import Mock
from io import BytesIO

from pygeoapi import join_util
from pygeoapi.provider.base import BaseProvider
from pygeoapi.util import FileObject, get_current_datetime

TEMP_DIR = Path(tempfile.gettempdir()) / "join_sources"


def prepare_join_dir(recreate=False):
    """Helper to remove (and recreate) a join source directory."""
    if TEMP_DIR.exists():
        shutil.rmtree(TEMP_DIR)
    if recreate:
        TEMP_DIR.mkdir(parents=True, exist_ok=True)


@pytest.fixture(scope='module', autouse=True)
def setup_module():
    """Runs before and after all tests in this module."""

    print("\n=== Setting up test_join_util module ===")
    prepare_join_dir(True)

    yield  # Tests run here

    # Teardown after all tests
    print("\n=== Tearing down test_join_util module ===")
    prepare_join_dir()


@pytest.fixture
def config_minimal():
    """Minimal pygeoapi config with joins enabled."""
    return {
        'server': {
            'joins': None
        }
    }


@pytest.fixture
def config():
    """pygeoapi config with joins fully configured."""
    return {
        'server': {
            'joins': {
                'source_dir': str(TEMP_DIR),
                'max_days': 7,
                'max_files': 100
            }
        }
    }


@pytest.fixture
def config_disabled():
    """Config without joins."""
    return {
        'server': {}
    }


@pytest.fixture
def valid_csv_content():
    """Valid CSV data."""
    return b"""id,name,population,area
1,City A,100000,50.5
2,City B,200000,75.3
3,City C,150000,60.2"""


@pytest.fixture
def csv_with_commas():
    """CSV with quoted fields containing commas."""
    return b"""id,name,address,population
1,City A,"123 Main St, Suite 100",100000
2,City B,"456 Oak Ave, Floor 2",200000"""


@pytest.fixture
def csv_empty_keys():
    """CSV with empty key values."""
    return b"""id,name,population
,City A,100000
2,City B,200000"""


@pytest.fixture
def csv_duplicate_keys():
    """CSV with duplicate keys."""
    return b"""id,name,population
1,City A,100000
1,City B,200000"""


@pytest.fixture
def csv_offset_header():
    """CSV with header not on first row."""
    return b"""# This is a comment
# Another comment
id,name,population
1,City A,100000
2,City B,200000"""


@pytest.fixture
def csv_custom_delimiter():
    """CSV with semicolon delimiter."""
    return b"""id;name;population
1;City A;100000
2;City B;200000"""


@pytest.fixture
def mock_provider():
    """Mock feature provider."""
    provider = Mock(spec=BaseProvider)
    provider.type = 'feature'
    provider.id_field = 'id'
    provider.get.return_value = {
        'id_field': 'id'
    }

    def mock_get_fields():
        return {
            'id': {'type': 'string'},
            'geometry': {'type': 'geometry'},
            'existing_field': {'type': 'string'}
        }

    def mock_get_key_fields():
        return {
            'id': {'type': 'string', 'default': True},
            'existing_field': {'type': 'string', 'default': False}
        }

    provider.get_fields = mock_get_fields
    provider.get_key_fields = mock_get_key_fields
    return provider


@pytest.fixture
def feature_collection():
    """Sample GeoJSON FeatureCollection."""
    return {
        'type': 'FeatureCollection',
        'features': [
            {
                'type': 'Feature',
                'id': '1',
                'properties': {'id': '1', 'name': 'Feature 1'},
                'geometry': {'type': 'Point', 'coordinates': [0, 0]}
            },
            {
                'type': 'Feature',
                'id': '2',
                'properties': {'id': '2', 'name': 'Feature 2'},
                'geometry': {'type': 'Point', 'coordinates': [1, 1]}
            },
            {
                'type': 'Feature',
                'id': '3',
                'properties': {'id': '3', 'name': 'Feature 3'},
                'geometry': {'type': 'Point', 'coordinates': [2, 2]}
            }
        ],
        'numberMatched': 3,
        'numberReturned': 3
    }


# Test JoinConfig class
def test_join_config_from_dict_minimal(config_minimal):
    """Test JoinConfig from minimal config."""
    cfg = join_util.JoinConfig.from_dict(config_minimal)
    assert cfg.enabled is True
    assert cfg.max_days == 0
    assert cfg.max_files == 0
    assert cfg.source_dir.exists()
    assert cfg.source_dir == Path(tempfile.gettempdir())


def test_join_config_from_dict_disabled(config_disabled):
    """Test JoinConfig when joins are not configured."""
    cfg = join_util.JoinConfig.from_dict(config_disabled)
    assert cfg.enabled is False
    assert cfg.max_days == 0
    assert cfg.max_files == 0


def test_join_config_from_dict_enabled(config):
    """Test JoinConfig when joins are configured."""
    cfg = join_util.JoinConfig.from_dict(config)
    assert cfg.enabled is True
    assert cfg.max_days == 7
    assert cfg.max_files == 100
    assert cfg.source_dir.exists()


def test_join_config_creates_directory(tmp_path):
    """Test that JoinConfig creates source directory if missing."""
    non_existent = tmp_path / "non_existent_dir"
    config = {
        'server': {
            'joins': {
                'source_dir': str(non_existent)
            }
        }
    }
    _ = join_util.JoinConfig.from_dict(config)
    assert non_existent.exists()


# Test enabled
def test_enabled(config_minimal, config, config_disabled):
    """Test enabled function with joins configured."""
    assert join_util.enabled(config_minimal) is True
    assert join_util.enabled(config) is True
    assert join_util.enabled(config_disabled) is False
    assert join_util.enabled({}) is False


# Test initialization
def test_init_disabled(config_disabled):
    """Test init with joins disabled."""
    result = join_util.init(config_disabled)
    assert result is False


def test_init_enabled_empty_dir(config):
    """Test init with joins enabled and empty directory."""
    result = join_util.init(config)
    assert result is True
    assert len(join_util._REF_CACHE) == 0


# This test has to be run here, else there will already be files
def test_list_sources_empty(config):
    """Test list_sources with no sources."""
    join_util.init(config)

    sources = join_util.list_sources('test_collection')
    assert sources == {}


def test_init_loads_existing_sources(config):
    """Test init loads existing join source files."""
    # Create a join source file
    source_id = '12345678-1234-1234-1234-123456789abc'
    source_file = TEMP_DIR / f'table-{source_id}.json'
    source_data = {
        'id': source_id,
        'timeStamp': get_current_datetime(),
        'collectionName': 'test_collection',
        'collectionKey': 'id',
        'joinSource': 'test.csv',
        'joinKey': 'city_id',
        'joinFields': ['name', 'population'],
        'numberOfRows': 2,
        'data': {
            '1': ['City A', '100000'],
            '2': ['City B', '200000'],
        }
    }

    prepare_join_dir(True)
    with open(source_file, 'w') as f:
        json.dump(source_data, f)

    result = join_util.init(config)
    assert result is True
    assert 'test_collection' in join_util._REF_CACHE
    assert source_id in join_util._REF_CACHE['test_collection']


def test_init_ignores_invalid_files(config):
    """Test init ignores files that don't match pattern."""
    prepare_join_dir(True)

    # Create invalid files
    (TEMP_DIR / 'invalid.json').write_text('{}')
    (TEMP_DIR / 'table-notauuid.json').write_text('{}')
    (TEMP_DIR / 'data.csv').write_text('')

    result = join_util.init(config)
    assert result is True
    assert len(join_util._REF_CACHE) == 0


# Test helper functions
def test_valid_id_valid():
    """Test _valid_id with valid UUID."""
    assert join_util._valid_id('12345678-1234-1234-1234-123456789abc') is True
    assert join_util._valid_id('{12345678-1234-1234-1234-123456789abc}') is True  # noqa


def test_valid_id_invalid():
    """Test _valid_id with invalid UUID."""
    assert join_util._valid_id('not-a-uuid') is False
    assert join_util._valid_id('12345') is False
    assert join_util._valid_id('') is False
    assert join_util._valid_id(None) is False


def test_make_source_path(config):
    """Test _make_source_path creates correct path."""
    join_util.init(config)
    join_id = '12345678-1234-1234-1234-123456789abc'
    path = join_util._make_source_path(join_id)

    assert path.name == f'table-{join_id}.json'
    assert path.parent == join_util._CONFIG.source_dir


def test_delete_source_nonexistent():
    """Test _delete_source with non-existent file."""
    path = TEMP_DIR / "nonexistent.json"
    result = join_util._delete_source(path)
    assert result is True


def test_delete_source_existing():
    """Test _delete_source with existing file."""
    prepare_join_dir(True)
    path = TEMP_DIR / "test.json"
    path.write_text('{}')

    assert path.exists()
    result = join_util._delete_source(path)
    assert result is True
    assert not path.exists()


def test_delete_source_silent():
    """Test _delete_source with silent=True on error."""
    prepare_join_dir(True)
    path = TEMP_DIR / "readonly.json"
    path.write_text('{}')
    path.chmod(0o444)  # Read-only

    # Result depends on OS permissions, just ensure no exception
    assert join_util._delete_source(path, silent=True) is False

    # Make file deletable again for cleanup
    path.chmod(0o777)  # Full permissions
    prepare_join_dir(True)


# Test process_csv
def test_process_csv_valid(config, mock_provider, valid_csv_content):
    """Test process_csv with valid CSV."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    result = join_util.process_csv('test_collection',
                                   mock_provider, form_data)

    assert 'id' in result
    assert result['collectionName'] == 'test_collection'
    assert result['collectionKey'] == 'id'
    assert result['joinKey'] == 'id'
    assert result['numberOfRows'] == 3
    assert 'data' in result
    assert '1' in result['data']
    assert result['data']['1'] == ['City A', '100000', '50.5']


def test_process_csv_custom_delimiter(config, mock_provider,
                                      csv_custom_delimiter):
    """Test process_csv with custom delimiter."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(csv_custom_delimiter),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj,
        'csvDelimiter': ';'
    }

    result = join_util.process_csv('test_collection',
                                   mock_provider, form_data)

    assert result['numberOfRows'] == 2
    assert '1' in result['data']


def test_process_csv_offset_header(config, mock_provider,
                                   csv_offset_header):
    """Test process_csv with header not on first row."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(csv_offset_header),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj,
        'csvHeaderRow': 3,
        'csvDataStartRow': 4
    }

    result = join_util.process_csv('test_collection',
                                   mock_provider, form_data)

    assert result['numberOfRows'] == 2


def test_process_csv_quoted_commas(config, mock_provider, csv_with_commas):
    """Test process_csv handles quoted fields with commas."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(csv_with_commas),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    result = join_util.process_csv('test_collection',
                                   mock_provider, form_data)

    assert '1' in result['data']
    # Address field should be intact with comma
    assert '123 Main St, Suite 100' in result['data']['1']


def test_process_csv_specific_fields(config, mock_provider, valid_csv_content):
    """Test process_csv with specific fields selected."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj,
        'joinFields': 'name,population'  # Exclude 'area'
    }

    result = join_util.process_csv('test_collection',
                                   mock_provider, form_data)

    assert result['joinFields'] == ['name', 'population']
    assert len(result['data']['1']) == 2


def test_process_csv_empty_key(config, mock_provider, csv_empty_keys):
    """Test process_csv raises error on empty key in CSV data."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(csv_empty_keys),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    with pytest.raises(ValueError, match='empty or missing key'):
        join_util.process_csv('test_collection', mock_provider,
                              form_data)


def test_process_csv_duplicate_key(config, mock_provider, csv_duplicate_keys):
    """Test process_csv raises error on duplicate key in CSV data."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(csv_duplicate_keys),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    with pytest.raises(ValueError, match='duplicate key'):
        join_util.process_csv('test_collection', mock_provider,
                              form_data)


def test_process_csv_missing_required_param(config, mock_provider,
                                            valid_csv_content):
    """Test process_csv raises error when missing required parameter."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        # Missing joinKey
        'joinFile': file_obj
    }

    with pytest.raises(KeyError):
        join_util.process_csv('test_collection', mock_provider,
                              form_data)


def test_process_csv_wrong_file_type(config, mock_provider):
    """Test process_csv raises error with wrong file type."""
    join_util.init(config)

    # Not a FileObject
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': 'not a file object'
    }

    with pytest.raises(ValueError, match='must be a'):
        join_util.process_csv('test_collection', mock_provider,
                              form_data)


def test_process_csv_wrong_content_type(config, mock_provider):
    """Test process_csv raises error with wrong content type."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.json',
        buffer=BytesIO(b'{}'),
        content_type='application/json'  # Wrong type
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    with pytest.raises(ValueError, match='must be of type "text/csv"'):
        join_util.process_csv('test_collection', mock_provider,
                              form_data)


def test_process_csv_invalid_delimiter(config, mock_provider,
                                       valid_csv_content):
    """Test process_csv raises error with multi-character delimiter."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj,
        'csvDelimiter': ',,'  # Invalid
    }

    with pytest.raises(ValueError, match='must be a single character'):
        join_util.process_csv('test_collection', mock_provider,
                              form_data)


def test_process_csv_invalid_header_row(config, mock_provider,
                                        valid_csv_content):
    """Test process_csv raises error with invalid header row."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj,
        'csvHeaderRow': 0  # Invalid
    }

    with pytest.raises(ValueError, match='must be at least 1'):
        join_util.process_csv('test_collection', mock_provider,
                              form_data)


def test_process_csv_data_before_header(config, mock_provider,
                                        valid_csv_content):
    """Test process_csv raises error when data row before header."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj,
        'csvHeaderRow': 2,
        'csvDataStartRow': 1  # Before header
    }

    with pytest.raises(ValueError, match='must be greater than'):
        join_util.process_csv('test_collection', mock_provider,
                              form_data)


def test_process_csv_header_exceeds_lines(config, mock_provider,
                                          valid_csv_content):
    """Test process_csv raises error when header row exceeds file length."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj,
        'csvDataStartRow': 101,  # Way more than actual lines
    }

    with pytest.raises(ValueError, match='exceeds number of CSV rows'):
        join_util.process_csv('test_collection', mock_provider,
                              form_data)


def test_process_csv_non_feature_provider(config, valid_csv_content):
    """Test process_csv raises error for non-feature provider."""
    join_util.init(config)

    coverage_provider = Mock(spec=BaseProvider)
    coverage_provider.type = 'coverage'  # Not 'feature'

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    with pytest.raises(ValueError, match='must be linked to a feature provider'):  # noqa
        join_util.process_csv('test_collection',
                              coverage_provider, form_data)


def test_process_csv_missing_join_key(config, mock_provider,
                                      valid_csv_content):
    """Test process_csv raises error when join key not in CSV."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'nonexistent_field',  # Not in CSV
        'joinFile': file_obj
    }

    with pytest.raises(ValueError, match='not found in CSV fields'):
        join_util.process_csv('test_collection', mock_provider,
                              form_data)


def test_process_csv_bad_collection_key(config, mock_provider,
                                        valid_csv_content):
    """Test process_csv raises error when collection key does not exist."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'nonexistent_key',  # not in collection
        'joinKey': 'id',
        'joinFile': file_obj
    }

    with pytest.raises(ValueError, match='not found in feature collection'):
        join_util.process_csv('test_collection', mock_provider,
                              form_data)


def test_process_csv_updates_cache(config, mock_provider, valid_csv_content):
    """Test process_csv updates REF_CACHE."""
    join_util.init(config)
    init_size = len(join_util._REF_CACHE.get('test_collection', {}))

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    result = join_util.process_csv('test_collection',
                                   mock_provider, form_data)

    assert 'test_collection' in join_util._REF_CACHE
    assert len(join_util._REF_CACHE['test_collection']) == init_size + 1
    assert result['id'] in join_util._REF_CACHE['test_collection']


def test_process_csv_creates_file(config, mock_provider, valid_csv_content):
    """Test process_csv creates JSON file on disk."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    result = join_util.process_csv('test_collection',
                                   mock_provider, form_data)

    # Check file exists
    ref = join_util._REF_CACHE['test_collection'][result['id']]['ref']
    assert ref.exists()

    # Verify file content
    with open(ref, 'r') as f:
        data = json.load(f)
    assert data['id'] == result['id']


# Test list_sources
def test_list_sources_disabled(config_disabled):
    """Test list_sources when joins disabled."""
    join_util.init(config_disabled)

    with pytest.raises(Exception, match='disabled'):
        join_util.list_sources('test_collection')


def test_list_sources_with_data(config, mock_provider, valid_csv_content):
    """Test list_sources returns existing sources."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    result = join_util.process_csv('test_collection',
                                   mock_provider, form_data)

    sources = join_util.list_sources('test_collection')
    assert result['id'] in sources
    assert 'timeStamp' in sources[result['id']]
    assert 'ref' in sources[result['id']]


# Test read_join_source
def test_read_join_source_invalid_id(config):
    """Test read_join_source with invalid UUID."""
    join_util.init(config)

    with pytest.raises(ValueError, match='invalid'):
        join_util.read_join_source('test_collection',
                                   'not-a-uuid')


def test_read_join_source_not_found(config):
    """Test read_join_source returns empty dict when not found."""
    join_util.init(config)

    valid_uuid = '12345678-1234-1234-1234-123456789abc'
    result = join_util.read_join_source('test_collection',
                                        valid_uuid)
    assert result == {}


def test_read_join_source_success(config, mock_provider, valid_csv_content):
    """Test read_join_source returns source data."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    created = join_util.process_csv('test_collection',
                                    mock_provider, form_data)

    result = join_util.read_join_source('test_collection',
                                        created['id'])
    assert result['id'] == created['id']
    assert result['collectionName'] == 'test_collection'
    assert 'data' in result


def test_read_join_source_caching(config, mock_provider, valid_csv_content):
    """Test read_join_source uses LRU cache."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    created = join_util.process_csv('test_collection',
                                    mock_provider, form_data)
    join_id = created['id']

    # First call
    result1 = join_util.read_join_source('test_collection',
                                         join_id)

    # Second call (should be cached)
    result2 = join_util.read_join_source('test_collection',
                                         join_id)

    assert result1 == result2
    assert id(result1) == id(result2)  # Same object from cache


# Test perform_join
def test_perform_join_success(config, mock_provider, valid_csv_content,
                              feature_collection):
    """Test perform_join adds fields to features."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    created = join_util.process_csv('test_collection',
                                    mock_provider, form_data)

    # Perform join
    join_util.perform_join(feature_collection, 'test_collection',
                           created['id'])

    # Check that fields were added
    assert 'name' in feature_collection['features'][0]['properties']
    assert 'population' in feature_collection['features'][0]['properties']
    assert feature_collection['features'][0]['properties']['name'] == 'City A'

    # Check joined flag
    assert feature_collection['features'][0]['joined'] is True


def test_perform_join_partial_match(config, mock_provider, valid_csv_content,
                                    feature_collection):
    """Test perform_join with features that don't all match."""
    join_util.init(config)

    # Add feature with non-matching ID
    feature_collection['features'].append({
        'type': 'Feature',
        'id': '999',
        'properties': {'id': '999', 'name': 'No Match'},
        'geometry': {'type': 'Point', 'coordinates': [3, 3]}
    })

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    created = join_util.process_csv('test_collection',
                                    mock_provider, form_data)

    # Perform join
    join_util.perform_join(feature_collection, 'test_collection',
                           created['id'])

    # Check matched features
    assert feature_collection['features'][0]['joined'] is True

    # Check unmatched feature
    assert feature_collection['features'][3]['joined'] is False
    assert 'population' not in feature_collection['features'][3]['properties']

    # Check numberJoined
    assert feature_collection['numberJoined'] == 3


def test_perform_join_no_source(config, feature_collection):
    """Test perform_join with non-existent source."""
    join_util.init(config)

    valid_uuid = '12345678-1234-1234-1234-123456789abc'

    # Should raise or handle gracefully
    with pytest.raises(Exception):
        join_util.perform_join(feature_collection,
                               'test_collection', valid_uuid)


# Test remove_source
def test_remove_source_invalid_id(config):
    """Test remove_source with invalid UUID."""
    join_util.init(config)

    with pytest.raises(ValueError, match='invalid'):
        join_util.remove_source('test_collection',
                                'not-a-uuid')


def test_remove_source_not_found(config):
    """Test remove_source returns False when not found."""
    join_util.init(config)

    valid_uuid = '12345678-1234-1234-1234-123456789abc'
    result = join_util.remove_source('test_collection',
                                     valid_uuid)
    assert result is False


def test_remove_source_success(config, mock_provider, valid_csv_content):
    """Test remove_source deletes file and cache entry."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    created = join_util.process_csv('test_collection',
                                    mock_provider, form_data)
    join_id = created['id']

    # Verify source exists
    assert 'test_collection' in join_util._REF_CACHE
    assert join_id in join_util._REF_CACHE['test_collection']

    # Remove source
    result = join_util.remove_source('test_collection', join_id)
    assert result is True

    # Verify removal
    assert join_id not in join_util._REF_CACHE.get('test_collection', {})


def test_remove_source_removes_collection(config, mock_provider,
                                          valid_csv_content):
    """Test remove_source removes collection key when last source deleted."""
    prepare_join_dir(True)
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    created = join_util.process_csv('test_collection',
                                    mock_provider, form_data)
    join_id = created['id']

    # Remove the only source
    join_util.remove_source('test_collection', join_id)

    # Collection key should be removed from cache
    assert 'test_collection' not in join_util._REF_CACHE


# Test cleanup_sources
def test_cleanup_sources_by_max_days(config, mock_provider, valid_csv_content):
    """Test cleanup removes sources older than max_days."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    # Create a source
    created = join_util.process_csv('test_collection',
                                    mock_provider, form_data)
    join_id = created['id']

    # Mock timestamp to be old
    old_timestamp = (datetime.now(timezone.utc) - timedelta(days=10)).strftime(
        '%Y-%m-%dT%H:%M:%S.%fZ'
    )
    join_util._REF_CACHE['test_collection'][join_id]['timeStamp'] = old_timestamp  # noqa

    # Run cleanup
    join_util._cleanup_sources()

    # Old source should be removed (max_days=7 in config)
    assert join_id not in join_util._REF_CACHE.get('test_collection', {})


def test_cleanup_sources_by_max_files(config, mock_provider,
                                      valid_csv_content):
    """Test cleanup removes sources exceeding max_files limit."""
    # Update config to have low max_files
    config['server']['joins']['max_files'] = 2
    prepare_join_dir(True)
    join_util.init(config)

    # Create 3 sources
    sources = []
    for i in range(3):
        file_obj = FileObject(
            name=f'test{i}.csv',
            buffer=BytesIO(valid_csv_content),
            content_type='text/csv'
        )

        form_data = {
            'collectionKey': 'id',
            'joinKey': 'id',
            'joinFile': file_obj
        }

        result = join_util.process_csv('test_collection',
                                       mock_provider, form_data)
        sources.append(result['id'])

    # Run cleanup
    join_util._cleanup_sources()

    # Should only keep 2 newest sources
    remaining = join_util._REF_CACHE.get('test_collection', {})
    assert len(remaining) <= 2


def test_cleanup_sources_disabled_when_not_configured(config_disabled):
    """Test cleanup does nothing when joins not configured."""
    join_util.init(config_disabled)

    # Should not raise error
    join_util._cleanup_sources()


def test_cleanup_sources_removes_nonexistent_files(config, mock_provider,
                                                   valid_csv_content):
    """Test cleanup removes cache entries for deleted files."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    created = join_util.process_csv('test_collection',
                                    mock_provider, form_data)
    join_id = created['id']

    # Manually delete the file
    ref = join_util._REF_CACHE['test_collection'][join_id]['ref']
    ref.unlink()

    # Run cleanup
    join_util._cleanup_sources()

    # Cache entry should be removed
    assert join_id not in join_util._REF_CACHE.get('test_collection', {})


# Test edge cases
def test_process_csv_empty_file(config, mock_provider):
    """Test process_csv with empty CSV file."""
    join_util.init(config)

    empty_csv = b""
    file_obj = FileObject(
        name='empty.csv',
        buffer=BytesIO(empty_csv),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    with pytest.raises(ValueError):
        join_util.process_csv('test_collection', mock_provider,
                              form_data)


def test_process_csv_only_header(config, mock_provider):
    """Test process_csv with only header row."""
    join_util.init(config)

    header_only_csv = b"id,name,population"
    file_obj = FileObject(
        name='header_only.csv',
        buffer=BytesIO(header_only_csv),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    with pytest.raises(ValueError, match='exceeds number of CSV rows'):
        join_util.process_csv('test_collection', mock_provider,
                              form_data)


def test_process_csv_skips_empty_rows(config, mock_provider):
    """Test process_csv skips empty rows."""
    join_util.init(config)

    csv_with_empty_rows = b"""id,name,population
1,City A,100000

2,City B,200000
,,
3,City C,150000"""

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(csv_with_empty_rows),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    result = join_util.process_csv('test_collection',
                                   mock_provider, form_data)

    # Should only have 3 rows (empty rows skipped)
    assert result['numberOfRows'] == 3


def test_process_csv_filters_provider_fields(config, mock_provider,
                                             valid_csv_content):
    """Test process_csv excludes fields that exist in provider."""
    join_util.init(config)

    # Mock provider has 'name' field already
    def mock_get_fields_with_name():
        return {
            'id': {'type': 'string'},
            'geometry': {'type': 'geometry'},
            'name': {'type': 'string'}  # Conflicts with CSV
        }

    mock_provider.get_fields = mock_get_fields_with_name

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    result = join_util.process_csv('test_collection',
                                   mock_provider, form_data)

    # 'name' should be excluded from join fields
    assert 'name' not in result['joinFields']
    assert 'population' in result['joinFields']
    assert 'area' in result['joinFields']


def test_perform_join_adds_foreign_members(config, mock_provider,
                                           valid_csv_content,
                                           feature_collection):
    """Test perform_join adds 'joined' and 'numberJoined' members."""
    join_util.init(config)

    file_obj = FileObject(
        name='test.csv',
        buffer=BytesIO(valid_csv_content),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': file_obj
    }

    created = join_util.process_csv('test_collection',
                                    mock_provider, form_data)

    # Perform join
    join_util.perform_join(feature_collection, 'test_collection',
                           created['id'])

    # Check foreign members
    assert 'numberJoined' in feature_collection
    assert feature_collection['numberJoined'] == 3

    for feature in feature_collection['features']:
        assert 'joined' in feature
        assert isinstance(feature['joined'], bool)


def test_find_source_path_raises_on_not_found(config):
    """Test _find_source_path raises KeyError when not found."""
    join_util.init(config)

    with pytest.raises(KeyError):
        join_util._find_source_path('nonexistent_collection',
                                    '12345678-1234-1234-1234-123456789abc')


def test_find_source_path_raises_when_disabled(config_disabled):
    """Test _find_source_path raises Exception when joins disabled."""
    join_util.init(config_disabled)

    with pytest.raises(Exception, match='disabled'):
        join_util._find_source_path('test_collection',
                                    '12345678-1234-1234-1234-123456789abc')
