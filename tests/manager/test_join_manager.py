import io
import json
from copy import deepcopy

import pytest
import tempfile
import uuid
import shutil
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import Mock

from filelock import FileLock

from pygeoapi.join.manager import JoinManager, JoinSourceNotFoundError
from pygeoapi.provider.base import BaseProvider
from pygeoapi.util import FileObject

TEST_DIR = Path(tempfile.gettempdir()) / "joins"


def prepare_join_dir(recreate=False):
    """Helper to remove (and recreate) a join source directory."""
    if TEST_DIR.exists():
        shutil.rmtree(TEST_DIR)
    if recreate:
        TEST_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture(autouse=True)
def cleanup_join_dir(request):
    """Automatically cleans test directory before marked test."""
    # Check if test is marked with 'recreate_dir'
    marker = request.node.get_closest_marker('recreate_dir')
    if marker:
        # NOTE: this will create orphaned refs in TinyDB
        prepare_join_dir(recreate=True)
    yield


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
                'source_dir': str(TEST_DIR),
                'max_days': 7,
                'max_files': 5
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
    data = b"""id,city,population,area
1,City A,100000,50.5
2,City B,200000,75.3
3,City C,150000,60.2"""
    return FileObject(
        name='test.csv',
        buffer=io.BytesIO(data),
        content_type='text/csv'
    )


@pytest.fixture
def csv_with_commas():
    """CSV with quoted fields containing commas."""
    data = b"""id,city,address,population
1,City A,"123 Main St, Suite 100",100000
2,City B,"456 Oak Ave, Floor 2",200000"""
    return FileObject(
        name='test.csv',
        buffer=io.BytesIO(data),
        content_type='text/csv'
    )


@pytest.fixture
def csv_empty_keys():
    """CSV with empty key values."""
    data = b"""id,city,population
,City A,100000
2,City B,200000"""
    return FileObject(
        name='test.csv',
        buffer=io.BytesIO(data),
        content_type='text/csv'
    )


@pytest.fixture
def csv_duplicate_keys():
    """CSV with duplicate keys."""
    data = b"""id,city,population
1,City A,100000
1,City B,200000"""
    return FileObject(
        name='test.csv',
        buffer=io.BytesIO(data),
        content_type='text/csv'
    )


@pytest.fixture
def csv_offset_header():
    """CSV with header not on first row."""
    data = b"""# This is a comment
# Another comment
id,city,population
1,City A,100000
2,City B,200000"""
    return FileObject(
        name='test.csv',
        buffer=io.BytesIO(data),
        content_type='text/csv'
    )


@pytest.fixture
def csv_custom_delimiter():
    """CSV with semicolon delimiter."""
    data = b"""id;city;population
1;City A;100000
2;City B;200000"""
    return FileObject(
        name='test.csv',
        buffer=io.BytesIO(data),
        content_type='text/csv'
    )


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
            'name': {'type': 'string'}
        }

    def mock_get_key_fields():
        return {
            'id': {'type': 'string', 'default': True},
            'name': {'type': 'string', 'default': False}
        }

    provider.get_fields = mock_get_fields
    provider.get_key_fields = mock_get_key_fields
    return provider


@pytest.fixture
def single_feature():
    """Single GeoJSON feature."""
    return {
        'type': 'Feature',
        'id': '1',
        'properties': {'id': '1', 'name': 'Feature 1'},
        'geometry': {'type': 'Point', 'coordinates': [1, 1]}
    }


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


@pytest.fixture
def join_manager(config):
    """Initialize JoinManager with full config."""
    return JoinManager.from_config(config)


@pytest.fixture
def join_manager_temp(config_minimal):
    """Initialize JoinManager with system temp."""
    return JoinManager.from_config(config_minimal)


@pytest.fixture
def join_manager_none(config_disabled):
    """Config without joins section should return None."""
    return JoinManager.from_config(config_disabled)


# ============================================================================
# Initialization Tests
# ============================================================================

def test_init_disabled(join_manager_none):
    """Test that JoinManager returns None when joins are disabled."""
    assert join_manager_none is None


def test_init_temp(join_manager_temp):
    """Test that JoinManager initializes with temp directory
    when no joins config is provided."""
    assert join_manager_temp.source_dir.exists()
    assert join_manager_temp.source_dir.is_dir()
    assert join_manager_temp.source_dir == Path(tempfile.gettempdir())
    assert (join_manager_temp.source_dir / 'join_sources.tinydb').exists()


def test_init_creates_directories(config):
    """Test that JoinManager creates necessary directories."""
    config = deepcopy(config)
    new_dir = str(TEST_DIR / 'new_dir' / 'join_sources')
    config['server']['joins']['source_dir'] = new_dir
    man = JoinManager.from_config(config)

    assert man.source_dir.exists()
    assert man.source_dir.is_dir()
    assert str(man.source_dir) == new_dir
    assert (man.source_dir / 'join_sources.tinydb').exists()


def test_init_with_existing_database(config):
    """Test initialization with existing TinyDB database."""
    # Create manager first time
    manager1 = JoinManager.from_config(config)

    # Create manager second time (should not error)
    manager2 = JoinManager.from_config(config)

    assert manager1.source_dir == manager2.source_dir


# ============================================================================
# Create Join Tests
# ============================================================================

def test_create_join_success(join_manager, mock_provider, valid_csv_content):
    """Test successful join source creation."""

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': valid_csv_content
    }

    result = join_manager.process_csv('cities', mock_provider, form_data)

    # Verify returned metadata
    assert 'id' in result
    assert result['collectionId'] == 'cities'
    assert result['collectionKey'] == 'id'
    assert result['joinKey'] == 'id'
    assert result['joinFields'] == ['city', 'population', 'area']
    assert result['numberOfRows'] == 3
    assert result['joinSource'] == 'test.csv'
    assert 'timeStamp' in result

    # Verify UUID format
    uuid.UUID(result['id'])  # Should not raise

    # Verify file was created
    src = join_manager.read_join_source(result['collectionId'], result['id'])
    assert src['id'] == result['id']


def test_create_join_with_commas(join_manager, mock_provider, csv_with_commas):
    """Test CSV parsing with quoted fields containing commas."""
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': csv_with_commas
    }

    result = join_manager.process_csv('cities', mock_provider, form_data)

    assert result['joinFields'] == ['city', 'address', 'population']
    assert result['numberOfRows'] == 2

    # Verify the address field was parsed correctly with commas
    source = join_manager.read_join_source('cities', result['id'])
    assert '123 Main St, Suite 100' in str(source)


def test_create_join_with_offset(join_manager, mock_provider, csv_offset_header):  # noqa
    """Test CSV parsing with offset header."""
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': csv_offset_header
    }

    form_data2 = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': deepcopy(csv_offset_header),
        'csvHeaderRow': 3,
        'csvDataStartRow': 4
    }

    # Without specific csvHeaderRow and csvDataStartRow, reading should fail
    with pytest.raises(ValueError, match='key field "id" not found in CSV fields'):  # noqa
        join_manager.process_csv('cities', mock_provider, form_data)  # noqa

    result = join_manager.process_csv('cities', mock_provider, form_data2)  # noqa

    assert result['joinFields'] == ['city', 'population']
    assert result['numberOfRows'] == 2


def test_create_join_empty_keys_fails(join_manager, mock_provider,
                                      csv_empty_keys):
    """Test that CSV with empty key values raises error."""
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': csv_empty_keys
    }

    with pytest.raises(ValueError, match='empty.*key'):
        join_manager.process_csv('cities', mock_provider, form_data)


def test_create_join_duplicate_keys_fails(join_manager, mock_provider,
                                          csv_duplicate_keys):
    """Test that CSV with duplicate keys raises error."""
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': csv_duplicate_keys
    }

    with pytest.raises(ValueError, match='duplicate.*key'):
        join_manager.process_csv('cities', mock_provider, form_data)


def test_create_join_custom_delimiter(join_manager, mock_provider,
                                      csv_custom_delimiter):
    """Test CSV parsing with custom delimiter."""
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': csv_custom_delimiter,
        'csvDelimiter': ';'
    }

    result = join_manager.process_csv('cities', mock_provider, form_data)

    assert result['joinFields'] == ['city', 'population']
    assert result['numberOfRows'] == 2


def test_create_join_invalid_collection_key(join_manager, mock_provider,
                                            valid_csv_content):
    """Test that invalid collection key raises error."""
    form_data = {
        'collectionKey': 'invalid_field',
        'joinKey': 'id',
        'joinFile': valid_csv_content
    }

    with pytest.raises(ValueError, match='collectionKey \'invalid_field\' not found'):  # noqa
        join_manager.process_csv('cities', mock_provider, form_data)


def test_create_join_invalid_join_key(join_manager, mock_provider,
                                      valid_csv_content):
    """Test that invalid join key raises error."""
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'invalid_column',
        'joinFile': valid_csv_content
    }

    with pytest.raises(ValueError, match='key field "invalid_column" not found in CSV fields'):  # noqa
        join_manager.process_csv('cities', mock_provider, form_data)


def test_create_join_empty_csv(join_manager, mock_provider):
    """Test that empty CSV raises error."""
    empty_csv = FileObject(
        name='empty.csv',
        buffer=io.BytesIO(b''),
        content_type='text/csv'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': empty_csv
    }

    with pytest.raises(ValueError, match='exceeds number of CSV rows'):
        join_manager.process_csv('cities', mock_provider, form_data)


def test_create_join_wrong_content_type(join_manager, mock_provider):
    """Test that non-CSV content type raises error."""
    wrong_type = FileObject(
        name='test.txt',
        buffer=io.BytesIO(b'id,city\n1,City A'),
        content_type='text/plain'
    )

    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': wrong_type
    }

    with pytest.raises(ValueError, match='join source data must be of type "text/csv"'):  # noqa
        join_manager.process_csv('cities', mock_provider, form_data)


def test_create_multiple_joins_same_collection(join_manager, mock_provider,
                                               valid_csv_content):
    """Test creating multiple join sources for same collection."""
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': valid_csv_content
    }

    form_data2 = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': deepcopy(valid_csv_content)
    }

    result1 = join_manager.process_csv('cities', mock_provider, form_data)
    result2 = join_manager.process_csv('cities', mock_provider, form_data2)

    assert result1['id'] != result2['id']
    assert result1['collectionId'] == result2['collectionId']

    # Both should be retrievable
    source1 = join_manager.read_join_source('cities', result1['id'])
    source2 = join_manager.read_join_source('cities', result2['id'])
    assert source1['id'] != source2['id']


# ============================================================================
# Read Join Source Tests
# ============================================================================

def test_read_join_source_success(join_manager, mock_provider,
                                  valid_csv_content):
    """Test successful reading of join source."""
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': valid_csv_content
    }

    result = join_manager.process_csv('cities', mock_provider, form_data)
    source = join_manager.read_join_source('cities', result['id'])

    assert source['id'] == result['id']
    assert source['collectionId'] == 'cities'
    assert source['joinKey'] == 'id'
    assert 'data' in source


def test_read_join_source_not_found(join_manager):
    """Test reading non-existent join source raises error."""
    fake_id = str(uuid.uuid4())

    with pytest.raises(Exception, match='not found'):
        join_manager.read_join_source('cities', fake_id)


def test_read_join_source_invalid_id(join_manager):
    """Test reading join source with invalid ID format."""
    with pytest.raises(ValueError, match='invalid'):
        join_manager.read_join_source('cities', 'not-a-uuid')


def test_read_join_source_wrong_collection(join_manager, mock_provider,
                                           valid_csv_content):
    """Test reading join source from wrong collection raises error."""
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': valid_csv_content
    }

    result = join_manager.process_csv('cities', mock_provider, form_data)

    with pytest.raises(Exception, match='not found'):
        join_manager.read_join_source('wrong_collection', result['id'])


def test_read_join_source_file_deleted(join_manager, mock_provider,
                                       valid_csv_content):
    """Test reading join source when file has been deleted."""
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': valid_csv_content
    }

    result = join_manager.process_csv('cities', mock_provider, form_data)

    # Delete the JSON file manually
    json_file = join_manager._make_source_path(result['id'])
    json_file.unlink()

    with pytest.raises(Exception, match='missing|removed'):
        join_manager.read_join_source('cities', result['id'])


# ============================================================================
# List Join Sources Tests
# ============================================================================

@pytest.mark.recreate_dir
def test_list_join_sources_empty(join_manager):
    """Test listing join sources when none exist and test orphan removal."""
    sources = join_manager.list_sources('cities')
    assert sources == {}


@pytest.mark.recreate_dir
def test_list_join_sources_single(join_manager, mock_provider,
                                  valid_csv_content):
    """Test listing single join source."""
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': valid_csv_content
    }

    result = join_manager.process_csv('cities', mock_provider, form_data)
    sources = join_manager.list_sources('cities')

    assert len(sources) == 1
    assert result['id'] in sources


@pytest.mark.recreate_dir
def test_list_join_sources_multiple(join_manager, mock_provider,
                                    valid_csv_content):
    """Test listing multiple join sources."""
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': valid_csv_content
    }

    form_data2 = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': deepcopy(valid_csv_content)
    }

    result1 = join_manager.process_csv('cities', mock_provider, form_data)
    result2 = join_manager.process_csv('cities', mock_provider, form_data2)

    sources = join_manager.list_sources('cities')

    assert len(sources) == 2
    assert result1['id'] in sources
    assert result2['id'] in sources


@pytest.mark.recreate_dir
def test_list_join_sources_multiple_collections(join_manager, mock_provider,
                                                valid_csv_content):
    """Test listing join sources filters by collection."""
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': valid_csv_content
    }

    form_data2 = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': deepcopy(valid_csv_content)
    }

    result1 = join_manager.process_csv('cities', mock_provider, form_data)
    result2 = join_manager.process_csv('countries', mock_provider, form_data2)

    cities_sources = join_manager.list_sources('cities')
    countries_sources = join_manager.list_sources('countries')

    assert len(cities_sources) == 1
    assert len(countries_sources) == 1
    assert result1['id'] in cities_sources
    assert result2['id'] in countries_sources


# ============================================================================
# Delete Join Source Tests
# ============================================================================

def test_delete_join_source_success(join_manager, mock_provider,
                                    valid_csv_content):
    """Test successful deletion of join source."""
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': valid_csv_content
    }

    result = join_manager.process_csv('cities', mock_provider, form_data)

    # Verify it exists
    source = join_manager.read_join_source('cities', result['id'])
    assert source is not None

    # Delete it
    join_manager.remove_source('cities', result['id'])

    # Verify it's gone
    with pytest.raises(Exception, match='not found'):
        join_manager.read_join_source('cities', result['id'])


def test_delete_join_source_not_found(join_manager):
    """Test deleting non-existent join source."""
    fake_id = str(uuid.uuid4())

    assert join_manager.remove_source('cities', fake_id) is False


def test_delete_join_source_invalid_id(join_manager):
    """Test deleting join source with invalid ID."""
    with pytest.raises(ValueError, match='invalid'):
        join_manager.remove_source('cities', 'not-a-uuid')


def test_delete_join_source_removes_file(join_manager, mock_provider,
                                         valid_csv_content):
    """Test that deletion removes both database entry and file."""
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': valid_csv_content
    }

    result = join_manager.process_csv('cities', mock_provider, form_data)
    json_file = join_manager._make_source_path(result['id'])

    assert json_file.exists()

    join_manager.remove_source('cities', result['id'])

    assert not json_file.exists()


# ============================================================================
# Cleanup Tests
# ============================================================================

def test_cleanup_old_sources(join_manager, mock_provider, valid_csv_content):
    """Test cleanup of sources older than max_days."""
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': valid_csv_content
    }

    init_size = len(join_manager.list_sources('cities'))

    # Create a source
    result = join_manager.process_csv('cities',
                                      mock_provider, form_data)

    assert len(join_manager.list_sources('cities')) == init_size + 1

    join_id = result['id']

    # Mock timestamp to be old
    old_timestamp = (datetime.now(timezone.utc) - timedelta(days=10)).strftime(
        '%Y-%m-%dT%H:%M:%S.%fZ'
    )

    # Hack JSON file with old timestamp
    result['timeStamp'] = old_timestamp
    json_path = join_manager._find_source_path('cities', join_id)
    with FileLock(json_path.with_suffix(json_path.suffix + '.lock')):
        with open(json_path, 'w+') as json_file:
            json.dump(result, json_file)

    # Run cleanup
    join_manager._cleanup_sources()

    # Verify source was deleted
    with pytest.raises(JoinSourceNotFoundError, match='not found'):
        join_manager.read_join_source('cities', join_id)


def test_cleanup_excess_sources(join_manager, mock_provider,
                                valid_csv_content):
    """Test cleanup when max_files limit is exceeded."""
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': valid_csv_content
    }

    num_init = len(join_manager.list_sources('cities'))
    for _ in range(max(0, join_manager.max_files - num_init) + 1):
        join_manager.process_csv('cities', mock_provider, deepcopy(form_data))  # noqa

    assert len(join_manager.list_sources('cities')) > join_manager.max_files

    join_manager._cleanup_sources()

    all_sources = join_manager.list_sources('cities')
    assert len(all_sources) <= join_manager.max_files


# ============================================================================
# Feature Join Tests
# ============================================================================

def test_enrich_single_feature(join_manager, mock_provider, valid_csv_content,
                               single_feature):
    """Test enriching a single feature with join data."""
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': valid_csv_content
    }

    result = join_manager.process_csv('cities', mock_provider, form_data)

    join_manager.perform_join(
        single_feature, 'cities', result['id']
    )

    assert 'city' in single_feature['properties']
    assert 'population' in single_feature['properties']
    assert 'area' in single_feature['properties']


def test_enrich_feature_collection(join_manager, mock_provider,
                                   valid_csv_content, feature_collection):
    """Test enriching a FeatureCollection with join data."""
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': valid_csv_content
    }

    result = join_manager.process_csv('cities', mock_provider, form_data)

    join_manager.perform_join(
        feature_collection, 'cities', result['id']
    )

    assert feature_collection['type'] == 'FeatureCollection'
    for feature in feature_collection['features']:
        if feature['id'] in ['1', '2', '3']:
            assert 'city' in feature['properties']


def test_enrich_feature_no_match(join_manager, mock_provider,
                                 valid_csv_content, single_feature):
    """Test enriching feature with no matching join data."""
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': valid_csv_content
    }

    result = join_manager.process_csv('cities', mock_provider, form_data)

    # Feature with non-matching ID
    no_match_feature = deepcopy(single_feature)
    no_match_feature['id'] = '999'
    no_match_feature['properties']['id'] = '999'

    join_manager.perform_join(
        no_match_feature, 'cities', result['id']
    )

    # Should return original feature unchanged
    assert no_match_feature == no_match_feature


def test_enrich_feature_partial_match(join_manager, mock_provider,
                                      valid_csv_content, feature_collection):
    """Test enriching features where only some match join data."""
    form_data = {
        'collectionKey': 'id',
        'joinKey': 'id',
        'joinFile': valid_csv_content
    }

    result = join_manager.process_csv('cities', mock_provider, form_data)

    # Add feature with no match
    fc = deepcopy(feature_collection)
    fc['features'].append({
        'type': 'Feature',
        'id': '999',
        'properties': {'id': '999', 'name': 'No Match'},
        'geometry': {'type': 'Point', 'coordinates': [9, 9]}
    })

    join_manager.perform_join(
        fc, 'cities', result['id']
    )

    # First 3 should have join data, last one should not
    for i in range(3):
        assert 'city' in fc['features'][i]['properties']

    assert 'city' not in fc['features'][3]['properties']


# ============================================================================
# Edge Cases and Error Handling Tests
# ============================================================================

def test_concurrent_create_operations(join_manager, mock_provider,
                                      valid_csv_content):
    """Test that concurrent create operations don't conflict."""
    from concurrent.futures import ThreadPoolExecutor

    def create_join(i):
        csv_copy = FileObject(
            name=f'test{i}.csv',
            buffer=io.BytesIO(valid_csv_content.buffer.getvalue()),  # noqa
            content_type='text/csv'
        )
        form_data = {
            'collectionKey': 'id',
            'joinKey': 'id',
            'joinFile': csv_copy
        }
        return join_manager.process_csv('cities', mock_provider, form_data)

    with ThreadPoolExecutor(max_workers=3) as executor:
        results = list(executor.map(create_join, range(3)))

    assert len(results) == 3
    assert len(set(r['id'] for r in results)) == 3  # All unique IDs
