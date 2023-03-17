# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
# Authors: Francesco Bartoli <xbartolone@gmail.com>
#
# Copyright (c) 2022 Tom Kralidis
# Copyright (c) 2022 Francesco Bartoli
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

from copy import deepcopy
import io
import json
import logging
import os
from pathlib import Path
from typing import Union, Any
from collections import OrderedDict
from urllib.parse import urlparse, urlunparse, urljoin

import click
from jsonschema import validate as jsonschema_validate
from requests import Session
import yaml

from pygeoapi import l10n
from pygeoapi.plugin import load_plugin
from pygeoapi.provider.base import ProviderTypeError, SchemaType
from pygeoapi.util import (filter_dict_by_key_value, get_provider_by_type,
                           filter_providers_by_type, to_json, yaml_load,
                           get_api_rules, get_base_url, url_join)

LOGGER = logging.getLogger(__name__)

OPENAPI_YAML = {
    'oapif': 'https://schemas.opengis.net/ogcapi/features/part1/1.0/openapi/ogcapi-features-1.yaml',  # noqa
    'oapip': 'https://schemas.opengis.net/ogcapi/processes/part1/1.0/openapi',
    'oacov': 'https://raw.githubusercontent.com/tomkralidis/ogcapi-coverages-1/fix-cis/yaml-unresolved',  # noqa
    'oapit': 'https://schemas.opengis.net/ogcapi/tiles/part1/1.0/openapi/ogcapi-tiles-1.yaml',  # noqa
    'oapir': 'https://raw.githubusercontent.com/opengeospatial/ogcapi-records/master/core/openapi',  # noqa
    'oaedr': 'https://schemas.opengis.net/ogcapi/edr/1.0/openapi' # noqa
}

THISDIR = os.path.dirname(os.path.realpath(__file__))


class Referencer:
    JSON_MIME_TYPE = 'application/json'
    YAML_MIME_TYPE = 'application/x-yaml'

    def __init__(self, components: dict):
        self.namespaces = {
            'common': 'https://schemas.opengis.net/ogcapi/common/part1/1.0/openapi/3.0/API-Common-Part-1_1_0.yaml',  # noqa
            'oapif-1': 'https://schemas.opengis.net/ogcapi/features/part1/1.0/openapi/ogcapi-features-1.yaml',  # noqa
            'oapif-2': 'https://schemas.opengis.net/ogcapi/features/part2/1.0/openapi/ogcapi-features-2.yaml', # noqa
            'oapip': 'https://schemas.opengis.net/ogcapi/processes/part1/1.0/openapi',  # noqa
            'oacov': 'https://raw.githubusercontent.com/tomkralidis/ogcapi-coverages-1/fix-cis/yaml-unresolved',  # noqa
            'oapit': 'https://schemas.opengis.net/ogcapi/tiles/part1/1.0/openapi/ogcapi-tiles-1.yaml',  # noqa
            'oapir': 'https://raw.githubusercontent.com/opengeospatial/ogcapi-records/master/core/openapi',  # noqa
            'oaedr': 'https://schemas.opengis.net/ogcapi/edr/1.0/openapi' # noqa
        }
        self.components = components
        self._session = Session()
        self._schemas = {}

    def _import_ref(self, path: str, schema: dict) -> Any:
        """ Imports (embeds) a schema object from a reference path. """
        assert path.startswith('#/components/')
        path_keys = path[13:].split('/')
        imported_parent = self.components
        external_parent = schema.get('components', {})
        assert isinstance(imported_parent, dict)
        assert isinstance(external_parent, dict)
        for i, k in enumerate(path_keys):
            imported_item = imported_parent.get(k, {})
            external_item = external_parent.get(k, {})
            if i < len(path_keys) - 1:
                # Traverse path
                assert isinstance(imported_item, dict)
                assert isinstance(external_item, dict)
                imported_parent = imported_item
                external_parent = external_item
                continue
            # We've reached the end of the component path
            if not imported_item:
                if external_item:
                    # Embed external component reference
                    imported_item = external_item
                    imported_parent[k] = imported_item
                else:
                    # External or local component not found
                    raise KeyError(f'{path} not found in schema')
            return imported_item

    @staticmethod
    def _parent_url(url: str):
        """ Returns the parent URL for the given URL.
        If there is no URL path, the root (scheme://netloc) is returned.
        Returned URLs will always have a trailing slash.
        """
        url_obj = urlparse(url)
        if url_obj.path in ('', '/'):
            # There is no path: return URL as-is
            return f"{url.rstrip('/')}/"
        path = f"/{'/'.join(url_obj.path.lstrip('/').split('/')[:-1])}"
        new_url_obj = (
            url_obj.scheme,
            url_obj.netloc,
            path,
            url_obj.params,
            url_obj.query,
            url_obj.fragment
        )
        return f"{urlunparse(new_url_obj).rstrip('/')}/"

    def _find_refs(self, obj: Any, schema: dict, visited=None):
        """ Finds and imports more references in the given object. """
        visited = visited or set()
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == "$ref" and isinstance(v, str) and v not in visited:
                    try:
                        found_ref = self._import_ref(v, schema)
                        visited.add(v)
                        self._find_refs(found_ref, schema, visited)
                    except (KeyError, AssertionError):
                        # Be silent about errors here
                        continue
                else:
                    self._find_refs(v, schema, visited)
        elif isinstance(obj, list):
            for o in obj:
                self._find_refs(o, schema, visited)

    def _test_url(self, url: str):
        """ Issues HEAD on the given URL to test that it's valid. """
        return self._session.head(url).raise_for_status()

    def _load_schema(self, url: str) -> dict:
        """ Loads schema URL as a Python dictionary and returns it. """
        response = self._session.get(url)
        response.raise_for_status()
        schema = None
        content_type = response.headers.get('Content-Type', '')
        if url.endswith('.json') or content_type == 'application/json':
            schema = response.json()
        elif url.endswith(('.yaml', '.yml')) or 'yaml' in content_type:
            schema = yaml_load(response.content)  # noqa
        if not schema:
            raise ValueError(f'No schema found at {url}')
        return schema

    def _resolve_ref_urls(self, schema_obj: dict, schema_base_url: str):
        """ Resolves relative URLs in schema references. """
        if isinstance(schema_obj, dict):
            for k, v in schema_obj.items():
                if k == "$ref" and isinstance(v, str):
                    if v.startswith('http') or not v.endswith(('.yaml', '.yml', '.json')):  # noqa
                        continue
                    schema_url = urljoin(schema_base_url, v)
                    try:
                        # Test that the URL is valid
                        self._test_url(schema_url)
                    except Exception as err:
                        LOGGER.debug(f"Failed to resolve {schema_url}: {err}")
                    else:
                        schema_obj[k] = schema_url
                    continue
                self._resolve_ref_urls(v, schema_base_url)
        elif isinstance(schema_obj, list):
            for o in schema_obj:
                self._resolve_ref_urls(o, schema_base_url)

    def create_ref_object(self, path: str, namespace_id: str = None) -> dict:
        """ Returns a $ref object for the given path and namespace.
        If namespace_id is None, predefined components (set at initialization)
        will be searched. Otherwise, the schema for the given namespace_id
        will be loaded and scanned for references. Any component matches will
        be imported (embedded).
        """
        schema = {}
        schema_url = None
        if namespace_id is not None:
            schema_url = self.namespaces.get(namespace_id)
            import_schema = True
            if not schema_url:
                raise KeyError(
                    f"Namespace URI for key '{namespace_id}' not found"
                )
            if path.endswith(('.yaml', '.yml', '.json')):
                schema_url = url_join(schema_url, path)
                import_schema = False

            # Get cached schema (if we parsed it before)
            schema = self._schemas.get(schema_url)

            if not import_schema:
                # Reference full schema URL instead of locally

                if schema is None:
                    # Test that the URL is valid
                    self._test_url(schema_url)

                # Return reference object to schema URL
                return {
                    '$ref': schema_url
                }

            # Import the schema object and reference locally
            if schema is None:
                # Load from URL
                schema = self._load_schema(schema_url)

                # Complete schema by resolving child reference URLs
                schema_base_url = self._parent_url(schema_url)
                self._resolve_ref_urls(schema, schema_base_url)

                # Cache schema
                self._schemas[schema_url] = schema

        # Embed schema component and return reference
        try:
            found_obj = self._import_ref(path, schema)
        except KeyError as err:
            # External or local component not found
            if schema_url:
                err.args = (f'{path} not found in {schema_url}',)
            else:
                err.args = (f'Local {path} not found',)
            raise

        # Try and find any child references that should also be imported
        self._find_refs(found_obj, schema)

        # Return component reference object
        return {
            "$ref": path
        }


def get_ogc_schemas_location(server_config):

    osl = server_config.get('ogc_schemas_location')

    value = 'https://schemas.opengis.net'

    if osl is not None:
        if osl.startswith('http'):
            value = osl
        elif osl.startswith('/'):
            value = os.path.join(server_config['url'], 'schemas')

    return value


def get_oas_30(cfg):
    """
    Generates an OpenAPI 3.0 Document

    :param cfg: configuration object

    :returns: OpenAPI definition YAML dict
    """

    # TODO: make openapi multilingual (default language only for now)
    server_locales = l10n.get_locales(cfg)
    locale_ = server_locales[0]

    api_rules = get_api_rules(cfg)

    # osl = get_ogc_schemas_location(cfg['server'])
    # OPENAPI_YAML['oapif-1'] = os.path.join(osl, 'ogcapi/features/part1/1.0/openapi/ogcapi-features-1.yaml')  # noqa
    # OPENAPI_YAML['oapif-2'] = os.path.join(osl, 'ogcapi/features/part2/1.0/openapi/ogcapi-features-2.yaml') # noqa

    LOGGER.debug('setting up server info')
    oas = OrderedDict({
        'openapi': '3.0.2',
        'info': {
            'title': l10n.translate(cfg['metadata']['identification']['title'], locale_),  # noqa
            'description': l10n.translate(cfg['metadata']['identification']['description'], locale_),  # noqa
            'x-keywords': l10n.translate(cfg['metadata']['identification']['keywords'], locale_),  # noqa
            'termsOfService':
                cfg['metadata']['identification']['terms_of_service'],
            'contact': {
                'name': cfg['metadata']['provider']['name'],
                'url': cfg['metadata']['provider']['url'],
                'email': cfg['metadata']['contact']['email']
            },
            'license': {
                'name': cfg['metadata']['license']['name'],
                'url': cfg['metadata']['license']['url']
            },
            'version': api_rules.api_version
        },
        'components': {
            'responses': {
                'Queryables': {
                    'description': 'successful queryables operation',
                    'content': {
                        'application/json': {
                            'schema': {
                                '$ref': '#/components/schemas/queryables'
                            }
                        }
                    }
                }
            },
            'parameters': {
                'f': {
                    'name': 'f',
                    'in': 'query',
                    'description': 'The optional f parameter indicates the output format which the server shall provide as part of the response document.  The default format is GeoJSON.',  # noqa
                    'required': False,
                    'schema': {
                        'type': 'string',
                        'enum': ['json', 'html', 'jsonld'],
                        'default': 'json'
                    },
                    'style': 'form',
                    'explode': False
                },
                'lang': {
                    'name': 'lang',
                    'in': 'query',
                    'description': 'The optional lang parameter instructs the server return a response in a certain language, if supported.  If the language is not among the available values, the Accept-Language header language will be used if it is supported. If the header is missing, the default server language is used. Note that providers may only support a single language (or often no language at all), that can be different from the server language.  Language strings can be written in a complex (e.g. "fr-CA,fr;q=0.9,en-US;q=0.8,en;q=0.7"), simple (e.g. "de") or locale-like (e.g. "de-CH" or "fr_BE") fashion.',  # noqa
                    'required': False,
                    'schema': {
                        'type': 'string',
                        'enum': [l10n.locale2str(sl) for sl in server_locales],
                        'default': l10n.locale2str(locale_)
                    }
                },
                'properties': {
                    'name': 'properties',
                    'in': 'query',
                    'description': 'The properties that should be included for each feature. The parameter value is a comma-separated list of property names.',  # noqa
                    'required': False,
                    'style': 'form',
                    'explode': False,
                    'schema': {
                        'type': 'array',
                        'items': {
                            'type': 'string'
                        }
                    }
                },
                'skipGeometry': {
                    'name': 'skipGeometry',
                    'in': 'query',
                    'description': 'This option can be used to skip response geometries for each feature.',  # noqa
                    'required': False,
                    'style': 'form',
                    'explode': False,
                    'schema': {
                        'type': 'boolean',
                        'default': False
                    }
                },
                # 'bbox-crs': {
                #     'name': 'bbox-crs',
                #     'in': 'query',
                #     'description': 'Indicates the EPSG for the given bbox coordinates.',  # noqa
                #     'required': False,
                #     'style': 'form',
                #     'explode': False,
                #     'schema': {
                #         'type': 'integer',
                #         'default': 4326
                #     }
                # },
                'offset': {
                    'name': 'offset',
                    'in': 'query',
                    'description': 'The optional offset parameter indicates the index within the result set from which the server shall begin presenting results in the response document.  The first element has an index of 0 (default).',  # noqa
                    'required': False,
                    'schema': {
                        'type': 'integer',
                        'minimum': 0,
                        'default': 0
                    },
                    'style': 'form',
                    'explode': False
                },
                'vendorSpecificParameters': {
                    'name': 'vendorSpecificParameters',
                    'in': 'query',
                    'description': 'Additional "free-form" parameters that are not explicitly defined',  # noqa
                    'schema': {
                        'type': 'object',
                        'additionalProperties': True
                    },
                    'style': 'form'
                }
            },
            'schemas': {
                # TODO: change schema once OGC will definitively publish it
                'queryable': {
                    'type': 'object',
                    'required': [
                        'queryable',
                        'type'
                    ],
                    'properties': {
                        'queryable': {
                            'description': 'the token that may be used in a CQL predicate',  # noqa
                            'type': 'string'
                        },
                        'title': {
                            'description': 'a human readable title for the queryable',  # noqa
                            'type': 'string'
                        },
                        'description': {
                            'description': 'a human-readable narrative describing the queryable',  # noqa
                            'type': 'string'
                        },
                        'language': {
                            'description': 'the language used for the title and description',  # noqa
                            'type': 'string',
                            'default': [
                                'en'
                            ]
                        },
                        'type': {
                            'description': 'the data type of the queryable',
                            'type': 'string'
                        },
                        'type-ref': {
                            'description': 'a reference to the formal definition of the type',  # noqa
                            'type': 'string',
                            'format': 'url'
                        }
                    }
                },
                'queryables': {
                    'type': 'object',
                    'required': [
                        'queryables'
                    ],
                    'properties': {
                        'queryables': {
                            'type': 'array',
                            'items': {
                                '$ref': '#/components/schemas/queryable'
                            }
                        }
                    }
                }
            }
        },
        'servers': [{
            'url': get_base_url(cfg),
            'description': l10n.translate(cfg['metadata']['identification']['description'], locale_)  # noqa
        }]
    })

    referencer = Referencer(oas['components'])

    paths = OrderedDict()
    paths['/'] = {
        'get': {
            'summary': 'Landing page',
            'description': 'Landing page',
            'tags': ['server'],
            'operationId': 'getLandingPage',
            'parameters': [
                referencer.create_ref_object('#/components/parameters/f'),
                referencer.create_ref_object('#/components/parameters/lang')
            ],
            'responses': {
                '200': referencer.create_ref_object('#/components/responses/LandingPage', 'common'),  # noqa
                '400': referencer.create_ref_object('#/components/responses/400', 'common'),  # noqa
                '500': referencer.create_ref_object('#/components/responses/500', 'common')  # noqa
            }
        }
    }

    paths['/openapi'] = {
        'get': {
            'summary': 'This document',
            'description': 'This document',
            'tags': ['server'],
            'operationId': 'getOpenapi',
            'parameters': [
                referencer.create_ref_object('#/components/parameters/f'),
                referencer.create_ref_object('#/components/parameters/lang'),
                {
                    'name': 'ui',
                    'in': 'query',
                    'description': 'UI to render the OpenAPI document',
                    'required': False,
                    'schema': {
                        'type': 'string',
                        'enum': ['swagger', 'redoc'],
                        'default': 'swagger'
                    },
                    'style': 'form',
                    'explode': False
                },
            ],
            'responses': {
                '200': referencer.create_ref_object('#/components/responses/200', 'common'),  # noqa
                '400': referencer.create_ref_object('#/components/responses/400', 'common'),  # noqa
                '500': referencer.create_ref_object('#/components/responses/500', 'common')  # noqa
            }
        }
    }

    paths['/conformance'] = {
        'get': {
            'summary': 'API conformance definition',
            'description': 'API conformance definition',
            'tags': ['server'],
            'operationId': 'getConformanceDeclaration',
            'parameters': [
                referencer.create_ref_object('#/components/parameters/f'),
                referencer.create_ref_object('#/components/parameters/lang')
            ],
            'responses': {
                '200': referencer.create_ref_object('#/components/responses/ConformanceDeclaration', 'common'),  # noqa
                '400': referencer.create_ref_object('#/components/responses/400', 'common'),  # noqa
                '500': referencer.create_ref_object('#/components/responses/500', 'common')  # noqa
            }
        }
    }

    paths['/collections'] = {
        'get': {
            'summary': 'Collections',
            'description': 'Collections',
            'tags': ['server'],
            'operationId': 'getCollections',
            'parameters': [
                referencer.create_ref_object('#/components/parameters/f'),
                referencer.create_ref_object('#/components/parameters/lang')
            ],
            'responses': {
                '200': referencer.create_ref_object('#/components/responses/Collections', 'oapif-1'),  # noqa
                '400': referencer.create_ref_object('#/components/responses/InvalidParameter', 'oapif-1'),  # noqa
                '500': referencer.create_ref_object('#/components/responses/ServerError', 'oapif-1')  # noqa
            }
        }
    }

    oas['tags'] = [
        {
            'name': 'server',
            'description': l10n.translate(cfg['metadata']['identification']['description'], locale_),  # noqa
            'externalDocs': {
                'description': 'information',
                'url': cfg['metadata']['identification']['url']
            }
        },
        {
            'name': 'stac',
            'description': 'SpatioTemporal Asset Catalog'
        }
    ]

    items_f = deepcopy(referencer.components['parameters']['f'])
    items_f['schema']['enum'].append('csv')
    items_l = deepcopy(referencer.components['parameters']['lang'])

    LOGGER.debug('setting up datasets')
    collections = filter_dict_by_key_value(
        cfg['resources'], 'type', 'collection')

    for k, v in collections.items():
        if v.get('visibility', 'default') == 'hidden':
            LOGGER.debug(f'Skipping hidden layer: {k}')
            continue
        name = l10n.translate(k, locale_)
        title = l10n.translate(v['title'], locale_)
        desc = l10n.translate(v['description'], locale_)
        collection_name_path = f'/collections/{k}'
        tag = {
            'name': name,
            'description': desc,
            'externalDocs': {}
        }
        for link in l10n.translate(v['links'], locale_):
            if link['type'] == 'information':
                tag['externalDocs']['description'] = link['type']
                tag['externalDocs']['url'] = link['url']
                break
        if len(tag['externalDocs']) == 0:
            del tag['externalDocs']

        oas['tags'].append(tag)

        paths[collection_name_path] = {
            'get': {
                'summary': f'Get {title} metadata',
                'description': desc,
                'tags': [name],
                'operationId': f'describe{name.capitalize()}Collection',
                'parameters': [
                    referencer.create_ref_object('#/components/parameters/f'),
                    referencer.create_ref_object('#/components/parameters/lang')  # noqa
                ],
                'responses': {
                    '200': referencer.create_ref_object('#/components/responses/Collection', 'oapif-1'),  # noqa
                    '400': referencer.create_ref_object('#/components/responses/InvalidParameter', 'oapif-1'),  # noqa
                    '404': referencer.create_ref_object('#/components/responses/NotFound', 'oapif-1'),  # noqa
                    '500': referencer.create_ref_object('#/components/responses/ServerError', 'oapif-1')  # noqa
                }
            }
        }

        LOGGER.debug('setting up collection endpoints')
        try:
            ptype = None

            if filter_providers_by_type(
                    collections[k]['providers'], 'feature'):
                ptype = 'feature'

            if filter_providers_by_type(
                    collections[k]['providers'], 'record'):
                ptype = 'record'

            p = load_plugin('provider', get_provider_by_type(
                            collections[k]['providers'], ptype))

            items_path = f'{collection_name_path}/items'

            coll_properties = deepcopy(referencer.components['parameters']['properties'])  # noqa

            coll_properties['schema']['items']['enum'] = list(p.fields.keys())

            paths[items_path] = {
                'get': {
                    'summary': f'Get {title} items',
                    'description': desc,
                    'tags': [name],
                    'operationId': f'get{name.capitalize()}Features',
                    'parameters': [
                        items_f,
                        items_l,
                        referencer.create_ref_object('#/components/parameters/bbox', 'oapif-1'),  # noqa
                        referencer.create_ref_object('#/components/parameters/limit', 'oapif-1'),  # noqa
                        referencer.create_ref_object('#/components/parameters/crs', 'oapif-2'),  # noqa
                        referencer.create_ref_object('#/components/parameters/bbox-crs', 'oapif-2'),  # noqa
                        referencer.create_ref_object('#/components/parameters/datetime', 'oapif-1'),  # noqa
                        referencer.create_ref_object('#/components/parameters/vendorSpecificParameters'),  # noqa
                        referencer.create_ref_object('#/components/parameters/skipGeometry'),  # noqa
                        referencer.create_ref_object('/parameters/sortby.yaml', 'oapir'),  # noqa
                        referencer.create_ref_object('#/components/parameters/offset'),  # noqa
                        coll_properties,
                    ],
                    'responses': {
                        '200': referencer.create_ref_object('#/components/responses/Features', 'oapif-1'),  # noqa
                        '400': referencer.create_ref_object('#/components/responses/InvalidParameter', 'oapif-1'),  # noqa
                        '404': referencer.create_ref_object('#/components/responses/NotFound', 'oapif-1'),  # noqa
                        '500': referencer.create_ref_object('#/components/responses/ServerError', 'oapif-1')  # noqa
                    }
                }
            }

            if p.editable:
                LOGGER.debug('Provider is editable; adding post')

                paths[items_path]['post'] = {
                    'summary': f'Add {title} items',
                    'description': desc,
                    'tags': [name],
                    'operationId': f'add{name.capitalize()}Features',
                    'requestBody': {
                        'description': 'Adds item to collection',
                        'content': {
                            'application/geo+json': {
                                'schema': {}
                            }
                        },
                        'required': True
                    },
                    'responses': {
                        '201': {'description': 'Successful creation'},
                        '400': referencer.create_ref_object('#/components/responses/InvalidParameter', 'oapif-1'),  # noqa
                        '500': referencer.create_ref_object('#/components/responses/ServerError', 'oapif-1')  # noqa
                    }
                }

                try:
                    schema_ref = p.get_schema(SchemaType.create)
                    paths[items_path]['post']['requestBody']['content'][schema_ref[0]] = {  # noqa
                        'schema': schema_ref[1]
                    }
                except Exception as err:
                    LOGGER.debug(err)

            if ptype == 'record':
                paths[items_path]['get']['parameters'].append(
                    referencer.create_ref_object('/parameters/q.yaml', 'oapir')
                )

            if p.fields:
                queryables_path = f'{collection_name_path}/queryables'

                paths[queryables_path] = {
                    'get': {
                        'summary': f'Get {title} queryables',
                        'description': desc,
                        'tags': [name],
                        'operationId': f'get{name.capitalize()}Queryables',
                        'parameters': [
                            items_f,
                            items_l
                        ],
                        'responses': {
                            '200': referencer.create_ref_object('#/components/responses/Queryables'),  # noqa
                            '400': referencer.create_ref_object('#/components/responses/InvalidParameter', 'oapif-1'),  # noqa
                            '404': referencer.create_ref_object('#/components/responses/NotFound', 'oapif-1'),  # noqa
                            '500': referencer.create_ref_object('#/components/responses/ServerError', 'oapif-1')  # noqa
                        }
                    }
                }

            if p.time_field is not None:
                paths[items_path]['get']['parameters'].append(
                    referencer.create_ref_object('#/components/parameters/datetime', 'oapif-1')  # noqa
                )

            for field, type_ in p.fields.items():

                if p.properties and field not in p.properties:
                    LOGGER.debug('Provider specified not to advertise property')  # noqa
                    continue

                if field == 'q' and ptype == 'record':
                    LOGGER.debug('q parameter already declared, skipping')
                    continue

                if type_ == 'date':
                    schema = {
                        'type': 'string',
                        'format': 'date'
                    }
                elif type_ == 'float':
                    schema = {
                        'type': 'number',
                        'format': 'float'
                    }
                elif type_ == 'long':
                    schema = {
                        'type': 'integer',
                        'format': 'int64'
                    }
                else:
                    schema = type_

                path_ = f'{collection_name_path}/items'
                paths[path_]['get']['parameters'].append({
                    'name': field,
                    'in': 'query',
                    'required': False,
                    'schema': schema,
                    'style': 'form',
                    'explode': False
                })

            paths[f'{collection_name_path}/items/{{featureId}}'] = {
                'get': {
                    'summary': f'Get {title} item by id',
                    'description': desc,
                    'tags': [name],
                    'operationId': f'get{name.capitalize()}Feature',
                    'parameters': [
                        referencer.create_ref_object('#/components/parameters/f'),  # noqa
                        referencer.create_ref_object('#/components/parameters/lang'),  # noqa
                        referencer.create_ref_object('#/components/parameters/featureId', 'oapif-1'),  # noqa
                        referencer.create_ref_object('#/components/parameters/crs', 'oapif-2')  # noqa
                    ],
                    'responses': {
                        '200': referencer.create_ref_object('#/components/responses/Feature', 'oapif-1'),  # noqa
                        '400': referencer.create_ref_object('#/components/responses/InvalidParameter', 'oapif-1'),  # noqa
                        '404': referencer.create_ref_object('#/components/responses/NotFound', 'oapif-1'),  # noqa
                        '500': referencer.create_ref_object('#/components/responses/ServerError', 'oapif-1')  # noqa
                    }
                }
            }

            try:
                schema_ref = p.get_schema()
                paths[f'{collection_name_path}/items/{{featureId}}']['get']['responses']['200'] = {  # noqa
                    'content': {
                        schema_ref[0]: {
                            'schema': schema_ref[1]
                        }
                    }
                }
            except Exception as err:
                LOGGER.debug(err)

            if p.editable:
                LOGGER.debug('Provider is editable; adding put/delete')
                put_path = f'{collection_name_path}/items/{{featureId}}'  # noqa
                paths[put_path]['put'] = {  # noqa
                    'summary': f'Update {title} items',
                    'description': desc,
                    'tags': [name],
                    'operationId': f'update{name.capitalize()}Features',
                    'parameters': [
                        referencer.create_ref_object('#/components/parameters/featureId', 'oapif-1')  # noqa
                    ],
                    'requestBody': {
                        'description': 'Updates item in collection',
                        'content': {
                            'application/geo+json': {
                                'schema': {}
                            }
                        },
                        'required': True
                    },
                    'responses': {
                        '204': {'description': 'Successful update'},
                        '400': referencer.create_ref_object('#/components/responses/InvalidParameter', 'oapif-1'),  # noqa
                        '500': referencer.create_ref_object('#/components/responses/ServerError', 'oapif-1')  # noqa
                    }
                }

                try:
                    schema_ref = p.get_schema(SchemaType.replace)
                    paths[put_path]['put']['requestBody']['content'][schema_ref[0]] = {  # noqa
                        'schema': schema_ref[1]
                    }
                except Exception as err:
                    LOGGER.debug(err)

                paths[f'{collection_name_path}/items/{{featureId}}']['delete'] = {  # noqa
                    'summary': f'Delete {title} items',
                    'description': desc,
                    'tags': [name],
                    'operationId': f'delete{name.capitalize()}Features',
                    'parameters': [
                        referencer.create_ref_object('#/components/parameters/featureId', 'oapif-1')  # noqa
                    ],
                    'responses': {
                        '200': {'description': 'Successful delete'},
                        '400': referencer.create_ref_object('#/components/responses/InvalidParameter', 'oapif-1'),  # noqa
                        '500': referencer.create_ref_object('#/components/responses/ServerError', 'oapif-1')  # noqa
                    }
                }

        except ProviderTypeError:
            LOGGER.debug('collection is not feature based')

        LOGGER.debug('setting up coverage endpoints')
        try:
            load_plugin('provider', get_provider_by_type(
                        collections[k]['providers'], 'coverage'))

            coverage_path = f'{collection_name_path}/coverage'

            paths[coverage_path] = {
                'get': {
                    'summary': f'Get {title} coverage',
                    'description': desc,
                    'tags': [name],
                    'operationId': f'get{name.capitalize()}Coverage',
                    'parameters': [
                        items_f,
                        items_l,
                        referencer.create_ref_object('#/components/parameters/bbox', 'oapif-1'),  # noqa
                        referencer.create_ref_object('#/components/parameters/bbox-crs', 'oapif-2'),  # noqa
                    ],
                    'responses': {
                        '200': referencer.create_ref_object('#/components/responses/Features', 'oapif-1'),  # noqa
                        '400': referencer.create_ref_object('#/components/responses/InvalidParameter', 'oapif-1'),  # noqa
                        '404': referencer.create_ref_object('#/components/responses/NotFound', 'oapif-1'),  # noqa
                        '500': referencer.create_ref_object('#/components/responses/ServerError', 'oapif-1')  # noqa
                    }
                }
            }

            coverage_domainset_path = f'{collection_name_path}/coverage/domainset'  # noqa

            paths[coverage_domainset_path] = {
                'get': {
                    'summary': f'Get {title} coverage domain set',
                    'description': desc,
                    'tags': [name],
                    'operationId': f'get{name.capitalize()}CoverageDomainSet',
                    'parameters': [
                        items_f,
                        items_l
                    ],
                    'responses': {
                        '200': {
                            'description': 'A coverage domain set.',
                            'content': {
                                'application/json': {
                                    'schema': referencer.create_ref_object('/schemas/cis_1.1/domainSet.yaml', 'oacov')  # noqa
                                },
                                'text/html': {
                                    'schema': {
                                        'type': 'string'
                                    }
                                }
                            }
                        },
                        '400': referencer.create_ref_object('#/components/responses/InvalidParameter', 'oapif-1'),  # noqa
                        '404': referencer.create_ref_object('#/components/responses/NotFound', 'oapif-1'),  # noqa
                        '500': referencer.create_ref_object('#/components/responses/ServerError', 'oapif-1')  # noqa
                    }
                }
            }

            coverage_rangetype_path = f'{collection_name_path}/coverage/rangetype'  # noqa

            paths[coverage_rangetype_path] = {
                'get': {
                    'summary': f'Get {title} coverage range type',
                    'description': desc,
                    'tags': [name],
                    'operationId': f'get{name.capitalize()}CoverageRangeType',
                    'parameters': [
                        items_f,
                        items_l
                    ],
                    'responses': {
                        '200': {
                            'description': 'A coverage range type.',
                            'content': {
                                'application/json': {
                                    'schema': referencer.create_ref_object('/schemas/cis_1.1/rangeType.yaml', 'oacov')  # noqa
                                },
                                'text/html': {
                                    'schema': {
                                        'type': 'string'
                                    }
                                }
                            }
                        },
                        '400': referencer.create_ref_object('#/components/responses/InvalidParameter', 'oapif-1'),  # noqa
                        '404': referencer.create_ref_object('#/components/responses/NotFound', 'oapif-1'),  # noqa
                        '500': referencer.create_ref_object('#/components/responses/ServerError', 'oapif-1')  # noqa
                    }
                }
            }
        except ProviderTypeError:
            LOGGER.debug('collection is not coverage based')

        LOGGER.debug('setting up tiles endpoints')
        tile_extension = filter_providers_by_type(
            collections[k]['providers'], 'tile')

        if tile_extension:
            tp = load_plugin('provider', tile_extension)
            referencer.components['schemas']['tileMatrixSetLink'] = {
                'type': 'object',
                'required': ['tileMatrixSet'],
                'properties': {
                    'tileMatrixSet': {
                        'type': 'string'
                    },
                    'tileMatrixSetURI': {
                        'type': 'string'
                    }
                }
            }
            tiles = {
                'type': 'object',
                'required': [
                    'tileMatrixSetLinks',
                    'links'
                ],
                'properties': {
                    'tileMatrixSetLinks': {
                        'type': 'array',
                        'items': referencer.create_ref_object('#/components/schemas/tileMatrixSetLink')  # noqa
                    },
                    'links': {
                        'type': 'array',
                        'items': referencer.create_ref_object('#/components/schemas/link', 'oapit')  # noqa
                    }
                }
            }
            referencer.components['schemas']['tiles'] = tiles

            referencer.components['responses']['Tiles'] = {
                'description': 'Retrieves the tiles description for this collection', # noqa
                'content': {
                    'application/json': {
                        'schema': referencer.create_ref_object('#/components/schemas/tiles')  # noqa
                    }
                }
            }

            tiles_path = f'{collection_name_path}/tiles'

            paths[tiles_path] = {
                'get': {
                    'summary': f'Fetch a {title} tiles description',
                    'description': desc,
                    'tags': [name],
                    'operationId': f'describe{name.capitalize()}Tiles',
                    'parameters': [
                        items_f,
                        # items_l  TODO: is this useful?
                    ],
                    'responses': {
                        '200': referencer.create_ref_object('#/components/responses/Tiles'),  # noqa
                        '400': referencer.create_ref_object('#/components/responses/InvalidParameter', 'oapif-1'),  # noqa
                        '404': referencer.create_ref_object('#/components/responses/NotFound', 'oapif-1'),  # noqa
                        '500': referencer.create_ref_object('#/components/responses/ServerError', 'oapif-1')  # noqa
                    }
                }
            }

            tiles_data_path = f'{collection_name_path}/tiles/{{tileMatrixSetId}}/{{tileMatrix}}/{{tileRow}}/{{tileCol}}'  # noqa

            paths[tiles_data_path] = {
                'get': {
                    'summary': f'Get a {title} tile',
                    'description': desc,
                    'tags': [name],
                    'operationId': f'get{name.capitalize()}Tiles',
                    'parameters': [
                        referencer.create_ref_object('#/components/parameters/tileMatrixSetId', 'oapit'), # noqa
                        referencer.create_ref_object('#/components/parameters/tileMatrix', 'oapit'),  # noqa
                        referencer.create_ref_object('#/components/parameters/tileRow', 'oapit'),  # noqa
                        referencer.create_ref_object('#/components/parameters/tileCol', 'oapit'),  # noqa
                        {
                            'name': 'f',
                            'in': 'query',
                            'description': 'The optional f parameter indicates the output format which the server shall provide as part of the response document.',  # noqa
                            'required': False,
                            'schema': {
                                'type': 'string',
                                'enum': [tp.format_type],
                                'default': tp.format_type
                            },
                            'style': 'form',
                            'explode': False
                        }
                    ],
                    'responses': {
                        '400': referencer.create_ref_object('#/components/responses/InvalidParameter', 'oapif-1'),  # noqa
                        '404': referencer.create_ref_object('#/components/responses/NotFound', 'oapif-1'),  # noqa
                        '500': referencer.create_ref_object('#/components/responses/ServerError', 'oapif-1')  # noqa
                    }
                }
            }
            mimetype = tile_extension['format']['mimetype']
            paths[tiles_data_path]['get']['responses']['200'] = {
                'description': 'successful operation',
                'content': {
                    mimetype: {
                        'schema': {
                            'type': 'string',
                            'format': 'binary'
                        }
                    }
                }
            }

        LOGGER.debug('setting up edr endpoints')
        edr_extension = filter_providers_by_type(
            collections[k]['providers'], 'edr')

        if edr_extension:
            ep = load_plugin('provider', edr_extension)

            edr_query_endpoints = []

            for qt in ep.get_query_types():
                edr_query_endpoints.append({
                    'path': f'{collection_name_path}/{qt}',
                    'qt': qt,
                    'op_id': f'query{qt.capitalize()}{k.capitalize()}'
                })
                if ep.instances:
                    edr_query_endpoints.append({
                        'path': f'{collection_name_path}/instances/{{instanceId}}/{qt}',  # noqa
                        'qt': qt,
                        'op_id': f'query{qt.capitalize()}Instance{k.capitalize()}'  # noqa
                    })

            for eqe in edr_query_endpoints:
                paths[eqe['path']] = {
                    'get': {
                        'summary': f"query {v['description']} by {eqe['qt']}",  # noqa
                        'description': v['description'],
                        'tags': [k],
                        'operationId': eqe['op_id'],
                        'parameters': [
                            referencer.create_ref_object(f"/parameters/{eqe['qt']}Coords.yaml", 'oaedr'),  # noqa
                            referencer.create_ref_object('#/components/parameters/datetime', 'oapif-1'),  # noqa
                            referencer.create_ref_object('/parameters/parameter-name.yaml', 'oaedr'),  # noqa
                            referencer.create_ref_object('/parameters/z.yaml', 'oaedr'),  # noqa
                            referencer.create_ref_object('#/components/parameters/f'),  # noqa

                        ],
                        'responses': {
                            '200': {
                                'description': 'Response',
                                'content': {
                                    'application/prs.coverage+json': {
                                        "schema": referencer.create_ref_object('/schemas/coverageJSON.yaml', 'oaedr')  # noqa
                                    }
                                }
                            }
                        }
                    }
                }

        LOGGER.debug('setting up maps endpoints')
        map_extension = filter_providers_by_type(
            collections[k]['providers'], 'map')

        if map_extension:
            mp = load_plugin('provider', map_extension)

            map_f = deepcopy(referencer.components['parameters']['f'])
            map_f['schema']['enum'] = [map_extension['format']['name']]
            map_f['schema']['default'] = map_extension['format']['name']

            pth = f'/collections/{k}/map'
            paths[pth] = {
                'get': {
                    'summary': 'Get map',
                    'description': f"{v['description']} map",
                    'tags': [k],
                    'operationId': 'getMap',
                    'parameters': [
                        referencer.create_ref_object('#/components/parameters/bbox', 'oapif-1'),  # noqa
                        referencer.create_ref_object('#/components/parameters/bbox-crs', 'oapif-2'),  # noqa
                        {
                            'name': 'width',
                            'in': 'query',
                            'description': 'Response image width',
                            'required': False,
                            'schema': {
                                'type': 'integer',
                            },
                            'style': 'form',
                            'explode': False
                        },
                        {
                            'name': 'height',
                            'in': 'query',
                            'description': 'Response image height',
                            'required': False,
                            'schema': {
                                'type': 'integer',
                            },
                            'style': 'form',
                            'explode': False
                        },
                        {
                            'name': 'transparent',
                            'in': 'query',
                            'description': 'Background transparency of map (default=true).',  # noqa
                            'required': False,
                            'schema': {
                                'type': 'boolean',
                                'default': True,
                            },
                            'style': 'form',
                            'explode': False
                        },
                        map_f
                    ],
                    'responses': {
                        '200': {
                            'description': 'Response',
                            'content': {
                                'application/json': {}
                            }
                        },
                        '400': referencer.create_ref_object('#/components/responses/InvalidParameter', 'oapif-1'),  # noqa
                        '500': referencer.create_ref_object('#/components/responses/ServerError', 'oapif-1')  # noqa
                    }
                }
            }
            if mp.time_field is not None:
                paths[pth]['get']['parameters'].append(
                    referencer.create_ref_object('#/components/parameters/datetime', 'oapif-1'),  # noqa
                )

    LOGGER.debug('setting up STAC')
    stac_collections = filter_dict_by_key_value(cfg['resources'],
                                                'type', 'stac-collection')
    if stac_collections:
        paths['/stac'] = {
            'get': {
                'summary': 'SpatioTemporal Asset Catalog',
                'description': 'SpatioTemporal Asset Catalog',
                'tags': ['stac'],
                'operationId': 'getStacCatalog',
                'parameters': [],
                'responses': {
                    '200': referencer.create_ref_object('#/components/responses/200', 'common'),  # noqa
                    'default': referencer.create_ref_object('#/components/responses/500', 'common')  # noqa
                }
            }
        }

    processes = filter_dict_by_key_value(cfg['resources'], 'type', 'process')

    has_manager = 'manager' in cfg['server']

    if processes:
        paths['/processes'] = {
            'get': {
                'summary': 'Processes',
                'description': 'Processes',
                'tags': ['server'],
                'operationId': 'getProcesses',
                'parameters': [
                    referencer.create_ref_object('#/components/parameters/f'),
                ],
                'responses': {
                    '200': referencer.create_ref_object('/responses/ProcessList.yaml', 'oapip'),  # noqa
                    'default': referencer.create_ref_object('#/components/responses/500', 'common')  # noqa
                }
            }
        }
        LOGGER.debug('setting up processes')

        for k, v in processes.items():
            if k.startswith('_'):
                LOGGER.debug(f'Skipping hidden layer: {k}')
                continue
            name = l10n.translate(k, locale_)
            p = load_plugin('process', v['processor'])

            md_desc = l10n.translate(p.metadata['description'], locale_)
            process_name_path = f'/processes/{name}'
            tag = {
                'name': name,
                'description': md_desc,  # noqa
                'externalDocs': {}
            }
            for link in l10n.translate(p.metadata['links'], locale_):
                if link['type'] == 'information':
                    tag['externalDocs']['description'] = link['type']
                    tag['externalDocs']['url'] = link['url']
                    break
            if len(tag['externalDocs']) == 0:
                del tag['externalDocs']

            oas['tags'].append(tag)

            paths[process_name_path] = {
                'get': {
                    'summary': 'Get process metadata',
                    'description': md_desc,
                    'tags': [name],
                    'operationId': f'describe{name.capitalize()}Process',
                    'parameters': [
                        referencer.create_ref_object('#/components/parameters/f')  # noqa
                    ],
                    'responses': {
                        '200': referencer.create_ref_object('#/components/responses/200', 'common'),  # noqa
                        'default': referencer.create_ref_object('#/components/responses/500', 'common')  # noqa
                    }
                }
            }

            paths[f'{process_name_path}/execution'] = {
                'post': {
                    'summary': f"Process {l10n.translate(p.metadata['title'], locale_)} execution",  # noqa
                    'description': md_desc,
                    'tags': [name],
                    'operationId': f'execute{name.capitalize()}Job',
                    'responses': {
                        '200': referencer.create_ref_object('#/components/responses/200', 'common'),  # noqa
                        '201': referencer.create_ref_object('/responses/ExecuteAsync.yaml', 'oapip'),  # noqa
                        '404': referencer.create_ref_object('/responses/NotFound.yaml', 'oapip'),  # noqa
                        'default': referencer.create_ref_object('#/components/responses/500', 'common')  # noqa
                    },
                    'requestBody': {
                        'description': 'Mandatory execute request JSON',
                        'required': True,
                        'content': {
                            'application/json': {
                                "schema": referencer.create_ref_object('/schemas/execute.yaml', 'oapip')  # noqa
                            }
                        }
                    }
                }
            }
            if 'example' in p.metadata:
                paths[f'{process_name_path}/execution']['post']['requestBody']['content']['application/json']['example'] = p.metadata['example']  # noqa

        name_in_path = {
            'name': 'jobId',
            'in': 'path',
            'description': 'job identifier',
            'required': True,
            'schema': {
                'type': 'string'
            }
        }

        if has_manager:
            paths['/jobs'] = {
                'get': {
                    'summary': 'Retrieve jobs list',
                    'description': 'Retrieve a list of jobs',
                    'tags': ['server'],
                    'operationId': 'getJobs',
                    'responses': {
                        '200': referencer.create_ref_object('#/components/responses/200', 'common'),  # noqa
                        '404': referencer.create_ref_object('/responses/NotFound.yaml', 'oapip'),  # noqa
                        'default': referencer.create_ref_object('#/components/responses/500', 'common')  # noqa
                    }
                }
            }

            paths['/jobs/{jobId}'] = {
                'get': {
                    'summary': 'Retrieve job details',
                    'description': 'Retrieve job details',
                    'tags': ['server'],
                    'parameters': [
                        name_in_path,
                        referencer.create_ref_object('#/components/parameters/f')  # noqa
                    ],
                    'operationId': 'getJob',
                    'responses': {
                        '200': referencer.create_ref_object('#/components/responses/200', 'common'),  # noqa
                        '404': referencer.create_ref_object('/responses/NotFound.yaml', 'oapip'),  # noqa
                        'default': referencer.create_ref_object('#/components/responses/500', 'common')  # noqa
                    }
                },
                'delete': {
                    'summary': 'Cancel / delete job',
                    'description': 'Cancel / delete job',
                    'tags': ['server'],
                    'parameters': [
                        name_in_path
                    ],
                    'operationId': 'deleteJob',
                    'responses': {
                        '204': {'description': 'Successful update'},  # noqa
                        '404': referencer.create_ref_object('/responses/NotFound.yaml', 'oapip'),  # noqa
                        'default': referencer.create_ref_object('#/components/responses/500', 'common')  # noqa
                    }
                },
            }

            paths['/jobs/{jobId}/results'] = {
                'get': {
                    'summary': 'Retrieve job results',
                    'description': 'Retrieve job results',
                    'tags': ['server'],
                    'parameters': [
                        name_in_path,
                        referencer.create_ref_object('#/components/parameters/f')  # noqa
                    ],
                    'operationId': 'getJobResults',
                    'responses': {
                        '200': referencer.create_ref_object('#/components/responses/200', 'common'),  # noqa
                        '404': referencer.create_ref_object('/responses/NotFound.yaml', 'oapip'),  # noqa
                        'default': referencer.create_ref_object('#/components/responses/500', 'common')  # noqa
                    }
                }
            }

    oas['paths'] = paths

    return oas


def get_oas(cfg, version='3.0'):
    """
    Stub to generate OpenAPI Document

    :param cfg: configuration object
    :param version: version of OpenAPI (default 3.0)

    :returns: OpenAPI definition YAML dict
    """

    if version == '3.0':
        return get_oas_30(cfg)
    else:
        raise RuntimeError('OpenAPI version not supported')


def validate_openapi_document(instance_dict):
    """
    Validate an OpenAPI document against the OpenAPI schema

    :param instance_dict: dict of OpenAPI instance

    :returns: `bool` of validation
    """

    schema_file = os.path.join(THISDIR, 'schemas', 'openapi',
                               'openapi-3.0.x.json')

    with open(schema_file) as fh2:
        schema_dict = json.load(fh2)
        jsonschema_validate(instance_dict, schema_dict)

        return True


def generate_openapi_document(cfg_file: Union[Path, io.TextIOWrapper],
                              output_format: str):
    """
    Generate an OpenAPI document from the configuration file

    :param cfg_file: configuration Path instance
    :param output_format: output format for OpenAPI document

    :returns: content of the OpenAPI document in the output
              format requested
    """
    if isinstance(cfg_file, Path):
        with cfg_file.open(mode="r") as cf:
            s = yaml_load(cf)
    else:
        s = yaml_load(cfg_file)
    pretty_print = s['server'].get('pretty_print', False)

    if output_format == 'yaml':
        content = yaml.safe_dump(get_oas(s), default_flow_style=False)
    elif output_format == 'json':
        content = to_json(get_oas(s), pretty=pretty_print)
    else:
        raise RuntimeError(f'Format {output_format} is not supported')
    return content


@click.group()
def openapi():
    """OpenAPI management"""
    pass


@click.command()
@click.pass_context
@click.argument('config_file', type=click.File(encoding='utf-8'))
@click.option('--format', '-f', 'oas_format', type=click.Choice(['json', 'yaml']),  # noqa
              default='yaml', help='output format (json|yaml)')
@click.option('--output-file', '-of', type=click.File('w', encoding='utf-8'),
              help='Name of output file')
def generate(ctx, config_file, output_file, oas_format='yaml'):
    """Generate OpenAPI Document"""

    if config_file is None:
        raise click.ClickException('--config/-c required')

    content = generate_openapi_document(config_file, oas_format)

    if output_file is None:
        click.echo(content)
    else:
        click.echo(f'Generating {output_file.name}')
        output_file.write(content)
        click.echo('Done')


@click.command()
@click.pass_context
@click.argument('openapi_file', type=click.File())
def validate(ctx, openapi_file):
    """Validate OpenAPI Document"""

    if openapi_file is None:
        raise click.ClickException('--openapi/-o required')

    click.echo(f'Validating {openapi_file}')
    instance = yaml_load(openapi_file)
    validate_openapi_document(instance)
    click.echo('Valid OpenAPI document')


openapi.add_command(generate)
openapi.add_command(validate)
