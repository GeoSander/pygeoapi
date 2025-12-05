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

import logging
from typing import Any

from pygeoapi import l10n, join_util
from pygeoapi.api import (
    APIRequest, API, SYSTEM_LOCALE, FORMAT_TYPES,
    F_JSON, F_JSONLD, F_HTML, HTTPStatus
)
from pygeoapi.openapi import get_visible_collections
from pygeoapi.plugin import load_plugin
from pygeoapi.provider.base import ProviderTypeError, ProviderGenericError
from pygeoapi.util import (
    get_provider_by_type, to_json, filter_providers_by_type,
    filter_dict_by_key_value, get_current_datetime
)

LOGGER = logging.getLogger(__name__)

API_NAME = 'joins'
CONFORMANCE_CLASSES = [
    # TODO: Endpoints
    # 'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/core',
    # 'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/data-joining',
    # 'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/join-delete',
    # 'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/file-joining',
    # TODO: Input
    # 'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/input-file-upload',
    # 'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/input-http-ref',
    # 'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/input-csv',
    # 'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/input-geojson',
    # TODO: Output
    # 'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/output-geojson',
    # 'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/output-geojson-direct',  # noqa
    # Encodings
    'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/html',
    'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/json',
    'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/geojson',
    # OpenAPI
    'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/oas30',
]


def init(cfg: dict) -> bool:
    """
    Shortcut to initialize join utility with config.
    Called dynamically by the main `API.__init__` method.

    :param cfg: pygeoapi configuration dict

    :returns: True if OGC API - Joins is available and initialized
    """
    return join_util.init(cfg)


def get_oas_30(cfg: dict, locale: str) -> tuple[list[dict[str, str]], dict[str, dict]]:  # noqa
    """
    Get OpenAPI fragments

    :param cfg: `dict` of configuration
    :param locale: `str` of locale

    :returns: `tuple` of `list` of tag objects, and `dict` of path objects
    """

    paths = {}

    if not join_util.enabled(cfg):
        LOGGER.info('OpenAPI: skipping OGC API - Joins endpoints setup')
        return [], {'paths': paths}

    LOGGER.debug('OpenAPI: setting up OGC API - Joins endpoints')

    collections = filter_dict_by_key_value(cfg['resources'],
                                           'type', 'collection')

    links_conf = {
        'type': 'array',
        'items': {
            'type': 'object',
            'required': [
                'href',
            ],
            'properties': {
                'href': {
                    'type': 'string'
                },
                'rel': {
                    'type': 'string',
                    'example': 'service'
                },
                'type': {
                    'type': 'string',
                    'example': 'application/json'
                },
                'hreflang': {
                    'type': 'string',
                    'example': 'en'
                },
                'title': {
                    'type': 'string'
                }
            }
        }
    }

    join_details_conf = {
        'type': 'object',
        'required': [
            'id',
            'links',
            'details'
        ],
        'properties': {
            'id': {
                'type': 'string',
            },
            'timeStamp': {
                'type': 'string',
                'format': 'date-time'
            },
            'details': {
                'type': 'object',
                'properties': {
                    'created': {
                        'type': 'string',
                        'format': 'date-time'
                    },
                    'sourceFile': {
                        'type': 'string'
                    },
                    'collectionKey': {
                        'type': 'string'
                    },
                    'joinKey': {
                        'type': 'string'
                    },
                    'joinFields': {
                        'type': 'array'
                    },
                    'numberOfRows': {
                        'type': 'integer'
                    }
                }
            },
            'links': links_conf
        }
    }

    for k, v in get_visible_collections(cfg).items():
        feature_provider = filter_providers_by_type(
            collections[k]['providers'], 'feature')
        if not feature_provider:
            # We can only do joins on features!
            continue

        title = l10n.translate(v['title'], locale)

        # Keys endpoint
        paths[f'/collections/{k}/keys'] = {
            'get': {
                'summary': f'Get {title} join key fields',
                'description': f'Lists all available {title} join key fields',
                'tags': [k, API_NAME],
                'operationId': f'get{k.capitalize()}Keys',
                'parameters': [
                    {'$ref': '#/components/parameters/f'},
                    {'$ref': '#/components/parameters/lang'},
                ],
                'responses': {
                    '200': {
                        'description': 'Response',
                        'content': {
                            'application/json': {
                                'schema': {
                                    'type': 'object',
                                    'properties': {
                                        'links': links_conf,
                                        'keys': {
                                            'type': 'array',
                                            'items': {
                                                'type': 'object',
                                                'required': [
                                                    'id',
                                                    'isDefault',
                                                    # 'links'
                                                ],
                                                'properties': {
                                                    'id': {
                                                        'type': 'string',
                                                    },
                                                    'language': {
                                                        'type': 'string',
                                                    },
                                                    'isDefault': {
                                                        'type': 'boolean',
                                                    },
                                                    # 'links': links_conf
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    '400': {'$ref': '#/components/responses/BadRequest'},
                    '404': {'$ref': '#/components/responses/NotFound'},
                    '500': {'$ref': '#/components/responses/ServerError'}
                }
            }
        }

        # Join source endpoints (list and create)
        paths[f'/collections/{k}/{API_NAME}'] = {
            'get': {
                'summary': 'Get all available join sources',
                'description': 'Lists all available join sources for this collection',  # noqa
                'tags': [k, API_NAME],
                'operationId': f'get{k.capitalize()}JoinSources',
                'parameters': [
                    {'$ref': '#/components/parameters/f'},
                    {'$ref': '#/components/parameters/lang'},
                ],
                'responses': {
                    '200': {
                        'description': 'Response',
                        'content': {
                            'application/json': {
                                'schema': {
                                    'type': 'object',
                                    'required': [
                                        'joins',
                                        'links'
                                    ],
                                    'properties': {
                                        'links': links_conf,
                                        'joins': {
                                            'type': 'array',
                                            'items': {
                                                'type': 'array',
                                                'items': {
                                                    'type': 'object',
                                                    'required': [
                                                        'id',
                                                        'links',
                                                        'timeStamp'
                                                    ],
                                                    'properties': {
                                                        'id': {
                                                            'type': 'string',
                                                        },
                                                        'timeStamp': {
                                                            'type': 'string',
                                                            'format': 'date-time',  # noqa
                                                        },
                                                        'links': links_conf
                                                    }
                                                }
                                            }
                                        },
                                        'numberMatched': {
                                            'type': 'integer'
                                        },
                                        'numberReturned': {
                                            'type': 'integer'
                                        },
                                        'timeStamp': {
                                            'type': 'string',
                                            'format': 'date-time'
                                        }
                                    }
                                }
                            }
                        }
                    },
                    '400': {'$ref': '#/components/responses/BadRequest'},
                    '404': {'$ref': '#/components/responses/NotFound'},
                    '500': {'$ref': '#/components/responses/ServerError'}
                }
            },
            'post': {
                'summary': 'Uploads a new join source',
                'description': 'Creates a new joinable table based on the provided parameters',  # noqa
                'tags': [k, API_NAME],
                'operationId': f'create{k.capitalize()}JoinSource',
                'parameters': [
                    {'$ref': '#/components/parameters/f'},
                    {'$ref': '#/components/parameters/lang'},
                ],
                'requestBody': {
                    'title': 'Join Source Parameters',
                    'description': 'CSV data and parameters to create a new join source.',  # noqa
                    'required': True,
                    'content': {
                        'multipart/form-data': {
                            'schema': {
                                'type': 'object',
                                'required': [
                                    'collectionKey',  # 'left-dataset-key',
                                    'joinFile',  # 'right-dataset-file',
                                    'joinKey',  # 'right-dataset-key'
                                ],
                                'properties': {
                                    'collectionKey': {
                                        'type': 'string',
                                        'description': 'The primary key field in the left side dataset (collection) to join on'  # noqa
                                    },
                                    'joinFile': {
                                        'type': 'string',
                                        'format': 'binary',
                                        'description': 'The right side dataset file (i.e. CSV) to upload'  # noqa
                                    },
                                    'joinKey': {
                                        'type': 'string',
                                        'description': 'The foreign key field in the right side dataset (i.e. CSV) that contains the key values to join on'  # noqa
                                    },
                                    # 'right-dataset-field-list'
                                    'joinFields': {
                                        'type': 'string',
                                        'description': 'Comma-separated list of field names in the CSV to include in the join result. If not specified, all non-conflicting fields are included.',  # noqa
                                        'example': 'name,population,area'
                                    },
                                    'csvDelimiter': {
                                        'type': 'string',
                                        'description': 'The delimiter character used in the CSV file (optional)',  # noqa
                                        'default': ',',
                                        'example': ','
                                    },
                                    'csvHeaderRow': {
                                        'type': 'integer',
                                        'description': 'The 1-based row number of the header row in the CSV file (optional)',  # noqa
                                        'default': 1,
                                        'minimum': 1
                                    },
                                    'csvDataStartRow': {
                                        'type': 'integer',
                                        'description': 'The 1-based row number where data starts in the CSV file (optional)',  # noqa
                                        'default': 2,
                                        'minimum': 1
                                    }
                                }
                            },
                            'encoding': {
                                'joinFile': {
                                    'contentType': 'text/csv'
                                }
                            }
                        }
                    }
                },
                'responses': {
                    '201': {
                        'description': 'Create a new join source table from an uploaded CSV.',  # noqa
                        'content': {
                            'application/json': {
                                'schema': join_details_conf
                            }
                        }
                    },
                    '400': {'$ref': '#/components/responses/BadRequest'},
                    '404': {'$ref': '#/components/responses/NotFound'},
                    '500': {'$ref': '#/components/responses/ServerError'}
                }
            }
        }

        # Joins endpoint (join metadata and delete)
        join_id_param = {
            'name': 'joinId',
            'in': 'path',
            'description': 'Join source identifier',
            'required': True,
            'schema': {
                'type': 'string'
            }
        }
        paths[f'/collections/{k}/{API_NAME}/{{joinId}}'] = {
            'get': {
                'summary': 'Get join source details',
                'description': 'Returns the metadata for a CSV join source',
                'tags': [k, API_NAME],
                'operationId': f'get{k.capitalize()}JoinSourceDetails',
                'parameters': [
                    join_id_param,
                    {'$ref': '#/components/parameters/f'},
                    {'$ref': '#/components/parameters/lang'},
                ],
                'responses': {
                    '200': {
                        'description': 'Details of a join source table (uploaded CSV).',  # noqa
                        'content': {
                            'application/json': {
                                'schema': join_details_conf
                            }
                        }
                    }
                },
                '400': {'$ref': '#/components/responses/BadRequest'},
                '404': {'$ref': '#/components/responses/NotFound'},
                '500': {'$ref': '#/components/responses/ServerError'}
            },
            'delete': {
                'summary': 'Delete a join source with the given id',
                'description': 'Deletes a join source with the given id',
                'tags': [k, API_NAME],
                'operationId': f'delete{k.capitalize()}JoinSource',
                'parameters': [
                    join_id_param,
                    {'$ref': '#/components/parameters/f'},
                    {'$ref': '#/components/parameters/lang'},
                ],
                'responses': {
                    "204": {'$ref': '#/components/responses/204'},
                    '400': {'$ref': '#/components/responses/BadRequest'},
                    '404': {'$ref': '#/components/responses/NotFound'},
                    '500': {'$ref': '#/components/responses/ServerError'}
                }
            }
        }

    return [{'name': API_NAME}], {'paths': paths}


def _prepare(api: API, request: APIRequest,
             collection: str) -> tuple[dict, dict, str]:
    """
    Prepare headers, collections, and dataset for API response handling.

    :param api: API instance
    :param request: A request object
    :param collection: Dataset name / collection path

    :returns: tuple of headers, collections, dataset name
    """

    headers = request.get_response_headers(SYSTEM_LOCALE, **api.api_headers)
    collections = filter_dict_by_key_value(api.config['resources'],
                                           'type', 'collection')
    dataset = collection.removeprefix(api.get_collections_url()).strip('/')
    return headers, collections, dataset


def _bad_request(api: API, request: APIRequest, headers: dict,
                 msg: str) -> tuple[dict, int, str]:
    return api.get_exception(
        HTTPStatus.BAD_REQUEST, headers, request.format,
        'InvalidParameterValue', msg)


def _not_found(api: API, request: APIRequest, headers: dict,
               msg: str) -> tuple[dict, int, str]:
    return api.get_exception(
        HTTPStatus.NOT_FOUND, headers, request.format,
        'NotFound', msg
    )


def _server_error(api: API, request: APIRequest, headers: dict,
                  msg: str) -> tuple[dict, int, str]:
    return api.get_exception(
        HTTPStatus.INTERNAL_SERVER_ERROR, headers, request.format,
        'NoApplicableCode', msg
    )


def list_joins(api: API, request: APIRequest,
               collection: str) -> tuple[dict, int, str]:
    """
    Returns all available joins from the server

    :param api: API instance
    :param request: A request object
    :param collection: Collection path/name (not used in this implementation)

    :returns: tuple of headers, status code, content
    """
    headers, collections, dataset = _prepare(api, request, collection)

    if dataset not in collections:
        msg = f'Collection not found: {dataset}'
        return _not_found(api, request, headers, msg)

    try:
        sources = join_util.list_sources(dataset)
    except Exception as e:
        LOGGER.error(str(e), exc_info=True)
        return _server_error(api, request, headers, str(e))

    # Build the joins list with proper structure
    joins_list = []
    uri = f'{api.get_collections_url()}/{dataset}'
    for source_id, source_obj in sources:
        join_item = {
            'id': source_id,
            'timeStamp': source_obj['timeStamp'],
            'links': [
                {
                    'type': FORMAT_TYPES[F_JSON],
                    'rel': request.get_linkrel(F_JSON),
                    'title': l10n.translate('Join source details as JSON', request.locale),  # noqa
                    'href': f'{uri}/joins/{source_id}?f={F_JSON}'
                }, {
                    'type': FORMAT_TYPES[F_JSONLD],
                    'rel': request.get_linkrel(F_JSONLD),
                    'title': l10n.translate('Join source details as RDF (JSON-LD)', request.locale),  # noqa
                    'href': f'{uri}/joins/{source_id}?f={F_JSONLD}'
                }, {
                    'type': FORMAT_TYPES[F_HTML],
                    'rel': request.get_linkrel(F_HTML),
                    'title': l10n.translate('Join source details as HTML', request.locale),  # noqa
                    'href': f'{uri}/joins/{source_id}?f={F_HTML}'
                }
            ]
        }
        joins_list.append(join_item)

    # Build the response with proper structure
    # TODO: support pagination
    response = {
        'links': [
            {
                'type': FORMAT_TYPES[F_JSON],
                'rel': request.get_linkrel(F_JSON),
                'title': l10n.translate('This document as JSON', request.locale),  # noqa
                'href': f'{uri}/joins?f={F_JSON}'
            }, {
                'type': FORMAT_TYPES[F_JSONLD],
                'rel': request.get_linkrel(F_JSONLD),
                'title': l10n.translate('This document as RDF (JSON-LD)', request.locale),  # noqa
                'href': f'{uri}/joins?f={F_JSONLD}'
            }, {
                'type': FORMAT_TYPES[F_HTML],
                'rel': request.get_linkrel(F_HTML),
                'title': l10n.translate('This document as HTML', request.locale),  # noqa
                'href': f'{uri}/joins?f={F_HTML}'
            }
        ],
        'joins': joins_list,
        'numberMatched': len(joins_list),
        'numberReturned': len(joins_list),
        'timeStamp': get_current_datetime()
    }

    # Set response language to requested provider locale
    # (if it supports language) and/or otherwise the requested pygeoapi
    # locale (or fallback default locale)
    l10n.set_response_language(headers, request.locale)

    return headers, HTTPStatus.OK, to_json(response, api.pretty_print)


def join_details(api: API, request: APIRequest,
                 collection: str, join_id: str) -> tuple[dict, int, str]:
    """
    Returns the metadata of a specific join source on the server

    :param api: API instance
    :param request: A request object
    :param collection: Collection path/name (not used in this implementation)
    :param join_id: The id of the join to retrieve metadata for

    :returns: tuple of headers, status code, content
    """
    headers, collections, dataset = _prepare(api, request, collection)

    if dataset not in collections:
        msg = f'Collection not found: {dataset}'
        return _not_found(api, request, headers, msg)

    try:
        details = join_util.read_join_source(dataset, join_id)

        uri = f'{api.get_collections_url()}/{dataset}'
        response = {
            'id': join_id,
            'timeStamp': get_current_datetime(),
            'details': {
                'created': details['timeStamp'],
                'sourceFile': details['joinSource'],
                'collectionKey': details['collectionKey'],
                'joinKey': details['joinKey'],
                'joinFields': details['joinFields'],
                'numberOfRows': details['numberOfRows']
            },
            'links': [
                {
                    'type': FORMAT_TYPES[F_JSON],
                    'rel': request.get_linkrel(F_JSON),
                    'title': l10n.translate('This document as JSON', request.locale),  # noqa
                    'href': f'{uri}/joins/{join_id}?f={F_JSON}'
                }, {
                    'type': FORMAT_TYPES[F_JSONLD],
                    'rel': request.get_linkrel(F_JSONLD),
                    'title': l10n.translate('This document as RDF (JSON-LD)', request.locale),  # noqa
                    'href': f'{uri}/joins/{join_id}?f={F_JSONLD}'
                }, {
                    'type': FORMAT_TYPES[F_HTML],
                    'rel': request.get_linkrel(F_HTML),
                    'title': l10n.translate('This document as HTML', request.locale),  # noqa
                    'href': f'{uri}/joins/{join_id}?f={F_HTML}'
                }, {
                    'type': 'application/geo+json',
                    'rel': 'results',
                    'title': 'Items with joined data as GeoJSON',
                    'href': f"{uri}/items?f={F_JSON}&joinId={join_id}",
                }, {
                    'type': FORMAT_TYPES[F_JSONLD],
                    'rel': 'results',
                    'title': 'Items with joined data as RDF (JSON-LD)',
                    'href': f"{uri}/items?f={F_JSONLD}&joinId={join_id}",  # noqa
                }, {
                    'type': FORMAT_TYPES[F_HTML],
                    'rel': 'results',
                    'title': 'Items with joined data items as HTML',
                    'href': f"{uri}/items?f={F_HTML}&joinId={join_id}",
                }
            ]
        }

    except ValueError as e:
        LOGGER.error(f'Invalid request parameter: {e}', exc_info=True)
        return _bad_request(api, request, headers, str(e))
    except KeyError as e:
        msg = 'Collection or join source not found'
        LOGGER.error(f'Invalid parameter value: {e}', exc_info=True)
        return _not_found(api, request, headers, msg)
    except Exception as e:
        LOGGER.error(f'Failed to retrieve join: {e}', exc_info=True)
        msg = f'Failed to retrieve join: {str(e)}'
        return _server_error(api, request, headers, msg)

    # Set response language to requested provider locale
    # (if it supports language) and/or otherwise the requested pygeoapi
    # locale (or fallback default locale)
    l10n.set_response_language(headers, request.locale)

    return headers, HTTPStatus.OK, to_json(response, api.pretty_print)


def create_join(api: API, request: APIRequest,
                collection: str) -> tuple[dict, int, Any]:
    """
    Creates a new join on the server.

    :param api: API instance
    :param request: A request object
    :param collection: Collection path/name (not used in this implementation)

    :returns: tuple of headers, status code, content
    """
    headers, collections, dataset = _prepare(api, request, collection)

    if not api.supports_joins:
        # TODO: perhaps a 406 Not Acceptable would be better?
        msg = 'OGC API - Joins is not available on this instance'
        return _server_error(api, request, headers, msg)

    if not request.supports_formdata:
        # i.e. python-multipart library is not installed for Starlette
        # TODO: perhaps a 406 Not Acceptable would be better?
        msg = 'multipart/form-data requests are not supported on this instance'
        return _server_error(api, request, headers, msg)

    if dataset not in collections:
        msg = f'Collection not found: {dataset}'
        return _not_found(api, request, headers, msg)

    try:
        # Get collection provider
        try:
            provider_def = get_provider_by_type(
                collections[dataset]['providers'], 'feature'
            )
            provider = load_plugin('provider', provider_def)
        except ProviderTypeError:
            msg = f'Feature provider not found for collection: {dataset}'
            return _bad_request(api, request, headers, msg)

        # Get provider locale (if any)
        prv_locale = l10n.get_plugin_locale(provider_def, request.raw_locale)

        details = join_util.process_csv(dataset, provider, request.form)

        uri = f'{api.get_collections_url()}/{dataset}'
        join_id = details['id']
        response = {
            'id': join_id,
            'timeStamp': get_current_datetime(),
            'details': {
                'created': details['timeStamp'],
                'sourceFile': details['joinSource'],
                'collectionKey': details['collectionKey'],
                'joinKey': details['joinKey'],
                'joinFields': details['joinFields'],
                'numberOfRows': details['numberOfRows']
            },
            'links': [
                {
                    'type': FORMAT_TYPES[F_JSON],
                    'rel': request.get_linkrel(F_JSON),
                    'title': l10n.translate('This document as JSON', request.locale),  # noqa
                    'href': f'{uri}/joins/{join_id}?f={F_JSON}'
                }, {
                    'type': FORMAT_TYPES[F_JSONLD],
                    'rel': request.get_linkrel(F_JSONLD),
                    'title': l10n.translate('This document as RDF (JSON-LD)', request.locale),  # noqa
                    'href': f'{uri}/joins/{join_id}?f={F_JSONLD}'
                }, {
                    'type': FORMAT_TYPES[F_HTML],
                    'rel': request.get_linkrel(F_HTML),
                    'title': l10n.translate('This document as HTML', request.locale),  # noqa
                    'href': f'{uri}/joins/{join_id}?f={F_HTML}'
                }, {
                    'type': 'application/geo+json',
                    'rel': 'results',
                    'title': 'Items with joined data as GeoJSON',
                    'href': f"{uri}/items?f={F_JSON}&joinId={details['id']}",  # noqa
                }, {
                    'type': FORMAT_TYPES[F_JSONLD],
                    'rel': 'results',
                    'title': 'Items with joined data as RDF (JSON-LD)',
                    'href': f"{uri}/items?f={F_JSONLD}&joinId={details['id']}",  # noqa
                }, {
                    'type': FORMAT_TYPES[F_HTML],
                    'rel': 'results',
                    'title': 'Items with joined data as HTML',
                    'href': f"{uri}/items?f={F_HTML}&joinId={details['id']}",  # noqa
                }
            ]
        }

    except ValueError as e:
        LOGGER.error(f'Invalid request parameter: {e}', exc_info=True)
        return _bad_request(api, request, headers, str(e))
    except KeyError as e:
        msg = 'Collection or join source not found'
        LOGGER.error(f'Invalid parameter value: {e}', exc_info=True)
        return _not_found(api, request, headers, msg)
    except Exception as e:
        LOGGER.error(f'Failed to create join: {e}', exc_info=True)
        msg = f'Failed to create join: {str(e)}'
        return _server_error(api, request, headers, msg)

    # Set response language to requested provider locale
    # (if it supports language) and/or otherwise the requested pygeoapi
    # locale (or fallback default locale)
    l10n.set_response_language(headers, prv_locale, request.locale)

    return headers, HTTPStatus.OK, to_json(response, api.pretty_print)


def delete_join(api: API, request: APIRequest,
                collection: str, join_id: str) -> tuple[dict, int, str]:
    """
    Removes a specific join source from the server.

    :param api: API instance
    :param request: A request object
    :param collection: Collection path/name
    :param join_id: The id of the join to remove

    :returns: tuple of headers, status code, content
    """
    headers, collections, dataset = _prepare(api, request, collection)

    if dataset not in collections:
        msg = f'Collection not found: {dataset}'
        return _not_found(api, request, headers, msg)

    try:
        if not join_util.remove_source(dataset, join_id):
            msg = f'Join source {join_id} not found for collection {dataset}'
            return _not_found(api, request, headers, msg)
    except ValueError as e:
        LOGGER.error(f'Invalid request parameter: {e}', exc_info=True)
        return _bad_request(api, request, headers, str(e))
    except Exception as e:
        LOGGER.error(f'Failed to delete join source: {e}',
                     exc_info=True)
        msg = f'Failed to delete join: {str(e)}'
        return _server_error(api, request, headers, msg)

    # Set response language to requested provider locale
    # (if it supports language) and/or otherwise the requested pygeoapi
    # locale (or fallback default locale)
    l10n.set_response_language(headers, request.locale)

    # TODO: return JSON on a 204? DELETE /jobs/{jobId} doesn't do this either
    return headers, HTTPStatus.NO_CONTENT, f'join source {join_id} deleted successfully'  # noqa


def key_fields(api: API, request: APIRequest,
               collection: str) -> tuple[dict, int, str]:
    """
    Returns all available join key fields for a collection

    :param api: API instance
    :param request: A request object
    :param collection: Collection path/name

    :returns: tuple of headers, status code, content
    """
    headers, collections, dataset = _prepare(api, request, collection)

    if dataset not in collections:
        msg = f'Collection not found: {dataset}'
        return _not_found(api, request, headers, msg)

    LOGGER.debug(f"Retrieving key field configuration "
                 f"for collection '{dataset}'")

    try:
        provider_def = get_provider_by_type(
            collections[dataset]['providers'], 'feature')
        provider = load_plugin('provider', provider_def)
    except ProviderTypeError:
        msg = f'Feature provider not found for collection: {dataset}'
        return _bad_request(api, request, headers, msg)

    try:
        fields = provider.get_key_fields()
    except ProviderGenericError as e:
        LOGGER.error(f'Error retrieving key fields: {e}', exc_info=True)
        return _server_error(api, request, headers, str(e))

    # Get provider locale (if any)
    prv_locale = l10n.get_plugin_locale(provider_def, request.raw_locale)

    uri = f'{api.get_collections_url()}/{dataset}'
    content = {
        'links': [
            {
                'type': FORMAT_TYPES[F_JSON],
                'rel': request.get_linkrel(F_JSON),
                'title': l10n.translate('This document as JSON', request.locale),  # noqa
                'href': f'{uri}/keys?f={F_JSON}'
            }, {
                'type': FORMAT_TYPES[F_JSONLD],
                'rel': request.get_linkrel(F_JSONLD),
                'title': l10n.translate('This document as RDF (JSON-LD)', request.locale),  # noqa
                'href': f'{uri}/keys?f={F_JSONLD}'
            }, {
                'type': FORMAT_TYPES[F_HTML],
                'rel': request.get_linkrel(F_HTML),
                'title': l10n.translate('This document as HTML', request.locale),  # noqa
                'href': f'{uri}/keys?f={F_HTML}'
            }
        ],
        'keys': []
    }

    for name, info in fields:
        content['keys'].append({
            'id': name,
            'type': info.get('type'),  # not always set (e.g. for ID)
            'isDefault': info['default'],
            'language': prv_locale.language  # TODO: is this really useful?
        })

    # Set response language to requested provider locale
    # (if it supports language) and/or otherwise the requested pygeoapi
    # locale (or fallback default locale)
    l10n.set_response_language(headers, prv_locale, request.locale)

    return headers, HTTPStatus.OK, to_json(content, api.pretty_print)
