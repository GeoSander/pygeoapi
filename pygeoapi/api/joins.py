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
from pygeoapi.plugin import load_plugin, PLUGINS
from pygeoapi.provider.base import ProviderTypeError
from pygeoapi.util import (
    get_provider_by_type, to_json, filter_providers_by_type,
    filter_dict_by_key_value, get_current_datetime
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
    'http://www.opengis.net/spec/ogcapi-joins-1/1.0/conf/output-geojson-direct',  # noqa
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

    :param cfg: pygeoapi configuration dict

    :returns: True if the join utility was initialized successfully
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

    if not join_util.init(cfg):
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
                    # {'$ref': '#/components/parameters/lang'},
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
                                                    'links'
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
                                                    'links': links_conf
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

        # Key value endpoint
        # See https://github.com/opengeospatial/ogcapi-joins/blob/master/sources/core/openapi/schemas/collectionKeyField.yaml  # noqa
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
                'operationId': f'get{k.capitalize()}KeyValues',
                'parameters': [
                    key_field_param,
                    {'$ref': '#/components/parameters/f'},
                    # {'$ref': '#/components/parameters/lang'},
                ],
                'responses': {
                    '200': {
                        'description': 'Response',
                        'content': {
                            'application/json': {
                                'schema': {
                                    'type': 'object',
                                    'required': [
                                        'keyField',
                                        'values',
                                        'links'
                                    ],
                                    'properties': {
                                        'links': links_conf,
                                        'keyField': {
                                            'type': 'string'
                                        },
                                        'values': {
                                            'type': 'array',
                                            'items': {
                                                'oneOf': [
                                                    {'type': 'integer'},
                                                    {'type': 'string'}
                                                ]
                                            }
                                        },
                                        'numberMatched': {
                                            'type': 'integer'
                                        },
                                        'numberReturned': {
                                            'type': 'integer'
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
            }
        }

        # Joins endpoints (list and create)
        paths[f'/collections/{k}/{API_NAME}'] = {
            'get': {
                'summary': 'Get all available joins',
                'description': 'Lists all available joins for this collection',
                'tags': [k, API_NAME],
                'operationId': f'get{k.capitalize()}Joins',
                'parameters': [
                    {'$ref': '#/components/parameters/f'},
                    # {'$ref': '#/components/parameters/lang'},
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
                'summary': 'Create a new join',
                'description': 'Creates a new left (outer) join based on the collection and provided parameters',  # noqa
                'tags': [k, API_NAME],
                'operationId': f'create{k.capitalize()}Join',
                'parameters': [
                    {'$ref': '#/components/parameters/f'},
                    # {
                    #     'in': 'header',
                    #     'name': 'Prefer',
                    #     'required': False,
                    #     'description': 'Indicates client preferences, including whether the client is capable of asynchronous processing.',  # noqa
                    #     'default': 'respond-async',
                    #     'schema': {
                    #         'type': 'string',
                    #         'enum': ['respond-async']
                    #     }
                    # }
                    # {'$ref': '#/components/parameters/lang'},
                ],
                'requestBody': {
                    'title': 'Join Parameters',
                    'description': 'CSV file and parameters required for the join operation.',  # noqa
                    'required': True,
                    'content': {
                        'multipart/form-data': {
                            'schema': {
                                'type': 'object',
                                'required': [
                                    # 'left-dataset-url',
                                    'collectionKey',  # 'left-dataset-key',
                                    # 'joinFileFormat',  # 'right-dataset-format',  # noqa
                                    'joinFile',  # 'right-dataset-file',
                                    'joinKey',  # 'right-dataset-key'
                                ],
                                'properties': {
                                    # 'left-dataset-url': {
                                    #     'type': 'string',
                                    #     'format': 'uri',
                                    #     'description': 'The URL for the OGC API collection (left dataset)',  # noqa
                                    #     'example': 'http://localhost:5000/collections/my-collection'  # noqa
                                    # },
                                    'collectionKey': {
                                        'type': 'string',
                                        'description': 'The primary key field in the left side dataset (collection) to join on'  # noqa
                                    },
                                    # TODO: part of joinFile/encoding already?
                                    # 'joinFileFormat': {
                                    #     'type': 'string',
                                    #     'description': 'The format (i.e. MIME type) of the right side dataset file to join',  # noqa
                                    #     'enum': ['text/csv'],
                                    #     'default': 'text/csv'
                                    # },
                                    'joinFile': {
                                        'type': 'string',
                                        'format': 'binary',
                                        'description': 'The right side dataset file to upload'  # noqa
                                    },
                                    'joinKey': {
                                        'type': 'string',
                                        'description': 'The foreign key field in the right side dataset that contains the key values for joining'  # noqa
                                    },
                                    # 'right-dataset-field-list'
                                    'joinFields': {
                                        'type': 'string',
                                        'description': 'Comma-separated list of field names from the right side dataset to include in the join result. If not specified, all fields are included.',  # noqa
                                        'example': 'name,population,area'
                                    },
                                    'csvDelimiter': {
                                        'type': 'string',
                                        'description': 'The delimiter character used in a CSV file (optional)',  # noqa
                                        'default': ',',
                                        'example': ','
                                    },
                                    'csvHeaderRow': {
                                        'type': 'integer',
                                        'description': 'The 1-based row number of the header row in a CSV file (optional)',  # noqa
                                        'default': 1,
                                        'minimum': 1
                                    },
                                    'csvDataStartRow': {
                                        'type': 'integer',
                                        'description': 'The 1-based row number where data starts in a CSV file (optional)',  # noqa
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
                        'description': 'Started asynchronous creation of join table.',  # noqa
                        'headers': {
                            'Location': {
                                'schema': {
                                    'type': 'string',
                                },
                                'description': 'The URL to check the status of the join.',  # noqa
                            },
                            'Preference-Applied': {
                                'schema': {
                                    'type': 'string'
                                },
                                'description': 'The preference applied to execute the join asynchronously (see RFC 2740).'  # noqa
                            }
                        },
                        'content': {
                            'application/json': {
                                'schema': {
                                    'type': 'object',
                                    'required': [
                                        'joinID',
                                        'status',
                                    ],
                                    'properties': {
                                        'joinID': {
                                            'type': 'string',
                                        },
                                        'status': {
                                            'type': 'string',
                                            'enum': [
                                                'accepted',
                                                'running',
                                                'successful',
                                                'failed',
                                                'dismissed'
                                            ]
                                        },
                                        # 'message': {
                                        #     'type': 'string',
                                        # },
                                        # 'created': {
                                        #     'type': 'string',
                                        #     'format': 'date-time'
                                        # },
                                        # 'started': {
                                        #     'type': 'string',
                                        #     'format': 'date-time'
                                        # },
                                        # 'finished': {
                                        #     'type': 'string',
                                        #     'format': 'date-time'
                                        # },
                                        # 'updated': {
                                        #     'type': 'string',
                                        #     'format': 'date-time'
                                        # },
                                        # 'progress': {
                                        #     'type': 'integer',
                                        #     'minimum': 0,
                                        #     'maximum': 100
                                        # },
                                        # 'links': links_conf
                                    }
                                }
                                # 'schema': {
                                #     'type': 'object',
                                #     'properties': {
                                #         'id': {
                                #             'type': 'string',
                                #             'description': 'Unique identifier for the created join',  # noqa
                                #             'example': 'leftDatasetName-rightDatasetName-1'  # noqa
                                #         },
                                #         'metadata': {
                                #             'type': 'object',
                                #             'properties': {
                                #                 'id': {'type': 'string'},
                                #                 'created': {
                                #                     'type': 'string',
                                #                     'format': 'date-time'
                                #                 },
                                #                 'leftDataset': {
                                #                     'type': 'object',
                                #                     'properties': {
                                #                         'name': {'type': 'string'},  # noqa
                                #                         'key': {'type': 'string'}  # noqa
                                #                     }
                                #                 },
                                #                 'rightDataset': {
                                #                     'type': 'object',
                                #                     'properties': {
                                #                         'name': {'type': 'string'},  # noqa
                                #                         'key': {'type': 'string'},  # noqa
                                #                         'format': {'type': 'string'},  # noqa
                                #                         'delimiter': {'type': 'string'},  # noqa
                                #                         'headerRow': {'type': 'integer'},  # noqa
                                #                         'dataStartRow': {'type': 'integer'}  # noqa
                                #                     }
                                #                 },
                                #                 'parameters': {
                                #                     'type': 'object',
                                #                     'properties': {
                                #                         'includeFields': {
                                #                             'type': 'array',
                                #                             'items': {'type': 'string'}  # noqa
                                #                         }
                                #                     }
                                #                 },
                                #                 'result': {
                                #                     'type': 'object',
                                #                     'properties': {
                                #                         'path': {'type': 'string'},  # noqa
                                #                         'format': {'type': 'string'}  # noqa
                                #                     }
                                #                 },
                                #                 'statistics': {
                                #                     'type': 'object',
                                #                     'properties': {
                                #                         'numberMatched': {
                                #                             'type': 'integer',  # noqa
                                #                             'description': 'Number of left dataset records matched with right dataset'  # noqa
                                #                         },
                                #                         'numberOfUnmatchedLeftItems': {  # noqa
                                #                             'type': 'integer',  # noqa
                                #                             'description': 'Number of left dataset records without a match'  # noqa
                                #                         },
                                #                         'numberOfUnmatchedRightItems': {  # noqa
                                #                             'type': 'integer',  # noqa
                                #                             'description': 'Number of right dataset records without a match'  # noqa
                                #                         }
                                #                     }
                                #                 }
                                #             }
                                #         }
                                #     }
                                # }
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
            'description': 'Join identifier',
            'required': True,
            'schema': {
                'type': 'string'
            }
        }
        paths[f'/collections/{k}/{API_NAME}/{{joinId}}'] = {
            'get': {
                'summary': 'Get join status',
                'description': 'Returns the status of an executed join',
                'tags': [k, API_NAME],
                'operationId': f'get{k.capitalize()}JoinStatus',
                'parameters': [
                    join_id_param,
                    {'$ref': '#/components/parameters/f'},
                    # {'$ref': '#/components/parameters/lang'},
                ],
                'responses': {
                    '200': {
                        'description': 'Response',
                        'content': {
                            'application/json': {
                                'schema': {
                                    'type': 'object'
                                }
                            }
                        }
                    }
                },
                '400': {'$ref': '#/components/responses/BadRequest'},
                '404': {'$ref': '#/components/responses/NotFound'},
                '500': {'$ref': '#/components/responses/ServerError'}
            },
            'delete': {
                'summary': 'Delete a join with the given id',
                'description': 'Deletes a join with the given id',
                'tags': [k, API_NAME],
                'operationId': f'delete{k.capitalize()}Join',
                'parameters': [
                    join_id_param,
                    {'$ref': '#/components/parameters/f'},
                    # {'$ref': '#/components/parameters/lang'},
                ],
                'responses': {
                    '200': {
                        'description': 'Response',
                        'content': {
                            'application/json': {
                                'schema': {
                                    'type': 'object'  # TODO
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

        # Join results endpoint
        paths[f'/collections/{k}/{API_NAME}/{{joinId}}/results'] = {
            'get': {
                'summary': 'Get results for a join with the given id',
                'description': 'Returns the output data of an established join',  # noqa
                'tags': [k, API_NAME],
                'operationId': f'get{k.capitalize()}JoinResult',
                'parameters': [
                    join_id_param,
                    {'$ref': '#/components/parameters/f'},
                    # {'$ref': '#/components/parameters/lang'},
                ],
                'responses': {
                    '200': {
                        'description': 'Response',
                        'content': {
                            'application/json': {
                                'schema': {
                                    'type': 'object'
                                }
                            }  # TODO: paginated response
                        }
                    }
                }
            }
        }

    return [{'name': API_NAME}], {'paths': paths}


def _prepare(api: API, request: APIRequest,
             dataset: str) -> tuple[dict, dict, str]:
    """
    Prepare headers, collections, and dataset for API response handling.

    :param api: API instance
    :param request: A request object
    :param dataset: Dataset name / collection path

    :returns: tuple of headers, collections, dataset name
    """

    headers = request.get_response_headers(SYSTEM_LOCALE, **api.api_headers)
    collections = filter_dict_by_key_value(api.config['resources'],
                                           'type', 'collection')
    dataset = dataset.removeprefix(api.get_collections_url()).strip('/')
    return headers, collections, dataset


def list_joins(api: API, request: APIRequest,
               dataset: str) -> tuple[dict, int, str]:
    """
    Returns all available joins from the server

    :param api: API instance
    :param request: A request object
    :param dataset: Dataset name (not used in this implementation)

    :returns: tuple of headers, status code, content
    """
    headers, collections, dataset = _prepare(api, request, dataset)

    if dataset not in collections:
        msg = f'Collection not found: {dataset}'
        return api.get_exception(
            HTTPStatus.NOT_FOUND, headers, request.format,
            'NotFound', msg
        )

    try:
        sources = join_util.list_sources(dataset)
    except Exception as e:
        LOGGER.error(str(e), exc_info=True)
        return api.get_exception(
            HTTPStatus.INTERNAL_SERVER_ERROR, headers, request.format,
            'NoApplicableCode', str(e)
        )

    if not sources:
        return api.get_exception(
            HTTPStatus.NOT_FOUND, headers, request.format, 'NotFound',
            f'No joins found for collection: {dataset}'
        )

    # Build the joins list with proper structure
    joins_list = []
    for source_id, source_obj in sources:
        join_item = {
            'id': source_id,
            'timeStamp': source_obj['timeStamp'],
            'links': [
                {
                    'href': f"{api.get_collections_url()}/{dataset}/joins/{source_id}",  # noqa
                    'rel': 'self',
                    'type': 'application/json',
                    'title': f'Metadata for join source {source_id}'
                },
                {
                    'href': f"{api.get_collections_url()}/{dataset}/items?joinId={source_id}",  # noqa
                    'rel': 'results',
                    'type': 'application/geo+json',
                    'title': f'Join {source_id} applied to {dataset}'
                }
            ]
        }
        joins_list.append(join_item)

    # Build the response with proper structure
    # TODO improve this response for all formats and pagination
    response = {
        'links': [
            {
                'href': f'{api.base_url}/joins?f=json',
                'rel': 'self',
                'type': 'application/json',
                'title': 'This document as JSON'
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
                 dataset: str, join_id: str) -> tuple[dict, int, str]:
    """
    Returns the metadata of a specific join source on the server

    :param api: API instance
    :param request: A request object
    :param dataset: Dataset name (not used in this implementation)
    :param join_id: The id of the join to retrieve metadata for

    :returns: tuple of headers, status code, content
    """
    headers, collections, dataset = _prepare(api, request, dataset)

    if dataset not in collections:
        msg = f'Collection not found: {dataset}'
        return api.get_exception(
            HTTPStatus.NOT_FOUND, headers, request.format,
            'NotFound', msg
        )

    try:
        metadata = join_util.read_join_source(dataset, join_id)

        response = {
            'links': [
                # TODO: HTML and so on
                {
                    'href': f'{api.base_url}/joins/{join_id}?f=json',
                    'rel': 'self',
                    'type': 'application/json',
                    'title': 'This document as JSON'
                }
            ],
            # TODO: proper response format
            'joinInfo': metadata
        }

    except ValueError as e:
        LOGGER.error(f'Invalid request parameter: {e}', exc_info=True)
        return api.get_exception(
            HTTPStatus.BAD_REQUEST, headers, request.format,
            'InvalidParameterValue', str(e))
    except KeyError as e:
        msg = 'Collection or join source not found'
        LOGGER.error(f'Invalid parameter value: {e}', exc_info=True)
        return api.get_exception(
            HTTPStatus.NOT_FOUND, headers, request.format,
            'NotFound', msg)
    except Exception as e:
        LOGGER.error(f'Failed to retrieve join: {e}', exc_info=True)
        msg = f'Failed to retrieve join: {str(e)}'
        return api.get_exception(
            HTTPStatus.INTERNAL_SERVER_ERROR, headers, F_JSON,
            'NoApplicableCode', msg
        )

    # Set response language to requested provider locale
    # (if it supports language) and/or otherwise the requested pygeoapi
    # locale (or fallback default locale)
    l10n.set_response_language(headers, request.locale)

    return headers, HTTPStatus.OK, to_json(response, api.pretty_print)


def create_join(api: API, request: APIRequest,
                dataset: str) -> tuple[dict, int, Any]:
    """
    Creates a new join on the server.

    :param api: API instance
    :param request: A request object
    :param dataset: Dataset name (not used in this implementation)

    :returns: tuple of headers, status code, content
    """
    headers, collections, dataset = _prepare(api, request, dataset)

    if dataset not in collections:
        msg = f'Collection not found: {dataset}'
        return api.get_exception(
            HTTPStatus.NOT_FOUND, headers, request.format,
            'NotFound', msg
        )

    if not request.is_valid(PLUGINS['formatter'].keys()):
        return api.get_format_exception(request)

    try:
        # Get collection provider
        try:
            provider_def = get_provider_by_type(
                collections[dataset]['providers'], 'feature'
            )
            provider = load_plugin('provider', provider_def)
        except ProviderTypeError:
            msg = f'Feature provider not found for collection: {dataset}'
            return api.get_exception(
                HTTPStatus.BAD_REQUEST, headers, request.format,
                'NoApplicableCode', msg
            )

        # Get provider locale (if any)
        prv_locale = l10n.get_plugin_locale(provider_def, request.raw_locale)

        details = join_util.process_csv(dataset, provider, request.form)
        response = {
            'links': [
                # TODO: HTML and so on
                # {
                #     'href': f'{api.base_url}/joins/{details['id']}?f=json',
                #     'rel': 'self',
                #     'type': 'application/json',
                #     'title': 'This document as JSON'
                # }
            ],
            # TODO: proper response format
            'joinInfo': details
        }

    except ValueError as e:
        LOGGER.error(f'Invalid request parameter: {e}', exc_info=True)
        return api.get_exception(
            HTTPStatus.BAD_REQUEST, headers, request.format,
            'InvalidParameterValue', str(e))
    except KeyError as e:
        msg = 'Collection or join source not found'
        LOGGER.error(f'Invalid parameter value: {e}', exc_info=True)
        return api.get_exception(
            HTTPStatus.NOT_FOUND, headers, request.format,
            'NotFound', msg)
    except Exception as e:
        LOGGER.error(f'Failed to retrieve join: {e}', exc_info=True)
        msg = f'Failed to retrieve join: {str(e)}'
        return api.get_exception(
            HTTPStatus.INTERNAL_SERVER_ERROR, headers, F_JSON,
            'NoApplicableCode', msg
        )

    # Set response language to requested provider locale
    # (if it supports language) and/or otherwise the requested pygeoapi
    # locale (or fallback default locale)
    l10n.set_response_language(headers, prv_locale, request.locale)

    return headers, HTTPStatus.OK, to_json(response, api.pretty_print)


def delete_join(api: API, request: APIRequest,
                dataset: str, join_id: str) -> tuple[dict, int, str]:
    """
    Removes a specific join source from the server.

    :param api: API instance
    :param request: A request object
    :param dataset: dataset name
    :param join_id: The id of the join to remove

    :returns: tuple of headers, status code, content
    """
    headers, collections, dataset = _prepare(api, request, dataset)

    if dataset not in collections:
        msg = f'Collection not found: {dataset}'
        return api.get_exception(
            HTTPStatus.NOT_FOUND, headers, request.format,
            'NotFound', msg
        )

    try:
        if not join_util.remove_source(dataset, join_id):
            msg = f'Join source {join_id} not found for collection {dataset}'
            return api.get_exception(
                HTTPStatus.NOT_FOUND, headers, request.format,
                'NotFound', msg)
    except ValueError as e:
        LOGGER.error(f'Invalid request parameter: {e}', exc_info=True)
        return api.get_exception(
            HTTPStatus.BAD_REQUEST, headers, request.format,
            'InvalidParameterValue', str(e))
    except Exception as e:
        LOGGER.error(f'Failed to delete join source: {e}',
                     exc_info=True)
        msg = f'Failed to delete join: {str(e)}'
        return api.get_exception(
            HTTPStatus.INTERNAL_SERVER_ERROR, headers, request.format,
            'NoApplicableCode', msg)

    # Set response language to requested provider locale
    # (if it supports language) and/or otherwise the requested pygeoapi
    # locale (or fallback default locale)
    l10n.set_response_language(headers, request.locale)

    # TODO: return JSON
    return headers, HTTPStatus.OK, 'Join source deleted.'


def key_fields(api: API, request: APIRequest,
               dataset: str) -> tuple[dict, int, str]:
    """
    Returns all available join key fields for a collection

    :param api: API instance
    :param request: A request object
    :param dataset: dataset name

    :returns: tuple of headers, status code, content
    """
    headers, collections, dataset = _prepare(api, request, dataset)

    if dataset not in collections:
        msg = f'Collection not found: {dataset}'
        return api.get_exception(
            HTTPStatus.NOT_FOUND, headers, request.format,
            'NotFound', msg
        )

    LOGGER.debug(f"Retrieving key field configuration "
                 f"for collection '{dataset}'")

    provider_def = get_provider_by_type(
        collections[dataset]['providers'], 'feature')
    fields = join_util.collection_keys(provider_def, dataset)

    # Get provider locale (if any)
    prv_locale = l10n.get_plugin_locale(provider_def, request.raw_locale)

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

    for field in fields:
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
                    'href': f'{api.get_collections_url()}/keys/{field_id}?f={F_JSON}'  # noqa
                }, {
                    'type': FORMAT_TYPES[F_JSONLD],
                    'rel': request.get_linkrel(F_JSONLD),
                    'title': l10n.translate('The key values as RDF (JSON-LD)', request.locale),  # noqa
                    'href': f'{api.get_collections_url()}/keys/{field_id}?f={F_JSONLD}'  # noqa
                }, {
                    'type': FORMAT_TYPES[F_HTML],
                    'rel': request.get_linkrel(F_HTML),
                    'title': l10n.translate('The key values as HTML', request.locale),  # noqa
                    'href': f'{api.get_collections_url()}/keys/{field_id}?f={F_HTML}'  # noqa
                }
            ]
        })

    # Set response language to requested provider locale
    # (if it supports language) and/or otherwise the requested pygeoapi
    # locale (or fallback default locale)
    l10n.set_response_language(headers, prv_locale, request.locale)

    return headers, HTTPStatus.OK, to_json(content, api.pretty_print)
