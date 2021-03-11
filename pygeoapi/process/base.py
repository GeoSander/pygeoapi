# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
#
# Copyright (c) 2019 Tom Kralidis
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

from pygeoapi import l10n

LOGGER = logging.getLogger(__name__)


class BaseProcessor:
    """generic Processor ABC. Processes are inherited from this class"""

    def __init__(self, processor_def, process_metadata, requested_locale: str = None):  # noqa
        """
        Initialize object

        :param processor_def:       processor definition
        :param process_metadata:    process metadata `dict`
        :param requested_locale:    requested process locale

        :returns: pygeoapi.processor.base.BaseProvider
        """
        self.name = processor_def['name']
        self.metadata = process_metadata

        # locale support
        self.locale = l10n.get_plugin_locale(processor_def, requested_locale)

    def execute(self, data):
        """
        execute the process

        :returns: tuple of MIME type and process response
        """

        raise NotImplementedError()

    def __repr__(self):
        return '<BaseProcessor> {}'.format(self.name)


class ProcessorGenericError(Exception):
    """processor generic error"""
    pass


class ProcessorExecuteError(ProcessorGenericError):
    """query / backend error"""
    pass
