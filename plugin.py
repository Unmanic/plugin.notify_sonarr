#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
    unmanic-plugins.plugin.py

    Written by:               Josh.5 <jsunnex@gmail.com>
    Date:                     27 Feb 2022, (12:22 PM)

    Copyright:
        Copyright (C) 2021 Josh Sunnex

        This program is free software: you can redistribute it and/or modify it under the terms of the GNU General
        Public License as published by the Free Software Foundation, version 3.

        This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
        implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
        for more details.

        You should have received a copy of the GNU General Public License along with this program.
        If not, see <https://www.gnu.org/licenses/>.

"""
import json
import logging
import os
import pprint
import time

import requests

import humanfriendly
from unmanic.libs.unplugins.settings import PluginSettings

# Configure plugin logger
logger = logging.getLogger("Unmanic.Plugin.notify_sonarr")


class Settings(PluginSettings):
    settings = {
        'host_url':                  'http://localhost:8989',
        'api_key':                   '',
        'mode':                      'update_mode',
        'limit_import_on_file_size': True,
        'minimum_file_size':         '100MB',
    }

    def __init__(self, *args, **kwargs):
        super(Settings, self).__init__(*args, **kwargs)
        self.form_settings = {
            "host_url":                  {
                "label": "Sonarr LAN IP Address",
            },
            "api_key":                   {
                "label": "Sonarr API Key",
            },
            "mode":                      {
                "label":          "Mode",
                "input_type":     "select",
                "select_options": [
                    {
                        'value': "update_mode",
                        'label': "Trigger series refresh on task complete",
                    },
                    {
                        'value': "import_mode",
                        'label': "Import episode on task complete",
                    },
                ],
            },
            "limit_import_on_file_size": self.__set_limit_import_on_file_size(),
            "minimum_file_size":         self.__set_minimum_file_size(),
        }

    def __set_limit_import_on_file_size(self):
        values = {
            "label": "Limit file import size",
        }
        if self.get_setting('mode') != 'import_mode':
            values["display"] = 'hidden'
        return values

    def __set_minimum_file_size(self):
        values = {
            "label": "Minimum file size",
        }
        if self.get_setting('mode') != 'import_mode':
            values["display"] = 'hidden'
        if not self.get_setting('limit_import_on_file_size'):
            values["display"] = 'hidden'
        return values


class Session(object):
    """
    Rolling my own class for connecting to the Arr API.
    Some Docker containers were giving issues with the session token when using pyarr.
    We can just add apiKey=? to the URL for what we are doing here...
    """

    def __init__(self, host_url, api_key):
        self.api_path = '/sonarr/api/v3'
        self.host_url = host_url
        self.api_key = api_key

    @staticmethod
    def __get(url, params=None):
        headers = {
            'Content-Type': 'application/json',
        }
        r = requests.get(url, headers=headers, params=params)
        if r.status_code == 200:
            return r.json()
        return {}

    @staticmethod
    def __post(url, data=None):
        headers = {
            'Content-Type': 'application/json',
        }
        r = requests.post(url, headers=headers, json=data, timeout=5)
        if r.status_code == 200:
            return r.json()
        return {}

    def get_parsed_title(self, title):
        # http://{host_url}/{api_path}}/parse?apiKey=1234567890&title=SomeTitle
        path = "/parse"
        params = {
            "apiKey": self.api_key,
            "title":  title,
        }
        url = "{host}{api}{path}".format(host=self.host_url.rstrip('/'), api=self.api_path, path=path)
        return self.__get(url, params=params)

    def get_queue(self):
        # http://{host_url}/{api_path}}/queue?apiKey=1234567890&page=1&pageSize=9999
        path = "/queue"
        params = {
            "apiKey":   self.api_key,
            "page":     '1',
            "pageSize": '9999',
        }
        url = "{host}{api}{path}".format(host=self.host_url.rstrip('/'), api=self.api_path, path=path)
        return self.__get(url, params=params)

    def post_command(self, name, **kwargs):
        # http://{host_url}/{api_path}}/command?apiKey=1234567890
        path = "/command?apiKey={api_key}".format(api_key=self.api_key)
        data = {
            "name": name,
            **kwargs,
        }
        url = "{host}{api}{path}".format(host=self.host_url.rstrip('/'), api=self.api_path, path=path)
        return self.__post(url, data=data)


def check_file_size_under_max_file_size(path, minimum_file_size):
    file_stats = os.stat(os.path.join(path))
    if int(humanfriendly.parse_size(minimum_file_size)) < int(file_stats.st_size):
        return False
    return True


def update_mode(api, dest_path):
    basename = os.path.basename(dest_path)

    # Fetch episode data
    episode_data = api.get_parsed_title(basename)

    # Fetch a series ID from Sonarr
    series_title = episode_data.get('series', {}).get('title')
    series_id = episode_data.get('series', {}).get('id')
    if not series_id:
        logger.error("Missing series ID. Failed to queued refresh of series for file: '{}'".format(dest_path))
        return

    # Run API command for RescanSeries
    #   - RescanSeries with a series ID
    result = api.post_command('RescanSeries', seriesId=series_id)
    if result.get('message'):
        logger.error("Failed to queued refresh of series ID '{}' for file: '{}'".format(series_id, dest_path))
        return
    logger.info("Successfully queued refreshed the Series '{}' for file: '{}'".format(series_title, dest_path))


def import_mode(api, source_path, dest_path):
    source_basename = os.path.basename(source_path)

    download_id = None
    episode_title = None

    queue = api.get_queue()
    message = pprint.pformat(queue, indent=1)
    logger.debug("Current queue \n{}".format(message))
    for item in queue.get('records', []):
        item_output_basename = os.path.basename(item.get('outputPath'))
        if item_output_basename == source_basename:
            download_id = item.get('downloadId')
            episode_title = item.get('title')
            break

    # Run import
    logger.warning("Sending path '{}'".format(dest_path))
    if download_id:
        # Run API command for DownloadedEpisodesScan
        #   - DownloadedEpisodesScan with a path and download_client_id
        logger.info("Queued import episode '{}' using download_client_id: '{}'".format(episode_title, download_id))
        result = api.post_command('DownloadedEpisodesScan', path=dest_path, download_client_id=download_id)
    else:
        # Run API command for DownloadedEpisodesScan without passing a download_client_id
        #   - DownloadedEpisodesScan with a path and download_client_id
        logger.info("Queued import using just the file path '{}'".format(dest_path))
        result = api.post_command('DownloadedEpisodesScan', path=dest_path)

    # Log results
    message = result
    if isinstance(result, dict) or isinstance(result, list):
        message = pprint.pformat(result, indent=1)
    logger.debug("Queued import result \n{}".format(message))
    if (isinstance(result, dict)) and result.get('message'):
        logger.error("Failed to queued import of file: '{}'".format(dest_path))
        return
    # TODO: Check for other possible outputs
    logger.info("Successfully queued import of file: '{}'".format(dest_path))


def process_files(settings, source_file, destination_files, host_url, api_key):
    api = Session(host_url, api_key)

    mode = settings.get_setting('mode')

    # Get the basename of the file
    for dest_file in destination_files:
        if mode == 'update_mode':
            update_mode(api, dest_file)
        elif mode == 'import_mode':
            if settings.get_setting('limit_import_on_file_size'):
                minimum_file_size = settings.get_setting('minimum_file_size')
                if check_file_size_under_max_file_size(dest_file, minimum_file_size):
                    # Ignore this file
                    logger.info("Ignoring file as it is under configured minimum size file: '{}'".format(dest_file))
                    continue
            import_mode(api, source_file, dest_file)


def on_postprocessor_task_results(data):
    """
    Runner function - provides a means for additional postprocessor functions based on the task success.

    The 'data' object argument includes:
        library_id                      - The library that the current task is associated with
        task_processing_success         - Boolean, did all task processes complete successfully.
        file_move_processes_success     - Boolean, did all postprocessor movement tasks complete successfully.
        destination_files               - List containing all file paths created by postprocessor file movements.
        source_data                     - Dictionary containing data pertaining to the original source file.

    :param data:
    :return:

    """
    # Configure settings object (maintain compatibility with v1 plugins)
    if data.get('library_id'):
        settings = Settings(library_id=data.get('library_id'))
    else:
        settings = Settings()

    # Fetch destination and source files
    source_file = data.get('source_data', {}).get('abspath')
    destination_files = data.get('destination_files', [])

    # Setup API
    host_url = settings.get_setting('host_url')
    api_key = settings.get_setting('api_key')
    process_files(settings, source_file, destination_files, host_url, api_key)
