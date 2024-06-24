#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
ServiceNow CMDB connector for Elastic Enterprise Search.

"""

import requests
import os
import json
import asyncio
from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.es.sink import OP_DELETE, OP_INDEX
from connectors.config import load_config

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from elasticsearch import Elasticsearch


DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "../..", "config.yml")
es_conf = load_config(DEFAULT_CONFIG)

# Elasticsearch  credentials for caching incremental cursor
es_host = es_conf['elasticsearch']['host']
es_api_key = es_conf['elasticsearch']['api_key']
cafile_path = es_conf['elasticsearch']['ca_certs']

CURSOR_TABLE_KEY = 'cursor_table'
INDEX_NAME = 'sync_cursor_index'

sn_headers = {"Accept": "application/json"}
CURSOR_TABLE_KEY = "sncmdb"
class SyncCursorEmpty(Exception):
    """Exception class to notify that incremental sync can't run because sync_cursor is empty.
    See: https://learn.microsoft.com/en-us/graph/delta-query-overview
    """

    pass

class SncmdbDataSource(BaseDataSource):
    """ServiceNow CMDB Connector"""
    incremental_sync_enabled = True
    def __init__(self, configuration):
        super().__init__(configuration=configuration)

    @classmethod
    def get_default_configuration(cls):
        # Calculate the default date of one year ago today.
        one_year_ago_date = datetime.now() - relativedelta(years=1)
        def_start_date = one_year_ago_date.strftime('%Y-%m-%d %H:%M:%S')
        # Default configuration UI fields rendered in Kibana.
        return {
            "domain": {
                "order": 1,
                "value": "dev195660.service-now.com",
                "label": "ServiceNow Domain",
                "type": "str"
            },
            "user": {
                "order": 2,
                "value": "admin",
                "label": "User",
                "type": "str"
            },
            "password": {
                "order": 3,
                "label": "Password",
                "type": "str",
                "sensitive": True,
                "value": "kQ7f-sAw!S1A"
            },
            "sn_items": {
                "order": 4,
                "display": "textarea",
                "value": "cmdb_ci_linux_server",
                "label": "Comma separated list of ServiceNow tables",
                "type": "list"
            },
             "start_date": {
                "order": 5,
                "value": def_start_date,
                "label": "Start Date (defaults to 1 year ago)",
                "tooltip": "format: YYYY-MM-DD HH:MM:SS, e.g. 2023-06-21 15:45:30",
                "type": "str",
                "required": False
            }
        }

    async def ping(self):
        cfg = self.configuration
        url = 'https://%s/api/now/table/%s' % (cfg["domain"],
                                               cfg["sn_items"][0])
        sn_params = {'sysparm_limit': '1',
             'sysparm_display_value': 'true',
             'sysparm_exclude_reference_link': 'true', }
        try:
            resp = requests.get(url, params=sn_params,
                                auth=(cfg["user"], cfg["password"]),
                                headers=sn_headers, stream=True)
        except Exception:
            logger.exception("Error while connecting to the ServiceNow.")
            return False
        if resp.status_code != 200:
            logger.exception("Error while connecting to the ServiceNow.")
            raise NotImplementedError
        return True
    
    # Helper functions.
    def _clean_empty(self, data):
        if data is None:
            return None
        res_data = [{item: value for item, value in row.items() if value}
                    for row in data]
        return res_data

    def _string_to_datetime(self, date_string):
        return datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
    

    def init_sync_cursor(self):
        self._sync_cursor = {CURSOR_TABLE_KEY: {}}
        return self._sync_cursor


    def _check_cache(self, sn_table):
        es = Elasticsearch(es_host,
                    ca_certs=cafile_path,
                    api_key=(es_api_key))
        res = es.get(index=INDEX_NAME, id=1)
        self._sync_cursor = res['_source']
        return self._sync_cursor[CURSOR_TABLE_KEY].get(sn_table, None)

       
    def _get_cached_tables(self):
        es = Elasticsearch(es_host,
                    ca_certs=cafile_path,
                    api_key=(es_api_key))
        res = es.get(index=INDEX_NAME, id=1)
        self._sync_cursor = res['_source']
        return list(self._sync_cursor[CURSOR_TABLE_KEY].keys())
    

    def _write_cache(self, sn_table, running_sys_updated_on):
        self._sync_cursor[CURSOR_TABLE_KEY][sn_table] = running_sys_updated_on.strftime('%Y-%m-%d %H:%M:%S')
        es = Elasticsearch(es_host,
                    ca_certs=cafile_path,
                    api_key=(es_api_key))
        es.index(index=INDEX_NAME, id=1, body=self._sync_cursor)
        logger.info(f"writing cursor: {self._sync_cursor}")
       

    # Main run loop.
    async def get_docs(self, filtering=None):
        self.init_sync_cursor()
        cfg = self.configuration
        tables_offset = {}
        sysparm_limit = 1000
        running_sys_updated_on = ""
        while True:
            sn_tables = cfg['sn_items']
            for sn_table in sn_tables:
                # Skip if table it is already fully synced.
                if sn_table in list(self._sync_cursor[CURSOR_TABLE_KEY].keys()):
                    continue
                # Set the offset to 0 per table if this is the first pass.
                if sn_table not in tables_offset:
                    tables_offset[sn_table] = 0
                logger.info(f"parsing table: {sn_table}")
                max_sys_updated_on = cfg['start_date']
                sn_params = {
                    'sysparm_limit': sysparm_limit,
                    'sysparm_offset': tables_offset[sn_table],
                    'sysparm_display_value': 'true',
                    'sysparm_exclude_reference_link': 'true',
                    'sysparm_query': 'sys_updated_on>=' + max_sys_updated_on + '^ORDERBYsys_updated_on'
                }
                logger.debug(f'service_now request for table: {sn_table} :{sn_params}')
                url = f'https://{cfg["domain"]}/api/now/table/{sn_table}'
                resp = requests.get(url, params=sn_params, auth=(cfg["user"],
                                    cfg["password"]), headers=sn_headers,
                                    stream=True)
                if resp.status_code != 200:
                    logger.warning(f"Status: {resp.status_code} Headers: {resp.headers} Error Response: {resp.json()}")
                    raise NotImplementedError
                # print(resp.text)
                data = resp.json()
                if data is not None:
                    table = self._clean_empty(data['result'])
                    for row in table:
                        try:
                            row['_id'] = row['sys_id']
                            row['url.domain'] = cfg["domain"]
                            lazy_download = None
                            doc = row, lazy_download
                            # Track the latest sys_updated_on value for caching
                            this_sys_update_ts = self._string_to_datetime(row['sys_updated_on'])
                            max_sys_updated_on_ts = self._string_to_datetime(max_sys_updated_on)
                            if this_sys_update_ts > max_sys_updated_on_ts:
                                running_sys_updated_on = this_sys_update_ts
                            yield doc
                        except Exception as err:
                            logger.error(f"Error processing: {row} Exception: {err}")

                # Update the offset for the next page
                tables_offset[sn_table] += sysparm_limit
                if len(data['result']) < sysparm_limit:
                    # Sync on this table is finished, save the latest sys_updated_on value for the next sync.
                    if not running_sys_updated_on == "":
                        self._write_cache(sn_table, running_sys_updated_on)
                    else:
                        self._write_cache(sn_table, None)

            # Once all tables are cached, exit/break out
            cached_table = self._get_cached_tables()
            sn_tables.sort()
            cached_table.sort()
            logger.debug(f"sntables: {sn_tables}")
            logger.debug(f"cachtbls: {cached_table}")
            if sn_tables == cached_table:
                logger.info(f"full sync complete")
                break


    async def get_docs_incrementally(self, sync_cursor, filtering=None):
        self._sync_cursor = sync_cursor
        if not self._sync_cursor:
            raise SyncCursorEmpty(
                "Unable to start incremental sync. Please perform a full sync to re-enable incremental syncs."
            )
        cfg = self.configuration
        tables_offset = {}
        sysparm_limit = 1000
        running_sys_updated_on = ""
        while True:
            sn_tables = cfg['sn_items']
            for sn_table in sn_tables:
                # Skip if table it is already fully synced.
                logger.info(f"parsing table: {sn_table}")
                # Set the offset to 0 per table if this is the first pass.
                if sn_table not in tables_offset:
                    tables_offset[sn_table] = 0
                # Read the latest sys_updated_on value if it was cached from the last sync.
                max_sys_updated_on = self._check_cache(sn_table)
                if max_sys_updated_on is not None:
                    logger.info(f'found state with date {max_sys_updated_on}')
                else:
                    raise SyncCursorEmpty(
                        "Unable to start incremental sync. Please perform a full sync to re-enable incremental syncs."
                    )
                sn_params = {
                    'sysparm_limit': sysparm_limit,
                    'sysparm_offset': tables_offset[sn_table],
                    'sysparm_display_value': 'true',
                    'sysparm_exclude_reference_link': 'true',
                    'sysparm_query': 'sys_updated_on>=' + max_sys_updated_on + '^ORDERBYsys_updated_on'
                }
                logger.debug(f'service_now request for table: {sn_table} :{sn_params}')
                url = f'https://{cfg["domain"]}/api/now/table/{sn_table}'
                resp = requests.get(url, params=sn_params, auth=(cfg["user"],
                                    cfg["password"]), headers=sn_headers,
                                    stream=True)
                if resp.status_code != 200:
                    logger.warning(f"Status: {resp.status_code} Headers: {resp.headers} Error Response: {resp.json()}")
                    raise NotImplementedError
                data = resp.json()
                if data is not None:
                    table = self._clean_empty(data['result'])
                    for row in table:
                        try:
                            row['_id'] = row['sys_id']
                            row['url.domain'] = cfg["domain"]
                            doc = row
                            # Track the latest sys_updated_on value for caching
                            this_sys_update_ts = self._string_to_datetime(row['sys_updated_on'])
                            max_sys_updated_on_ts = self._string_to_datetime(max_sys_updated_on)
                            if this_sys_update_ts > max_sys_updated_on_ts:
                                running_sys_updated_on = this_sys_update_ts
                            yield doc, None, OP_INDEX
                        except Exception as err:
                            logger.error(f"Error processing: {row} Exception: {err}")
                
                # Update the offset for the next page
                tables_offset[sn_table] += sysparm_limit
            if len(data['result']) < sysparm_limit:
                # Sync on this table is finished, save the latest sys_updated_on value for the next sync.
                if not running_sys_updated_on == "":
                    self._write_cache(sn_table, running_sys_updated_on)
                break
            