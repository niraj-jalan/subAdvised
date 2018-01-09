from datetime import datetime
import ast
import logging
import logging.config
import re

from abc import ABC, abstractmethod
import json

from DBUtils import *
from CashFlow import CashFlow

CONF_LOGGING_INI = '../conf/logging.ini'


class BaseParser(ABC):
    def __init__(self, config):
        super().__init__()
        self._env = config.get('APP', 'env')
        self._file_name = None
        self._config = config
        self._iso_currency_map = DBUtils.get_data_map(config, self._env, 'get_currency_by_iso_code', None)
        self._account_by_custodian_map = DBUtils.get_data_map(config, self._env, 'get_account_by_custodian_code', None)

    @abstractmethod
    def parse(self):
        pass

    def save_to_database(self, data_list):
        sql_insert_param_list = []
        for cash_flow_rec in data_list:
            row = []
            row.append(float(cash_flow_rec.cash_flow_amount))
            row.append(int(cash_flow_rec.settlement_date))
            row.append(int(cash_flow_rec.account_id))
            row.append(int(cash_flow_rec.cash_flow_type))
            row.append('Batch')  # posted by user
            row.append(cash_flow_rec.cash_flow_comments)
            row.append(int(cash_flow_rec.currency_code))
            row.append(int(cash_flow_rec.purpose_code))
            row.append('0')  # recurrence code
            row.append('10')  # SCD Posting status
            row.append(int(cash_flow_rec.trade_date))
            sql_insert_param_list.append(row)

        cash_flow_records = DBUtils.insert(self._config, None, 'cash_flow_insert_sql', sql_insert_param_list)

        return cash_flow_records

    def find_values(self, search_id, json_repr):
        '''
        Utility function to find if get all values for an attribute from a JSON string
        :param json_repr:
        :return: List of all possible values
        '''
        results = []

        def _decode_dict(a_dict):
            try:
                results.append(a_dict[search_id])
            except KeyError:
                pass
            return a_dict

        json.loads(json_repr, object_hook=_decode_dict)  # Return value ignored.
        return results
