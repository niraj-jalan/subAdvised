import csv
import xlrd

from parsers.BaseParser import *


class BNYMellon(BaseParser):
    global logger
    logger = logging.getLogger(__name__)
    logging.config.fileConfig('%s' % CONF_LOGGING_INI, disable_existing_loggers=False)

    def parse(self):
        file_name = self._file_name
        config = self._config

        custodian_list = self.find_values('custodianId', config.get('BNYMellon', 'account_mapping'))
        data_points_json = config.get('BNYMellon', 'data_points')
        data_points = ast.literal_eval(data_points_json)
        data_points_row_data = self.find_values('row_data', data_points_json)

        found_custodian = False
        account_id = None
        settlement_date = None
        currency = 'USD'
        cash_flow_records = []

        book = xlrd.open_workbook(file_name)
        for sheet in book.sheets():
            for i in range(sheet.nrows):
                row = sheet.row_values(i)
                check_data = None
                read_cell = False

                if found_custodian == False:
                    check_Cust = [e for e in custodian_list if e in row]
                    if len(check_Cust) > 0:
                        found_custodian = True
                        account_id = [account['accountId'] for account in
                                      json.loads(config.get('BNYMellon', 'account_mapping')) if
                                      account['custodianId'] == check_Cust[0]]
                        logger.debug('Custodian Account row - %s' % row)
                        logger.debug('Account Id - %s' % account_id[0])

                data_list = [e for e in data_points_row_data if e in str(row)]
                for data in data_list:
                    sheet_number = [sh['sheet'] for sh in json.loads(data_points_json) if sh['row_data'] == data]
                    if sheet.number == sheet_number[0]:
                        logger.debug('Data row - %s' % row)
                        for cell in row:
                            # capture values in the cell that occur after the data cell
                            if str(data) in str(cell):
                                read_cell = True

                            if read_cell:
                                # get the report date
                                if 'date'.lower() in str(cell).lower():
                                    # if date is part of the same cell then get the date
                                    settlement_date = cell.split(':')[1].strip()
                                    if len(settlement_date) > 0:
                                        settlement_date = datetime.strptime(settlement_date, '%m/%d/%Y').strftime(
                                            '%Y%m%d')
                                    continue
                                elif 'currency' in str(cell).lower():
                                    # find the reporting currency
                                    if len(cell.split(':')) > 1:
                                        currency = cell.split(':')[1].strip()
                                    continue

                                '''
                                if BaseParser.is_date(str(cell)):
                                    settlement_date = datetime.strptime(str(cell), '%m/%d/%Y').strftime('%Y%m%d')
                                    continue
                                '''

                                if BaseParser.is_number(cell):
                                    try:
                                        amount = float(cell)
                                        if abs(amount) > 0:
                                            cash_rec = CashFlow()
                                            crs_purpose_code = \
                                            [row['crs_purpose_code'] for row in data_points if row['row_data'] == data][
                                                0]
                                            if amount >= 0:
                                                cash_rec.cash_flow_type = \
                                                [row['cash_flow_type'] for row in data_points if
                                                 row['row_data'] == data][0]
                                                cash_rec.cash_flow_amount = amount
                                            elif amount < 0:
                                                cash_rec.cash_flow_type = \
                                                [row['cash_flow_type_negative'] for row in data_points if
                                                 row['row_data'] == data][0]
                                                cash_rec.cash_flow_amount = amount * -1

                                            cash_rec.settlement_date = settlement_date
                                            cash_rec.account_id = account_id[0]
                                            cash_rec.trade_date = settlement_date
                                            cash_rec.purpose_code = crs_purpose_code
                                            cash_rec.currency_code = self._iso_currency_map.get(currency, ('USD', 4))[
                                                1]  # default the currency code to USD
                                            cash_rec.cash_flow_comments = data
                                            cash_rec.cash_flow_status = 10

                                            cash_flow_records.append(cash_rec)
                                    except KeyError as ke:
                                        logging.exception(ke)
                                        pass

        logger.info('BNP parser - Parse records - %s' % ''.join(str(rec) for rec in cash_flow_records))
        return cash_flow_records

    def save_to_database(self, data_list):
        return super().save_to_database(data_list)
