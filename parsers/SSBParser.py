from csv import reader

from parsers.BaseParser import *


class SSBParser(BaseParser):
    global logger
    logger = logging.getLogger(__name__)
    logging.config.fileConfig('%s' % CONF_LOGGING_INI, disable_existing_loggers=False)

    def parse(self):
        file_name = self._file_name
        config = self._config
        ssb_purpose_code_json = config.get('SSB', 'ssb_purpose_code_json')
        ssb_category_list = self.find_values('ssb_category', ssb_purpose_code_json)

        logger.info('SSb parser - Parse fileName - %s' % file_name)
        cash_flow_records = []
        with open(file_name) as f:
            lines = f.readlines()

            # first line is the account name
            account_name = lines[0].rstrip()

            # check if this is the right State Street Report
            if 'CASH AVAILABILITY' not in lines[1]:
                logger.info('Incorrect Report. Please check the report')
                raise Exception('Incorrect Report. Please check the report')

            # check if data is available
            if 'Currency USD Not Finalized!' in lines[0]:
                logger.info('Data is not available yet. Report is empty')
                raise Exception('Data is not available yet. Report is empty')

            # report date is on the 3rd line of the report. Getting only the date part using regular expression.
            match = re.search(r'\d+-([a-zA-Z]{3})-\d{4}', lines[2])
            # report_date = datetime.strptime(match.group(), '%d-%b-%Y').date()
            report_date = datetime.strptime(match.group(), '%d-%b-%Y').strftime('%Y%m%d')

            # line 3 has the custodian account code for the account
            custodian_account_code = lines[3].rstrip()

            # loop over the rest of the line and find Sub-total buckets and their amounts
            for line in lines:
                if "Sub Total" in line:
                    amount = 0.0
                    # find the sub-total bucket
                    match = re.search('\(([^)]+)\)', line)
                    line_item = match.group(1)
                    is_amount_negative = False

                    # get the amount. Numbers are quoted string with comma. Using inbuilt CSV library to parse fields
                    for fields in reader([line], skipinitialspace=True):
                        amount = fields[4].rstrip()
                        is_amount_negative = True if '(' in amount else False
                        amount = amount.replace(',', '').replace('(', '').replace(')', '')
                        break

                    # process the line_item only if amount is <> 0
                    if float(amount) == 0.0:
                        continue
                    else:
                        # once you have all the required parameters, build the CRS JSON object.
                        # if line_item in SSBParser.ssb_category_map[]
                        logger.debug('Extract from %s: %s - %s - %s' % (file_name, report_date, line_item, amount))
                        if line_item in ssb_category_list:
                            cash_flow_rec = CashFlow()
                            cash_flow_rec.cash_flow_comments = line_item
                            cash_flow_rec.custodian_account_code = custodian_account_code
                            cash_flow_rec.account_id = self._account_by_custodian_map[custodian_account_code][1]
                            cash_flow_rec.account_name = account_name

                            # Find the purpose code from the ssb_purpose_code_mapping list
                            for item in json.loads(ssb_purpose_code_json):
                                if line_item == item['ssb_category']:
                                    cash_flow_rec.purpose_code = item.get('crs_purpose_code')
                                    # if the line_item is 'Capital Stock' and amount is negative then its a withdrawal
                                    if is_amount_negative:
                                        cash_flow_rec.cash_flow_type = item.get('cash_flow_type_negative')
                                    else:
                                        cash_flow_rec.cash_flow_type = item.get('cash_flow_type')
                                    break

                            cash_flow_rec.trade_date = report_date  # trade date is same as report date and settlement date
                            cash_flow_rec.settlement_date = report_date
                            cash_flow_rec.cash_flow_amount = amount

                            # currently all feeds from State street are in USD
                            cash_flow_rec.currency_code = self._iso_currency_map['USD'][1]
                            cash_flow_rec.cash_flow_status = 20  # this is the Approved status code of Entered
                            cash_flow_records.append(cash_flow_rec)

        logger.info('SSb parser - Parse records - %s' % ''.join(str(rec) for rec in cash_flow_records))
        return cash_flow_records

    def save_to_database(self, data_list):
        return super().save_to_database(data_list)
