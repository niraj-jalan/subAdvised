import csv

from parsers.BaseParser import *


class BNYMellon(BaseParser):
    global logger
    logger = logging.getLogger(__name__)
    logging.config.fileConfig('%s' % CONF_LOGGING_INI, disable_existing_loggers=False)

    def parse(self):
        file_name = self._file_name
        config = self._config

        bnp_account_mapping = config.get('BNYMellon', 'account_mapping_json')
        bnp_column_mapping = ast.literal_eval(config.get('BNYMellon', 'column_mapping_dict'))

        TRADE_DATE_COLUMN = bnp_column_mapping['posted_date']
        SETTLEMENT_DATE_COLUMN = bnp_column_mapping['settlement_date']
        CASH_FLOW_AMOUNT_COLUMN = bnp_column_mapping['cash_flow_amount']
        CURRENCY_CODE_COLUMN = bnp_column_mapping['currency_code']
        ACCOUNT_COLUMN = bnp_column_mapping['account']

        '''
        json_string='{"account_mapping":[{"acronym":"PACFUNSG","accountId":"983","custodianId":"Fund Number : PF02"},{"acronym":"MIGMCF","accountId":"532","custodianId":"Fund Number : 890204E"},{"acronym":"MIGSEF","accountId":"531","custodianId":"Fund Number : 890025E"},{"acronym":"PRUCO","accountId":"840","custodianId":"PDBS AST LORD ABBETT FIXED INC PORT"}],"data_points":[{"sheet":0, "row_data":"NET CAPITAL STOCK"},{"sheet":0, "row_data":"EXPENSES"}, {"sheet":0, "row_data":"Net Fundshare Activity"}]}'
params = json.loads(json_string)
custodian_list = find_values('custodianId', json_string)
data_points = find_values('row_data', json_string)
print(data_points)
found_custodian = False
account_id = None

book = xlrd.open_workbook('c:/temp/PF02_PF Developing Growth Fund.xls')
#book = xlrd.open_workbook('c:/temp/PDBS.xls')
for sheet in book.sheets():
    #sheet = book.sheet_by_index(i)
    for i in range(sheet.nrows):
        row = sheet.row_values(i)
        if found_custodian == False:
            check_Cust = [e for e in custodian_list if  e in row]
            if len(check_Cust) > 0:
                found_custodian = True
                account_id = [account['accountId'] for  account in params['account_mapping'] if account['custodianId'] == check_Cust[0]]
                logger.debug('Custodian Account row - %s' %row)
                logger.debug('Account Id - %s'%account_id[0])

        if 'Date:' in str(row):
            print(row)

        check_data = [e for e in data_points if e in row]
        if len(check_data) > 0 :
            sheet_number = [sh['sheet'] for sh in params['data_points'] if sh['row_data'] == check_data[0]]
            if sheet.number == sheet_number[0]:
                logger.debug('Data row - %s'%row)
                for cell in row:
                    if is_number(cell):
                        amount = float(cell)
                        print(amount)
                continue

        
        
        We are parsing the file in columnar data. 
        Advantages is that now the mapping is based on columns headers and not column positions. 
        '''
        logger.info('BNPParibas parser - Parse fileName - %s' % file_name)
        columnar_data = {}
        rolled_up_rec = {}
        cash_flow_records = []

        with open(file_name, newline='') as f:
            reader = csv.reader((row.replace('\x00', '') for row in f), delimiter="\t")
            headers = next(reader)
            for h in headers:
                columnar_data[h] = []
            for row in reader:
                for h, v in zip(headers, row):
                    columnar_data[h].append(v)

        # build record for rows only where there is a valid trade date
        for i in range(0, len(columnar_data['Trade date'])):
            if len(columnar_data[TRADE_DATE_COLUMN][i].rstrip()) > 0:

                # unique record key is a combination of <posted_date> - <settlement date> - <isin> - <currency>
                trade_date = datetime.strptime(columnar_data[TRADE_DATE_COLUMN][i], '%d/%m/%Y').strftime('%Y%m%d')
                settlement_date = datetime.strptime(columnar_data[SETTLEMENT_DATE_COLUMN][i], '%d/%m/%Y').strftime(
                    '%Y%m%d')
                account_code = columnar_data[ACCOUNT_COLUMN][i]
                # Find the CRS account id based on the account mapping list
                for item in json.loads(bnp_account_mapping):
                    if account_code in item['isin']:
                        account_code = item['accountId']  # item.get('accountId')
                        break

                currency = columnar_data[CURRENCY_CODE_COLUMN][i]
                amount = float(
                    columnar_data[CASH_FLOW_AMOUNT_COLUMN][i].replace(',', '').replace('(', '').replace(')', ''))

                key = '_'.join((str(trade_date), str(settlement_date), str(account_code), currency))
                if key in rolled_up_rec:
                    cash_flow_rec = rolled_up_rec[key]
                    temp = amount + cash_flow_rec.cash_flow_amount
                    cash_flow_rec.cash_flow_amount = temp
                    rolled_up_rec[key] = cash_flow_rec
                else:
                    cash_flow_rec = CashFlow()
                    cash_flow_rec.trade_date = trade_date
                    cash_flow_rec.settlement_date = settlement_date
                    cash_flow_rec.account_id = account_code
                    cash_flow_rec.currency_code = self._iso_currency_map[currency][1]
                    cash_flow_rec.cash_flow_amount = amount
                    rolled_up_rec[key] = cash_flow_rec

        # Update additional parameters for the rolled up totals and add it to the list
        for key, rec in rolled_up_rec.items():
            # set the purpose code .. currently BNP is sending only contributions and withdrawals
            rec.purpose_code = 51  # CRS purpose code for CAPSTOCK

            # set the CASH FLOW TYPE ( Contribution or Withdrawal) based on positive or negative amount
            if rec.cash_flow_amount >= 0:
                rec.cash_flow_type = 1001
            else:
                rec.cash_flow_type = 1002
                rec.cash_flow_amount = amount * -1

            rec.cash_flow_status = 20  # this is the Approved status code of Entered
            cash_flow_records.append(rec)

        logger.info('BNP parser - Parse records - %s' % ''.join(str(rec) for rec in cash_flow_records))
        return cash_flow_records

    def save_to_database(self, data_list):
        return super().save_to_database(data_list)
