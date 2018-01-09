import logging
import os

from parsers.BaseParser import *

global logger
logger = logging.getLogger(__name__)
logging.config.fileConfig('%s' % CONF_LOGGING_INI, disable_existing_loggers=False)

print(os.path.dirname(__file__))
'''
file_name = 'c:/temp/bnp.txt'
with open(file_name) as f:
    reader = csv.reader(f, delimiter="\t")
    headers = next(reader)
    print(headers)
    columns = {}
    for h in headers:
        columns[h] = []
    for row in reader:
        for h, v in zip(headers, row):
            columns[h].append(v)

    print(headers)
    print(columns)
'''
config = configparser.ConfigParser(allow_no_value=True)
config.read('../conf/CRS_subAdvised.properties')

bnp_column_mapping = ast.literal_eval(config.get('BNP', 'bnp_column_mapping_dict'))
logger.debug(bnp_column_mapping)
print(bnp_column_mapping['currency_code'])

ssb_purpose_code_json = config.get('SSB', 'ssb_purpose_code_json')
logger.debug(ssb_purpose_code_json)
logger.debug(json.loads(ssb_purpose_code_json))

bnp_account_mapping = config.get('BNP', 'bnp_account_mapping_json')
print(bnp_account_mapping)
for item in json.loads(bnp_account_mapping):
    print(item)
    if 'LU1629362713' in item['isin']:
        print(item.get('accountId'))

'''
def rename(dir, pattern):
    for pathAndFilename in glob.iglob(os.path.join(dir, pattern)):
        title, ext = os.path.splitext(os.path.basename(pathAndFilename))
        os.rename(pathAndFilename, os.path.join(dir, title+'_'+ext[1:9]+'.csv'))

rename('c:\\temp\\test1', '*.*')
'''
'''

sqlParams = []
sqlParams.append(353)
sqlParams.append('test.txt')
sqlParams.append(1)
sqlParams.append('BATCH')
import_process_type = DBUtils.callStoredProc(config, 'generate_import_process_id', sqlParams)
print(import_process_type)

#data = DBUtils.get_data_map(config, None,  'get_currency_by_iso_code', None)
'''

'''
json_data = DBUtils.select(config, sqlName='cash_flow_purpose_code_sql', sqlParams=None, isJson=True)
decoded = json.loads(json_data)
for x in decoded:
    print('%s - %s'%(x['purpose_code'],x['purpose_code_description']))
'''

'''
def find_values(id, json_repr):
    results = []

    def _decode_dict(a_dict):
        try: results.append(a_dict[id])
        except KeyError: pass
        return a_dict

    json.loads(json_repr, object_hook=_decode_dict)  # Return value ignored.
    return results

ssb_purpose_code_json = '[{"ssb_category":"Expenses", "crs_purpose_code":54},{"ssb_category":"Capital Stock", "crs_purpose_code":51}]'
ssb_category_list = json.loads(ssb_purpose_code_json)
print(ssb_category_list)
line_item = 'Expenses'

if line_item in find_values('ssb_category', ssb_purpose_code_json):
    for item in ssb_category_list:
        if line_item == item['ssb_category']:
            print(item.get('crs_purpose_code'))

'''
'''
json_input = '{"persons": [{"name": "Brian", "city": "Seattle"}, {"name": "David", "city": "Amsterdam"} ] }'
try:
    decoded = json.loads(json_input)
    print(decoded['persons'][0]['name'])
    # Access data
    for x in decoded['persons']:
        print(x['name'])
        print(x['city'])

except (ValueError, KeyError, TypeError) as e:
    print("JSON format error")
    print(type(e))
    print(e)


def majorityElement(num):
    majorityIndex = 0
    count = 0
    for i in range(0, len(num)):
        if num[majorityIndex] == num[i]:
            count +=1
        else:
            count -=1
        print('%s - %s' % (num[majorityIndex], count))
        if count == 0 :
            majorityIndex = i
            count = 1


    return num[majorityIndex]


#num = [1,4,4,4,4,4,4,4,2,16,2,19,3,4,2,4,15,4,60]
#print(majorityElement(num));
'''
