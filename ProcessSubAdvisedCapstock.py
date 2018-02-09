import glob
import logging.handlers
import logging.config
import os
import shutil
from pathlib import Path

from ParserFactory import *
from DBUtils import *


class SubAdvisedProcessor:
    global logger
    global config
    global config_path
    global parser_type
    global data_dir
    global filePattern
    global process_date

    def setLogging(self):
        # create logger with 'SQlExtract'
        global logger
        logger = logging.getLogger('SubAdvisedProcessor - ' + __name__)
        logging.config.fileConfig('../conf/logging.ini', disable_existing_loggers=False)

    def checkArguments(self, argv):
        if argv is None:
            argv = sys.argv

        global config_path
        if argv[1] is not None and argv[1] != 'None':
            config_path = argv[1]
            logger.debug(" Config file  - %s" % config_path)
        else:
            config_path = '../conf/CRS_subAdvised.properties'
            # sys.exit("Configuration file is not passed. Please check the arguments")

        global parser_type
        parser_type = None
        if len(argv) > 2:
            parser_type = str(argv[2]).rstrip()
        logger.info('parser_type - %s' % parser_type)

        global data_dir
        data_dir = None
        if len(argv) > 3:
            data_dir = str(argv[3]).rstrip()
        logger.info('data_dir - %s' % data_dir)

        global filePattern
        if len(argv) > 4:
            filePattern = str(argv[4]).rstrip()
        else:
            filePattern = ''

        global process_date
        if len(argv) > 5:
            process_date = str(argv[4]).rstrip()
        else:
            process_date = datetime.today().strftime("%Y%m%d")

    def setConfig(self, path2Config):
        confFile = Path(path2Config)
        try:
            my_abs_path = confFile.resolve()
        except FileNotFoundError:
            # doesn't exist
            logger.debug("Config file could not be found. Check the file location.")
            sys.exit("Config file could not be found. Check the file location.")

        else:
            # exists
            # config = configparser.RawConfigParser()
            logger.debug('Loading the config file')
            global config
            config = configparser.ConfigParser(allow_no_value=True)
            config.read(path2Config)

    def main(argv=None):

        processed_fileName = '../data/processed_' + datetime.today().strftime("%Y%m%d") + '.txt'
        if not os.path.isfile(processed_fileName):
            open(processed_fileName, 'a').close()


        processor = SubAdvisedProcessor()
        dir = os.path.dirname(__file__)
        if argv is None:
            argv = sys.argv

        # set the Logger
        processor.setLogging()

        # check arguments
        processor.checkArguments(argv)

        # load application specific properties file
        processor.setConfig(os.path.join(dir, config_path))

        parser = ParserFactory.factory(parser_type, config)

        '''
        get the json proeprty string from the database
        parse and load the properties in the config
        '''
        # json_string = config.get(parser_type, 'json_properties_string')
        params = []
        params.append(parser_type)
        json_string = DBUtils.select(config, 'get_parser_config', params, False)[0]
        for k, v in json.loads(json_string).items():
            logger.debug('JSON String parsed Properties from Database - %s - %s' % (k, v))
            config.set(parser_type, str(k), str(v).replace('\'', '"'))

        config.set('APP', 'process_date', process_date)
        return_code = 0

        # this is also the feed id
        # import_process_type = json.loads(DBUtils.select(config, 'get_sub_advised_import_process_type', None, True))[0]['import_process_type_id']
        # logger.debug('Import Process Type Id - %s' % import_process_type)

        for file_name in [os.path.basename(filename) for filename in glob.glob1(data_dir, filePattern)]:
            logger.debug('#' * 50)
            file_success = False
            accountId = None
            filename = None

            try:
                print(file_name)
                # look if the file is in the processed file list
                if file_name in open(processed_fileName, 'r+').read():
                    logger.error(
                        " File %s is already processed for today. To reprocessed, change the file name and rerun again." % file_name)
                    return_code = 101


                else:
                    filename = data_dir + '\\' + file_name
                    parser._file_name = filename
                    parser._config = config

                    # get the parsed results
                    parsed_records = []
                    parsed_records = parser.parse()

                    # save the records to CRS database
                    if len(parsed_records) > 0:
                        accountId = parsed_records[0].account_id
                        import_process_id = None

                        rows_inserted = parser.save_to_database(parsed_records)

                        # check if config says to auto- approve. Write the code below to auto-Approve
                        if str(accountId) in config.get(parser_type, 'auto_approve_account_list') and len(
                                rows_inserted) > 0:
                            logger.debug('Auto Approve is set to True for account - %s' % accountId)
                            for row in rows_inserted:
                                params = [row[0], 'BATCH']
                                DBUtils.callStoredProc(config, 'cash_flow_update_status', params)

                                # check the config for auto post to SCD
                                if str(accountId) in config.get(parser_type, 'auto_post_to_SCD_account_list'):
                                    logger.debug('Auto Post is set to True for account - %s' % accountId)
                                    params = [row[0], '20']
                                    DBUtils.callStoredProc(config, 'cash_flow_update_scd_status', params)

                                # check the config for auto post to Bloomberg
                                if str(accountId) in config.get(parser_type, 'auto_post_to_BBG_account_list'):
                                    logger.debug(
                                        'Auto Release to Trade Support is set to True for account - %s' % accountId)
                                    params = [row[0], 'BATCH']
                                    DBUtils.callStoredProc(config, 'cash_flow_release_to_trading_support', params)

                    # set file status to success to archive the file.
                    file_success = True
            except Exception as inst:
                # update the import process with the failed reason
                '''
                sqlParams = []
                sqlParams.append(import_process_id)
                sqlParams.append(repr(inst))
                sqlParams.append('12')
                sqlParams.append(accountId)
                DBUtils.callStoredProc(config, 'update_import_process', sqlParams)
                '''
                traceback.print_stack()
                logging.exception('Got exception on main handler')
                # setting the return code to 101 to ensure the tidal scheduler job failed with error log
                # to report file parsing errors for single file.
                return_code = 101
            finally:
                # rename the original file and append with timestamp for archiving purposes
                if filename is not None:
                    logger.info('Archive the file')
                    # add file name to the processed file list.
                    f = open(processed_fileName, 'a')
                    status = 'SUCCESS' if file_success else 'FAILED'
                    f.write('%s  - %s \n' % (file_name, status))
                    f.close()

                    modifiedTime = os.path.getmtime(filename)
                    timeStamp = datetime.fromtimestamp(modifiedTime).strftime("%Y%m%d_%H%M%S")
                    os.rename(filename, filename + "_" + timeStamp)
                    if file_success:
                        archive_filename = os.path.join(dir, '../data/archive/' + file_name + "_" + timeStamp)
                        shutil.move(filename + "_" + timeStamp, archive_filename)
                        logger.debug("File moved to archive folder - %s" % archive_filename)
                    else:
                        error_filename = os.path.join(dir, '../data/error/' + file_name + "_" + timeStamp)
                        shutil.move(filename + "_" + timeStamp, error_filename)
                        logger.error("Error File moved to error folder - %s" % error_filename)

                    logger.info('Done archiving the output file')

                logger.debug('#' * 50)

        return return_code


if __name__ == "__main__":
    sys.exit(SubAdvisedProcessor.main())
