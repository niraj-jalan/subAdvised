import collections
import configparser
import json
import re
import sys
import traceback
import logging
import logging.config

import pypyodbc


class DBUtils:
    global logger
    logger = logging.getLogger(__name__)
    logging.config.fileConfig('../conf/logging.ini', disable_existing_loggers=False)

    @staticmethod
    def __getConnection(config, dbName=None):
        DB = 'DB' if dbName is None else dbName
        user = config.get(DB, 'DB_USER')
        password = config.get(DB, 'DB_PASSWORD')
        database = config.get(DB, 'DB_NAME')
        server = config.get(DB, 'DB_SERVER')
        driver = config.get(DB, 'DB_DRIVER')
        con_string = 'DRIVER={%s};SERVER=%s;UID=%s;PWD=%s;DATABASE=%s;' % (driver, server, user, password, database)
        logger.info('Connection String - DRIVER={%s};SERVER=%s;UID=%s;DATABASE=%s;' % (driver, server, user, database))
        return pypyodbc.connect(con_string)

    @staticmethod
    def __closeConnection(cnxn):
        if cnxn is not None:
            cnxn.close()
        return 1

    @staticmethod
    def __close_cursor(cursor):
        if cursor is not None:
            cursor.close()
        return 1

    @staticmethod
    def __writeJSON(data, cur, columns2print):
        json_list = []
        columns = [column[0] for column in cur.description]
        for row in cur:
            d = collections.OrderedDict()
            if columns2print is None:
                for column in columns:
                    d[str(column)] = row[str(column)]
            else:
                for index in columns2print:
                    d[str(columns[int(index)])] = row[str(columns[int(index)])]
            data.append(d)

        json_list = json.dumps(data)
        '''       
        objects_file = 'data_objects.js'
        with open(objects_file, 'w') as f:
            f.write(json_list)
        '''
        return json_list

    @staticmethod
    def __writeHeaders(data, cur, columns2print):
        line = ''
        columns = [column[0] for column in cur.description]
        if columns2print is None:
            for column in columns:
                line = line + ('"%s",' % str(column))
        else:
            for index in columns2print:
                line = line + ('"%s",' % str(columns[int(index)]))

        line = line[:-1]  # remote the last comma from the line
        data.append(line + "\n")

    @staticmethod
    def __writeData(data, cur, columns2print):
        for row in cur:
            line = ""
            if columns2print is None:
                for col in row:
                    # f.write('"%s",' % str(col))
                    line = line + ('%s,' % str(col)) if col is not None else  line + '"",'
            else:
                for index in columns2print:
                    col = row[int(index)]
                    line = line + ('%s,' % str(col)) if col is not None else  line + '"",'

            # after all the columns, write the record to the file after removing the last comma
            line = line[:-1]
            data.append(line + "\n")

    @staticmethod
    def __run_sql(config, dbName, sqlName, sqlParams):
        '''
        RUn SQL method will run the sql and return the entire results set as a list of rows with no column information.
        Please note that this function returns all the rows and will be stored in the memory. Avoid using it for large
         query results. Instead use the provided select function.
        :param config: Config maps for all properties
        :param dbName: Database section in the properties file
        :param sqlName: SQL name in the properties file
        :param sqlParams: SQL parameters
        :return: Returns a list of all rows returned by the query
        '''

        connection = None
        cursor = None
        if dbName is None:
            dbName = 'DB'
        sql = config.get('DB', sqlName)
        sql = DBUtils.__process_sql(sql, sqlParams)
        try:
            connection = DBUtils.__getConnection(config, dbName)
            cursor = connection.cursor()
            if sql.find('?') > 0 and sqlParams is not None:
                cursor.execute(sql, sqlParams)
            else:
                cursor.execute(sql)

            return cursor.fetchall()

        except pypyodbc.DatabaseError as err:
            logger.error('Error while running sql - %s [params - %s]' % (sql, sqlParams))
            logger.error('Error State - %s - Error Message - %s' % (err.args[0], err.args[1]))
            # traceback.print_exc(file=sys.stderr)
            raise
        finally:
            DBUtils.__close_cursor(cursor)
            DBUtils.__closeConnection(connection)

        return cursor

    @staticmethod
    def __process_sql(sql, sqlParams):
        # check if sql contains #, then sql binding is not possible.
        if sql.find('#') > 0:
            if sqlParams is not None:
                for index in range(0, len(sqlParams)):
                    pattern = '(#' + str(index) + ')'
                    sql = re.sub(pattern, sqlParams[index], sql)
            else:
                logger.debug("No parameters are passed in. Check the runtime command.")
                sys.exit("SQL requires parameters to Run. No parameters are passed in as argument.")
        if sql.find('#') > 0:
            logger.debug("Insufficient parameters are passed in. Check the runtime command.")
            sys.exit("Insufficient SQL parameters are passed in as argument.")
        logger.info('sql - %s' % sql)
        logger.info('SQL Parameters - ' + str(sqlParams))
        return sql

    @staticmethod
    def select(config, sqlName, sqlParams, isJson, dbName=None):

        connection = None
        cursor = None
        data = []
        if dbName is None:
            dbName = 'DB'

        try:
            columns2print = config.get('DB', sqlName + '_columns_to_output').split(',')
        except(configparser.NoOptionError, configparser.NoSectionError):
            columns2print = None

        try:
            sql = config.get(dbName, sqlName)
            sql = DBUtils.__process_sql(sql, sqlParams)
            connection = DBUtils.__getConnection(config, dbName)
            cursor = connection.cursor()
            if sql.find('?') > 0 and sqlParams is not None:
                cursor.execute(sql, sqlParams)
            else:
                cursor.execute(sql)

            if (isJson):
                logger.info('Build Json Object')
                return DBUtils.__writeJSON(data, cursor, columns2print)
            else:
                # DBUtils.__writeHeaders(data, cursor, columns2print)
                DBUtils.__writeData(data, cursor, columns2print)
        except pypyodbc.DatabaseError as err:
            error, = err.args
            sys.stderr.write(error.error_desc)
            logger.error(error.error_desc)
            raise Exception('Error while running sql - %s [params - %s]' % (sql, sqlParams))
        finally:
            DBUtils.__close_cursor(cursor)
            DBUtils.__closeConnection(connection)

        return data

    @staticmethod
    def update(config, dbName, sqlName, data_rec_list):

        logger.info(' Entering DBUtils.update statement ')
        pass
        logger.info(' Exiting DBUtils.update statement ')

    @staticmethod
    def insert(config, dbName, sqlName, data_rec_list):
        logger.info(' Entering DBUtils.insert statement ')

        return_list = []
        connection = None
        cursor = None
        if dbName is None:
            dbName = 'DB'

        try:
            sql = config.get(dbName, sqlName)
            logger.debug(sql)
            connection = DBUtils.__getConnection(config, dbName)
            cursor = connection.cursor()
            # insert_count = cursor.executemany(sql, data_rec_list)
            for rec in data_rec_list:
                logger.debug('Params - %s' % rec)
                cursor.execute(sql, rec)
                row_return = []
                for row in cursor:
                    for col in row:
                        row_return.append(col)

                return_list.append(row_return)

            connection.commit()
            logger.info('DBUtils.insert statement - Inserted %s' % return_list)
        except pypyodbc.DatabaseError as err:
            # error, = err.args
            # sys.stderr.write(err.message)
            # logger.error(err.message)
            raise Exception('Error while running sql - %s [data - %s]' % (sql, data_rec_list))
        finally:
            DBUtils.__close_cursor(cursor)
            DBUtils.__closeConnection(connection)

        return return_list

    @staticmethod
    def get_data_map(config, dbName, sqlName, sqlParams):
        data = {}

        rows = DBUtils.__run_sql(config, dbName, sqlName, sqlParams)
        for row in rows:
            rId = row[0]
            data[rId] = row

        return data

    @staticmethod
    def callStoredProc(config, sqlname, sqlParams):
        ret_code = None
        conn = None
        cursor = None
        procName = None
        try:
            conn = DBUtils.__getConnection(config)
            procName = config.get('DB', sqlname)
            sql = """SET NOCOUNT ON;
                 DECLARE @ret int
                 EXEC @ret = %s %s
                 SELECT @ret""" % (procName, ','.join(['?'] * len(sqlParams)))
            logger.debug('Stored Proc Sql - %s  where params - %s' % (sql, sqlParams))

            cursor = conn.cursor()
            ret_code = int(cursor.execute(sql, sqlParams).fetchone()[0])

            conn.commit()
            logger.info('DBUtils.callStoredProc statement - re_code %s' % ret_code)

        except pypyodbc.DatabaseError as err:
            # error, = err.args
            # sys.stderr.write(err.message)
            # logger.error(err.message)
            raise Exception('Error while running stored proc - %s [params - %s]' % (procName, sqlParams))
        finally:
            DBUtils.__close_cursor(cursor)
            DBUtils.__closeConnection(conn)

        return ret_code
