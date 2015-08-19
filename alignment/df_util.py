import logging
import sys
#
import pandas as pd
import sqlalchemy


def delete_record_from_table(unique_key_dict, table_name, engine, logger):
    unique_key_list = sorted(unique_key_dict.keys())
    first_where = True
    statement = 'DELETE FROM ' + table_name + ' WHERE'
    for unique_key in unique_key_list:
        if first_where:
            statement += ' ' + '\'' + unique_key + '\'' + '=' + '\'' + unique_key_dict[unique_key] + '\''
            first_where = False
        else:
            statement += ' AND ' + '\'' + unique_key + '\'' + '=' + '\'' + unique_key_dict[unique_key] + '\''
    statement += ';'
    sql = sqlalchemy.sql.text(statement)
    conn = engine.connect()
    trans = conn.begin()
    try:
        result = conn.execute(sql)
        trans.commit()
        conn.close()
        logger.info('result=%s' % result)
    except Exception as e:
        trans.rollback()
        conn.close()
        logger.debug('exception: %s' % e)
        sys.exit(1)


def save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger):
    logger.info('df=%s\n' % df)
    if engine.has_table(table_name):  # table already exists
        logger.info('table %s already exists' % table_name)

        delete_record_from_table(unique_key_dict, table_name, engine, logger)

        try:
            df.to_sql(table_name, engine, if_exists='append')
            logger.info('wrote sql to table: %s' % table_name)
        except Exception as e:
            logger.debug('exception: %s' % e)
            sys.exit(1)
    else:  # first creation of table
        logger.info('table %s does not yet exist' % table_name)
        try:
            df.to_sql(table_name, engine, if_exists='fail')
            logger.info('wrote sql to table: %s' % table_name)
        except Exception as e:
            logger.debug('exception: %s' % e)
            sys.exit(1)
