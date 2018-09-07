#!/usr/bin/env python

import logging
import os
from datetime import date, timedelta, datetime

import click
import pyodbc
import yaml

NAME = "vertica-loader"


@click.command()
@click.argument('config_file', type=click.Path(exists=True, readable=True))
@click.option('--start-date', default=date.today().strftime('%Y-%m-%d'), help='First (or only) date to load')
@click.option('--end-date', default=None, help='Last date to load')
@click.option('--date-format', default='%Y-%m-%d',
              help='format to use for dates, note you must also provide --start-date or this breaks')
@click.option('--dsn', default='vertica', help='ODBC DSN to use for this job')
@click.option('--debug/--no-debug', default=True, help='Log at DEBUG level')
def run(config_file, start_date, end_date, date_format, dsn, debug):
    """Load a set of delimited files from local filesystem to vertica"""
    logger = configure_logger(debug)
    logger.info("Load started")

    dates = compute_dates(start_date, end_date, date_format)
    tables = load_table_configs(config_file, dates)

    # Setup vertica connection
    cursor = create_cursor(dsn)

    for table in tables:
        logger.info("Loading %s" % table.table)
        statements = table.generate_sql()
        for stmt in statements:
            logger.debug(stmt)
            cursor.execute(stmt)
            logger.info("Coppied %s rows" % cursor.rowcount)

    logger.info("Load completed")


def compute_dates(start_date, end_date, date_fmt='%Y-%m-%d'):
    """
    Create an inclusive sequenece, formatting the dates using a common formatter
    :return:
    """
    if end_date is None:
        return [start_date]

    d1 = datetime.strptime(start_date, date_fmt).date()
    d2 = datetime.strptime(end_date, date_fmt).date()

    delta = d2 - d1
    return [(d1 + timedelta(days=i)).strftime(date_fmt) for i in range(delta.days + 1)]


def load_table_configs(config_file, dates):
    """
    Load the table configs for this job.  This can be 1 or more tables and assumes that they use a common
    file format and are partitioned by date.
    """
    with open(config_file, 'r') as f:
        settings = yaml.load(f)

    file_spec = FileSpec(**settings['file_spec'])

    table_configs = []
    if type(settings['tables']) is list:
      # new style config
      for tbl in settings['tables']:
        truncate = True
        if 'truncate' in tbl:
          truncate = tbl['truncate']
        fields = None
        if 'fields' in tbl:
          fields = tbl['fields']
        delete_before_insert = False
        if 'delete_before_insert' in tbl:
          delete_before_insert = tbl['delete_before_insert']
        table_config = LoadConfig(table=tbl['name'],
                                  path=tbl['path'],
                                  file_spec=file_spec,
                                  dates=dates,
                                  truncate=truncate,
                                  delete_before_insert=delete_before_insert,
                                  fields=fields)
        table_configs.append(table_config)

    else:
      # old style config
      for table, path in settings['tables'].items():
        table_config = LoadConfig(table=table, path=path, file_spec=file_spec, dates=dates)
        table_configs.append(table_config)

    return table_configs


class FileSpec(object):
    def __init__(self, delimiter=',', skip_header=True, format=None, quoted=False):
        self.delimiter = delimiter
        self.skip_header = skip_header
        self.format = format
        self.quoted = quoted

    def formatted_statement(self):
        stmt = ""
        if self.format:
            stmt += self.format

        stmt += "DELIMITER '%s'" % self.delimiter

        if self.quoted:
            stmt += " ENCLOSED BY '\"'"

        if self.skip_header:
            stmt += " SKIP 1"

        stmt += " DIRECT"
        return stmt


class LoadConfig(object):
    def __init__(self, table, path, fields=None, file_spec=FileSpec(), truncate=True, dates=[], delete_before_insert=False):
        self.table = table
        self.path = path
        self.fields = fields
        self.file_spec = file_spec
        self.truncate = truncate
        self.dates = dates
        self.delete_before_insert = delete_before_insert

    def generate_sql(self):
        """
        Generate a list of SQL statements that correspond to loads for 1 or more files
        """
        statements = []

        # If the truncate flag is set, then truncate before performing any COPY statements
        if self.truncate:
            trunc_sql = "TRUNCATE TABLE %s;" % (self.table)
            statements.append(trunc_sql)

        # Load each day in the dates list
        for day in self.dates:
            data_file = self.path.format(date=day)
            if not os.path.exists(data_file):
                raise Exception("Path %s does not exists, %s table cannot be loaded" % (data_file, self.table))

            # if this is set, delete everything from the table with a matching path (ie- filename and date)
            if type(self.delete_before_insert) is bool and self.delete_before_insert:
              del_sql = "DELETE FROM %s WHERE source_file='%s';" % (self.table, data_file)
              statements.append(del_sql)
            elif type(self.delete_before_insert) is dict:
              # FIXME: find a better way to do this:
              if not re.search('[^a-z_A-Z0-9]', self.delete_before_insert['field']) and \
                 not re.search('\'', self.delete_before_insert['value']):
                del_sql = "DELETE FROM %s WHERE %s='%s';" % \
                    (self.table,
                     self.delete_before_insert['field'],
                     self.delete_before_insert['value'].format(date=day))
                statements.append(del_sql)

            table_fields = self.table
            if self.fields is not None:
                table_fields += "(%s)" % self.fields.format(path=data_file,date=day)

            file_stmt = self.file_spec.formatted_statement()
            copy_sql = "COPY %s FROM LOCAL '%s' %s;" % (table_fields, data_file, file_stmt)
            statements.append(copy_sql)

            commit_sql = "INSERT INTO last_updated (name, updated_at, updated_by) "  \
                         "VALUES ('{table}', now(), 'Vertica-CSV-Loader');".format(table=self.table)
            statements.append(commit_sql)
            statements.append("COMMIT;")

        return statements


def create_cursor(dsn):
    """
    create the pyodbc cursor that will execute the statemnts
    """
    cnxn = pyodbc.connect("DSN=%s" % dsn)
    return cnxn.cursor()


def configure_logger(debug):
    """
    Setup a logger
    """
    log_level = (logging.WARN, logging.DEBUG)[debug]
    log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(NAME)
    logger.setLevel(log_level)

    ch = logging.StreamHandler()
    ch.setFormatter(log_format)
    logger.addHandler(ch)

    logger.debug("Logging is set to DEBUG level")
    return logger


if __name__ == '__main__':
    run()
