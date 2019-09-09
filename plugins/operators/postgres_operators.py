import logging
import os
import unicodecsv as csv
import io
import uuid
import gzip

from airflow import LoggingMixin
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.decorators import apply_defaults

from datetime import timedelta, datetime

from tempfile import NamedTemporaryFile

log = logging.getLogger(__name__)

class FileDownloadOperator(BaseOperator):
    """
    Downloads a file from a connection.
    """
    template_fields = ('source_path','dest_path',)
    template_ext = ()
    ui_color = '#ffcc44'

    @apply_defaults
    def __init__(self,
                 source_type,
                 source_path,
                 source_conn_id=None,
                 dest_path=None,
                 *args, **kwargs):
        super(FileDownloadOperator, self).__init__(*args, **kwargs)

        self.source_type = source_type
        self.source_conn_id = source_conn_id
        self.source_path = source_path
        self.dest_path = dest_path

    def execute(self, context):
        if self.dest_path:
            print('got dest_path')
            print(self.dest_path)

        print('after if self.dest_path')

        # Return the destination path as an xcom variable
        return self.dest_path

class PostgresToS3Operator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        postgres_conn_id,
        s3_bucket,
        s3_key,
        s3_conn_id,
        query,
        *args,
        **kwargs
    ):
        super(PostgresToS3Operator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.query = query

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        res = self.query_db(self.query, postgres_hook)
        res.seek(0)
        s3_hook.load_file_obj(res, key=self.s3_key, bucket_name=self.s3_bucket, replace=True)
        return True

    def query_db(self, query, hook):
        content = hook.get_records(sql=query)
        print('Got content')
        print(content)
        logging.info("Got the following records: {}".format(content))
        return self.gen_file(content)

    def gen_file(self, content):
        output = io.BytesIO()
        writer = csv.writer(output, delimiter='|', encoding='utf-8')
        writer.writerows(content)
        return output

    def cleanup(self, fname):
        os.remove(fname)

class ExecutePostgresQueryOperator(BaseOperator):

    @apply_defaults
    def __init__(self, pool_conn_id, query, *args, **kwargs):
        """
        ExecutePostgresQueryOperator:
        Postgres operator which runs a specified query against specified database.
        :param      pool_conn_id: id of the pool connection determined in the Airflow Connections;
        :type       pool_conn_id: str
        :param      query: SQL query to execute - can be string or function converting airflow context to query
        :type       query: str | fn
        """
        super(ExecutePostgresQueryOperator, self).__init__(*args, **kwargs)
        self.pg_hook = PostgresHook(postgres_conn_id=pool_conn_id)
        self.pool_conn_id = pool_conn_id
        self.query = query

    def execute(self, context):
        query = self.query if type(self.query) == str else self.query(context)
        logging.info("Executing Postgres query {}".format(query))
        self.pg_hook.run(query)

class PostgresUpsertOperator(BaseOperator):
    """
    Executes an upsert from one to another table in the same Redshift database by completely replacing the rows of target table with the rows in staging table that contain the same business key

    :param src_postgres_conn_id: reference to a specific postgres database
    :type src_postgres_conn_id: string

    :param dest_postgres_conn_id: reference to a specific postgres database
    :type dest_postgres_conn_id: string

    :param src_table: reference to a specific table in postgres database
    :type table: string

    :param dest_table: reference to a specific table in postgres database
    :type table: string

    :param src_keys business keys that are supposed to be matched with dest_keys business keys in the target table
    :type table: string

    :param dest_keys business keys that are supposed to be matched with src_keys business keys in the source table
    :type table: string
    """
    @apply_defaults
    def __init__(self, src_postgres_conn_id, dest_postgres_conn_id,
      src_table, dest_table, src_keys, dest_keys, *args, **kwargs):

      self.src_postgres_conn_id = src_postgres_conn_id
      self.dest_postgres_conn_id = dest_postgres_conn_id
      self.src_table = src_table
      self.dest_table = dest_table
      self.src_keys = src_keys
      self.dest_keys = dest_keys
      super(PostgresUpsertOperator , self).__init__(*args, **kwargs)

    def execute(self, context):
      self.hook = PostgresHook(postgres_conn_id=self.src_postgres_conn_id)
      conn = self.hook.get_conn()
      cursor = conn.cursor()
      logging.info("Connected with " + self.src_postgres_conn_id)
      # build the SQL statement
      sql_statement = "begin transaction; "
      sql_statement += "delete from " + self.dest_table + " using " + self.src_table + " where "
      for i in range (0,len(self.src_keys)):
        sql_statement += self.src_table + "." + self.src_keys[i] + " = " + self.dest_table + "." + self.dest_keys[i]
        if(i < len(self.src_keys)-1):
          sql_statement += " and "

      sql_statement += "; "
      sql_statement += " insert into " + self.dest_table + " select * from " + self.src_table + " ; "
      sql_statement += " end transaction; "

      print(sql_statement)
      cursor.execute(sql_statement)
      cursor.close()
      conn.commit()
      logging.info("Upsert command completed")

class PostgresToPostgresOperator(BaseOperator):

    template_fields = ('sql', 'query_parameters', 'dest_table',
                       'postgres_preoperator', 'postgres_postoperator')
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            sql,
            dest_table,
            src_postgres_conn_id=None,
            dest_postgres_conn_id=None,
            postgres_preoperator=None,
            postgres_postoperator=None,
            query_parameters=None,
            *args, **kwargs):
        super(PostgresToPostgresOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.dest_table = dest_table
        self.src_postgres_conn_id = src_postgres_conn_id
        self.dest_postgres_conn_id = dest_postgres_conn_id
        self.postgres_preoperator = postgres_preoperator
        self.postgres_postoperator = postgres_postoperator
        self.query_parameters = query_parameters

    def execute(self, context):
        logging.info('Executing: ' + str(self.sql))
        src_postgres = PostgresHook(postgres_conn_id=self.src_postgres_conn_id)
        dest_postgres = PostgresHook(postgres_conn_id=self.dest_postgres_conn_id)

        logging.info(
            "Transferring Postgres query results into other Postgres database.")
        conn = src_postgres.get_conn()

        cursor = conn.cursor()

        cursor.execute(self.sql, self.query_parameters)

        if self.postgres_preoperator:
            logging.info("Running Postgres preoperator")
            dest_postgres.run(self.postgres_preoperator)

        if cursor.rowcount != 0:
            rows_to_insert = []

            logging.info("Inserting rows into Postgres")

            for i, row in enumerate(cursor):
                rows_to_insert.append(row)

            results = list(cursor.fetchall())

            print("results")
            print(results)

            logging.info("Enumerable that will be insert into Postgres is as follows")
            print(rows_to_insert)

            dest_postgres.insert_rows(table=self.dest_table, rows=rows_to_insert)
            logging.info(str(cursor.rowcount) + " rows inserted")
        else:
            logging.info("No rows inserted")

        if self.postgres_postoperator:
            logging.info("Running Postgres postoperator")
            dest_postgres.run(self.postgres_postoperator)

        logging.info("Done.")

class PostgresTableRowsCountOperator(BaseOperator, LoggingMixin):
    """
    PostgresTableRowsCountOperator:
    Postgres operator which querying for a specified table rows count.
    :param      pool_conn_id: id of the pool connection determined in the Airflow Connections;
    :type       pool_conn_id: str
    :param      db_name: target database/schema name;
    :type       db_name: str
    :param      table_name: target table name on which count query going to be executed;
    :type       table_name: str
    """

    @apply_defaults
    def __init__(self, pool_conn_id, db_name, table_name, *args, **kwargs):
        super(PostgresTableRowsCountOperator, self).__init__(*args, **kwargs)
        self.pg_hook = PostgresHook(postgres_conn_id=pool_conn_id, schema=db_name)
        self.pool_conn_id = pool_conn_id
        self.db_name = db_name
        self.table_name = table_name

    # query for the rows count inside the execute method
    def execute(self, context):
        # log entry point of the execution
        self.log.info(
            "Execution based on PostgresHook: {} and Schema: {} for the table: {}"
                .format(self.pool_conn_id, self.db_name, self.table_name)
        )
        # execute
        result = self.pg_hook.get_first(
            sql="SELECT COUNT(*) FROM {};".format(self.table_name)
        )
        # show the result in the log
        log.info("Result: {}".format(result))
        # return result as output of the current task
        return result

class S3ToPostgresOperator(BaseOperator):
    """
    Download a file from S3 bucket and import to PostgreSQL database via COPY command
    """

    template_fields = ('dest_table_name', 's3_key_name', 'pg_preoperator', 'pg_postoperator')
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            postgres_conn,
            dest_table_name,
            dest_table_columns,
            s3_conn_id,
            s3_bucket_name,
            s3_key_name,
            pg_preoperator=None,
            pg_postoperator=None,
            *args, **kwargs):
        super(S3ToPostgresOperator, self).__init__(*args, **kwargs)
        self.postgres_conn = postgres_conn
        self.dest_table_name = dest_table_name
        self.dest_table_columns = dest_table_columns
        self.pg_preoperator = pg_preoperator
        self.pg_postoperator = pg_postoperator
        self.s3_conn_id = s3_conn_id
        self.s3_bucket_name = s3_bucket_name
        self.s3_key_name = s3_key_name
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):

        logging.info('Downloading file from S3 and inserting into PostgreSQL via COPY command')

        s3 = S3Hook(aws_conn_id=self.s3_conn_id)

        with NamedTemporaryFile(mode='w') as temp_file_s3:

            logging.info('S3 file to read: [{}/{}]'.format(self.s3_bucket_name, self.s3_key_name))
            logging.info('Writing S3 file to [{}].'.format(temp_file_s3.name))
            try:
                temp_file_s3.write(s3.read_key(self.s3_key_name, self.s3_bucket_name))
            except Exception as e:
                logging.error('Error: {}'.format(str(e)))
                logging.error('Error: {}'.format(str(type(e))))
                raise AirflowException('Error: {}'.format(str(e)))

            temp_file_s3.flush()

            with open(temp_file_s3.name, 'r') as file_read:

                # Skip first row
                next(file_read)

                pgsql = PostgresHook(postgres_conn_id=self.postgres_conn)

                pgsql_conn = pgsql.get_conn()
                pgsql_conn.autocommit = True
                pgsql_cursor = pgsql_conn.cursor()

                if self.pg_preoperator is not None:
                    logging.info('Running pg_preoperator query.')
                    pgsql.run(self.pg_preoperator)

                try:

                    logging.info('Start to copy file to database')

                    logging.info('Destination table: [{}]'.format(self.dest_table_name))
                    pgsql_cursor.copy_from(
                        file=file_read,
                        sep=',',
                        table=self.dest_table_name,
                        columns=self.dest_table_columns
                    )
                    pgsql_conn.commit()

                except Exception as e:
                    logging.error('Error trying to load file to db: [{}]'.format(str(e)))
                    raise AirflowException(str(e))

                if self.pg_postoperator is not None:
                    logging.info('Running pg_postoperator query [{}].'.format(self.pg_postoperator))
                    logging.info('Post operator result: {}'.format(pgsql.get_first(self.pg_postoperator)[0]))

                logging.info('File Uploaded to database.')

            logging.info('Done.')
