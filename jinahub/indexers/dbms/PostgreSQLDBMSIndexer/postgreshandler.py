__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"

import psycopg2
from psycopg2 import pool

from jina import DocumentArray, Document
from jina.logging.logger import JinaLogger
from typing import Optional


def doc_without_embedding(d: Document):
    new_doc = Document(d, copy=True)
    new_doc.ClearField('embedding')
    return new_doc.SerializeToString()


class PostgreSQLDBMSHandler:
    """
    Postgres Handler to connect to the database and can apply add, update, delete and query.

    :param hostname: hostname of the machine
    :param port: the port
    :param username: the username to authenticate
    :param password: the password to authenticate
    :param database: the database name
    :param collection: the collection name
    :param args: other arguments
    :param kwargs: other keyword arguments
    """

    def __init__(
        self,
        hostname: str = '127.0.0.1',
        port: int = 5432,
        username: str = 'default_name',
        password: str = 'default_pwd',
        database: str = 'postgres',
        table: Optional[str] = 'default_table',
        max_connections: int = 5,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.logger = JinaLogger(self.__class__.__name__)
        self.table = table

        try:
            self.postgreSQL_pool = psycopg2.pool.SimpleConnectionPool(
                1,
                max_connections,
                user=username,
                password=password,
                database=database,
                host=hostname,
                port=port,
            )
            self.use_table()
        except (Exception, psycopg2.Error) as error:
            self.logger.error('Error while connecting to PostgreSQL', error)

    def __enter__(self):
        self.connection = self._get_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self._close_connection(self.connection)

    def use_table(self):
        """
        Use table if exists or create one if it doesn't.

        Create table if needed with id, vecs and metas.
        """
        connection = self._get_connection()
        cursor = connection.cursor()
        cursor.execute(
            'select exists(select * from information_schema.tables where table_name=%s)',
            (self.table,),
        )
        if cursor.fetchone()[0]:
            self.logger.info('Using existing table')
        else:
            try:
                cursor.execute(
                    f'CREATE TABLE {self.table} ( \
                    ID VARCHAR PRIMARY KEY,  \
                    VECS BYTEA,  \
                    METAS BYTEA);'
                )
                self.logger.info('Successfully created table')
            except (Exception, psycopg2.Error) as error:
                self.logger.error('Error while creating table!')
        self._close_connection(connection)

    def add(self, docs: DocumentArray, *args, **kwargs):
        """Insert the documents into the database.

        :param docs: list of Documents
        :param args: other arguments
        :param kwargs: other keyword arguments
        :param args: other arguments
        :param kwargs: other keyword arguments
        :return record: List of Document's id added
        """
        row_count = 0
        cursor = self.connection.cursor()
        for doc in docs:
            try:
                cursor.execute(
                    f'INSERT INTO {self.table} (ID, VECS, METAS) VALUES (%s, %s, %s)',
                    (doc.id, doc.embedding.tobytes(), doc_without_embedding(doc)),
                )
                row_count += cursor.rowcount
            except psycopg2.errors.UniqueViolation:
                self.logger.warning(
                    f'Document with id {doc.id} already exists in PSQL database. Skipping...'
                )
                self.connection.rollback()
        self.connection.commit()
        return row_count

    def update(self, docs: DocumentArray, *args, **kwargs):
        """Updated documents from the database.

        :param docs: list of Documents
        :param args: other arguments
        :param kwargs: other keyword arguments
        :return record: List of Document's id after update
        """
        row_count = 0
        cursor = self.connection.cursor()
        for doc in docs:
            cursor.execute(
                f'UPDATE {self.table} SET VECS = %s, METAS = %s WHERE ID = %s',
                (doc.embedding.tobytes(), doc_without_embedding(doc), doc.id),
            )
            row_count += cursor.rowcount
        self.connection.commit()
        return row_count

    def delete(self, docs: DocumentArray, *args, **kwargs):
        """Delete document from the database.

        :param docs: list of Documents
        :param args: other arguments
        :param kwargs: other keyword arguments
        :return record: List of Document's id after deletion
        """
        row_count = 0
        cursor = self.connection.cursor()
        for doc in docs:
            cursor.execute(f'DELETE FROM {self.table} where (ID) = (%s);', (doc.id,))
            row_count += cursor.rowcount
        self.connection.commit()
        return row_count

    def close(self):
        self.postgreSQL_pool.closeall()

    def query(self, docs: DocumentArray, **kwargs):
        """Use the Postgre db as a key-value engine, returning the metadata of a document id"""
        cursor = self.connection.cursor()
        for doc in docs:
            # retrieve metadata
            cursor.execute(f'SELECT METAS FROM {self.table} WHERE ID = %s;', (doc.id,))
            result = cursor.fetchone()
            data = bytes(result[0])
            retrieved_doc = Document(data)
            # how to assign all fields but embedding?
            doc.content = retrieved_doc.content
            doc.mime_type = doc.mime_type

    def _close_connection(self, connection):
        # restore it to the pool
        self.postgreSQL_pool.putconn(connection)

    def _get_connection(self):
        # by default psycopg2 is not auto-committing
        # this means we can have rollbacks
        # and maintain ACID-ity
        connection = self.postgreSQL_pool.getconn()
        connection.autocommit = False
        return connection

    def get_size(self):
        cursor = self.connection.cursor()
        cursor.execute(f'SELECT COUNT(*) from {self.table}')
        records = cursor.fetchall()
        return records[0][0]
