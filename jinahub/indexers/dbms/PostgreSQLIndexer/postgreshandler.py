__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"

import psycopg2
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
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.logger = JinaLogger(self.__class__.__name__)
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.table = table

    def __enter__(self):
        return self.connect()

    def connect(self) -> 'PostgreSQLDBMSHandler':
        """Connect to the database. """

        import psycopg2
        from psycopg2 import Error

        try:
            self.connection = psycopg2.connect(
                user=self.username,
                password=self.password,
                database=self.database,
                host=self.hostname,
                port=self.port,
            )
            self.cursor = self.connection.cursor()
            self.logger.info('Successfully connected to the database')
            self.use_table()
            self.connection.commit()
        except (Exception, Error) as error:
            self.logger.error('Error while connecting to PostgreSQL', error)
        return self

    def use_table(self):
        """
        Use table if exists or create one if it doesn't.

        Create table if needed with id, vecs and metas.
        """
        from psycopg2 import Error

        self.cursor.execute(
            'select exists(select * from information_schema.tables where table_name=%s)',
            (self.table,),
        )
        if self.cursor.fetchone()[0]:
            self.logger.info('Using existing table')
        else:
            try:
                self.cursor.execute(
                    f"CREATE TABLE {self.table} ( \
                    ID VARCHAR PRIMARY KEY,  \
                    VECS BYTEA,  \
                    METAS BYTEA);"
                )
                self.logger.info('Successfully created table')
            except (Exception, Error) as error:
                self.logger.error('Error while creating table!')

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
        for doc in docs:
            try:
                self.cursor.execute(
                    f'INSERT INTO {self.table} (ID, VECS, METAS) VALUES (%s, %s, %s)',
                    (doc.id, doc.embedding.tobytes(), doc_without_embedding(doc)),
                )
                row_count += self.cursor.rowcount
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

        for doc in docs:
            self.cursor.execute(
                f'UPDATE {self.table} SET VECS = %s, METAS = %s WHERE ID = %s',
                (doc.embedding.tobytes(), doc_without_embedding(doc), doc.id),
            )
            row_count += self.cursor.rowcount
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
        for doc in docs:
            self.cursor.execute(
                f'DELETE FROM {self.table} where (ID) = (%s);', (doc.id,)
            )
            row_count += self.cursor.rowcount
        self.connection.commit()
        return row_count

    def __exit__(self, *args):
        """ Make sure the connection to the database is closed."""

        from psycopg2 import Error

        try:
            self.connection.close()
            self.cursor.close()
            self.logger.info('PostgreSQL connection is closed')
        except (Exception, Error) as error:
            self.logger.error('Error while closing: ', error)

    def query(self, docs: DocumentArray, **kwargs):
        """Use the Postgre db as a key-value engine, returning the metadata of a document id"""
        for doc in docs:
            # retrieve metadata
            self.cursor.execute(
                f'SELECT METAS FROM {self.table} WHERE ID = %s;', doc.id
            )
            result = self.cursor.fetchone()
            data = bytes(result[0])
            retrieved_doc = Document(data)
            # how to assign all fields but embedding?
            doc.content = retrieved_doc.content
            doc.mime_type = doc.mime_type
