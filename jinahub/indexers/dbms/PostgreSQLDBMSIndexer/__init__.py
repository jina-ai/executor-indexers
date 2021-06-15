__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"

from typing import Tuple, Generator, Dict

import numpy as np
from jina import Executor, requests, DocumentArray

from jina_commons import get_logger
from jina_commons.indexers.dump import export_dump_streaming
from .postgreshandler import PostgreSQLDBMSHandler


class PostgreSQLDBMSIndexer(Executor):
    """:class:`PostgreSQLDBMSIndexer` PostgreSQL based DBMS Indexer.
    Initialize the PostgreSQLDBIndexer.

    :param hostname: hostname of the machine
    :param port: the port
    :param username: the username to authenticate
    :param password: the password to authenticate
    :param database: the database name
    :param table: the table name to use
    :param args: other arguments
    :param kwargs: other keyword arguments
    """

    def __init__(
        self,
        hostname: str = '127.0.0.1',
        port: int = 5432,
        username: str = 'postgres',
        password: str = '123456',
        database: str = 'postgres',
        table: str = 'default_table',
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.table = table
        self.logger = get_logger(self)
        self.handler = PostgreSQLDBMSHandler(
            hostname=self.hostname,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
            table=self.table,
        )

    def _get_generator(self) -> Generator[Tuple[str, np.array, bytes], None, None]:
        with self.handler as handler:
            # always order the dump by id as integer
            handler.cursor.execute(f'SELECT * from {handler.table} ORDER BY ID::int')
            records = handler.cursor.fetchall()
            for rec in records:
                yield rec[0], np.frombuffer(bytes(rec[1])), bytes(rec[2])

    @property
    def size(self):
        """Obtain the size of the table

        .. # noqa: DAR201
        """
        with self.handler as postgres_handler:
            postgres_handler.cursor.execute(
                f'SELECT COUNT(*) from {self.handler.table}'
            )
            records = postgres_handler.cursor.fetchall()
            return records[0][0]

    @requests(on='/index')
    def add(self, docs: DocumentArray, **kwargs):
        """Add a Document to PostgreSQLDBMS.

        :param docs: list of Documents
        """

        with self.handler as postgres_handler:
            postgres_handler.add(docs)

    @requests(on='/update')
    def update(self, docs: DocumentArray, **kwargs):
        """Updated document from the database.

        :param docs: list of Documents
        """

        with self.handler as postgres_handler:
            postgres_handler.update(docs)

    @requests(on='/delete')
    def delete(self, docs: DocumentArray, **kwargs):
        """Delete document from the database.

        :param docs: list of Documents
        """

        with self.handler as postgres_handler:
            postgres_handler.delete(docs)

    @requests(on='/dump')
    def dump(self, parameters: Dict, **kwargs):
        """Dump the index

        :param parameters: a dictionary containing the parameters for the dump
        """

        path = parameters.get('dump_path')
        if path is None:
            self.logger.error(f'No "dump_path" provided for {self}')

        shards = int(parameters.get('shards'))
        if shards is None:
            self.logger.error(f'No "shards" provided for {self}')

        export_dump_streaming(
            path, shards=shards, size=self.size, data=self._get_generator()
        )
