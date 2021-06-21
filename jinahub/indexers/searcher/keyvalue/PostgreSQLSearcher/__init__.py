__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"

from typing import Tuple, Generator, Dict, List, Union

import numpy as np
from jina import Executor, requests, DocumentArray
from jina.logging.logger import JinaLogger

from jina_commons import get_logger
from jinahub.indexers.indexer.PostgreSQLIndexer import PostgreSQLDBMSHandler


class PostgreSQLSearcher(Executor):
    """:class:`PostgreSQLIndexer` PostgreSQL based BDMS Indexer.
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
        default_traversal_paths: Union[str, List[str]] = 'r',
        *args,
        **kwargs,
    ):

        super().__init__(*args, **kwargs)
        self.default_traversal_paths = default_traversal_paths
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
        # we traverse the matches and retrieve their data
        self.default_traversal = 'm'

    def _get_generator(self) -> Generator[Tuple[str, np.array, bytes], None, None]:
        with self.handler as handler:
            # always order the dump by id as integer
            handler.cursor.execute(f"SELECT * from {handler.table} ORDER BY ID::int")
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
                f"SELECT COUNT(*) from {self.handler.table}"
            )
            records = postgres_handler.cursor.fetchall()
            return records[0][0]

    def get_handler(self) -> 'PostgreSQLDBMSHandler':
        """Get the handler to PostgreSQLDBMS."""
        return self.handler

    def get_query_handler(self) -> 'PostgreSQLDBMSHandler':
        """Get the handler to PostgresSQLDBMS."""
        return self.handler

    def __exit__(self, *args):
        """ Make sure the connection to the database is closed."""

        from psycopg2 import Error

        try:
            self.connection.close()
            self.cursor.close()
            self.logger.info('PostgreSQL connection is closed')
        except (Exception, Error) as error:
            self.logger.error('Error while closing: ', error)

    @requests(on='/query')
    def query(self, docs: DocumentArray, parameters: Dict, **kwargs):
        trav_paths = parameters.get('traversal_paths', self.default_traversal_paths)

        with self.handler as postgres_handler:
            for trav_path in trav_paths:
                postgres_handler.query(docs.traverse_flat(trav_path))
