__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"

from typing import Optional

from pymongo import MongoClient
from jina.logging.logger import JinaLogger
from jina import Document, DocumentArray


class MongoHandler:
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 27017,
        username: Optional[str] = 'root',
        password: Optional[str] = '123456',
        database: str = 'jina_index',
        collection: str = 'jina_index',
    ):
        self._logger = JinaLogger('mongo_handler')
        self._database_name = database
        self._collection_name = collection
        self._collection = None
        if username and password:
            self._connection = MongoClient(
                f'mongodb://{username}:{password}@{host}:{port}'
            )
        else:
            self._connection = MongoClient(f'mongodb://{host}:{port}')
        self._logger.info(f'Connected to mongodb instance at {host}:{port}')

    @property
    def collection(self):
        if not self._collection:
            self._collection = self._connection[self._database_name][
                self._collection_name
            ]
            self._collection.create_index(
                'id', unique=True
            )  # create index on doc.id field if index not exist.
            return self._collection
        else:
            return self._collection

    def add(self, docs: DocumentArray, **kwargs):
        self.collection.insert_many(
            documents=[doc.dict() for doc in docs],
            ordered=False,  # all document inserts will be attempted.
        )

    def update(self, docs: DocumentArray, **kwargs):
        for doc in docs:
            self.collection.update_one(
                filter={'id': {'$eq': doc.id}}, update={'$set': doc.dict()}, upsert=True
            )

    def delete(self, docs: DocumentArray, **kwargs):
        doc_ids = [doc.id for doc in docs]
        self.collection.delete_many(filter={'id': {'$in': doc_ids}})

    def search(self, docs: DocumentArray, **kwargs):
        for doc in docs:
            result = self.collection.find_one(
                filter={'id': doc.id}, projection={'_id': False}
            )
            if result:
                doc.update(Document(result))

    def get_size(self):
        return self.collection.count()

    def close(self):
        if self._connection:
            self._connection.close()
