__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"

from typing import Optional

import numpy as np
from pymongo import MongoClient
from jina.logging.logger import JinaLogger
from jina import Document, DocumentArray


def doc_without_embedding(d: Document):
    new_doc = Document(d, copy=True)
    new_doc.ClearField('embedding')
    new_doc.ClearField('id')
    return new_doc.dict()


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
        """Get the collection, if the collection is new, create index based on ID field."""
        if not self._collection:
            self._collection = self._connection[self._database_name][
                self._collection_name
            ]
            self._collection.create_index(
                'ID', unique=True
            )  # create index on doc.id field if index not exist.
            return self._collection
        else:
            return self._collection

    def add(self, docs: DocumentArray, **kwargs):
        """Insert document ID, VECS and METAS from docs into mongodb instance."""
        dict_docs = []
        for doc in docs:
            item = {}
            item['ID'] = doc.id
            item['VECS'] = doc.embedding.tolist()
            item['METAS'] = doc_without_embedding(doc)
            dict_docs.append(item)
        self.collection.insert_many(
            documents=dict_docs,
            ordered=False,  # all document inserts will be attempted.
        )

    def update(self, docs: DocumentArray, **kwargs):
        """Update item ID, VECS and METAS from docs based on doc id."""
        for doc in docs:
            embed = []
            if doc.embedding:
                embed = doc.embedding.tolist()
            self.collection.update_one(
                filter={'ID': {'$eq': doc.id}},
                update={'$set': {'VECS': embed, 'METAS': doc_without_embedding(doc)}},
                upsert=True,
            )

    def delete(self, docs: DocumentArray, **kwargs):
        """Delete item from docs based on doc id."""
        doc_ids = [doc.id for doc in docs]
        self.collection.delete_many(filter={'ID': {'$in': doc_ids}})

    def search(self, docs: DocumentArray, **kwargs):
        for doc in docs:
            result = self.collection.find_one(
                filter={'ID': doc.id}, projection={'_id': False, 'METAS': 1}
            )
            if result:
                retrieved_doc = Document(result['METAS'])
                doc.update(retrieved_doc)

    def get_size(self):
        """Get the size of collection"""
        return self.collection.count()

    def close(self):
        """Close connection."""
        if self._connection:
            self._connection.close()
