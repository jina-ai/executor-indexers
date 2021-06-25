import os
from typing import Dict, List

import lmdb
from jina import Executor, Document, DocumentArray, requests

from jina_commons import get_logger
from jina_commons.indexers.dump import export_dump_streaming


class LMDBIndexer(Executor):
    """An lmdb-based DBMS Indexer for Jina

    For more information on lmdb check their documentation: https://lmdb.readthedocs.io/en/release/
    """

    def __init__(
        self,
        map_size: int = 1048576000,  # in bytes, 1000 MB
        default_traversal_paths: List[str] = ['r'],
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.default_traversal_paths = default_traversal_paths
        self.file = os.path.join(self.workspace, 'db.lmdb')
        if not os.path.exists(self.workspace):
            os.makedirs(self.workspace)
        self.logger = get_logger(self)
        # see https://lmdb.readthedocs.io/en/release/#environment-class for usage
        self.env = lmdb.Environment(
            self.file,
            map_size=map_size,
            subdir=False,
            readonly=False,
            metasync=True,
            sync=True,
            map_async=False,
            mode=493,
            create=True,
            readahead=True,
            writemap=False,
            meminit=True,
            max_readers=126,
            max_dbs=0,  # means only one db
            max_spare_txns=1,
            lock=True,
        )

    @requests(on='/index')
    def index(self, docs: DocumentArray, parameters: Dict, **kwargs):
        """Add entries to the index

        :param docs: the documents to add
        :param parameters: parameters to the request
        """
        traversal_paths = parameters.get(
            'traversal_paths', self.default_traversal_paths
        )
        with self.env.begin(write=True) as transaction:
            for d in docs.traverse_flat(traversal_paths):
                transaction.put(d.id.encode(), d.SerializeToString())

    @requests(on='/update')
    def update(self, docs: DocumentArray, parameters: Dict, **kwargs):
        """Update entries from the index by id

        :param docs: the documents to update
        :param parameters: parameters to the request
        """
        traversal_paths = parameters.get(
            'traversal_paths', self.default_traversal_paths
        )
        with self.env.begin(write=True) as transaction:
            for d in docs.traverse_flat(traversal_paths):
                # TODO figure out if there is a better way to do update in LMDB
                # issue: the defacto update method is an upsert (if a value didn't exist, it is created)
                # see https://lmdb.readthedocs.io/en/release/#lmdb.Cursor.replace
                if transaction.delete(d.id.encode()):
                    transaction.replace(d.id.encode(), d.SerializeToString())

    @requests(on='/delete')
    def delete(self, docs: DocumentArray, parameters: Dict, **kwargs):
        """Delete entries from the index by id

        :param docs: the documents to delete
        :param parameters: parameters to the request
        """
        traversal_paths = parameters.get(
            'traversal_paths', self.default_traversal_paths
        )
        with self.env.begin(write=True) as transaction:
            for d in docs.traverse_flat(traversal_paths):
                transaction.delete(d.id.encode())

    def _get(self, docs: DocumentArray, parameters: Dict, **kwargs):
        traversal_paths = parameters.get(
            'traversal_paths', self.default_traversal_paths
        )
        docs_to_get = docs.traverse_flat(traversal_paths)
        with self.env.begin(write=True) as transaction:
            for i, d in enumerate(docs_to_get):
                id = d.id
                docs[i] = Document(transaction.get(d.id.encode()))
                docs[i].id = id

    @requests(on='/dump')
    def dump(self, parameters: Dict, **kwargs):
        """Dump data from the index

        Requires
        - dump_path
        - shards
        to be part of `parameters`

        :param parameters: parameters to the request"""
        path = parameters.get('dump_path', None)
        if path is None:
            self.logger.error('parameters["dump_path"] was None')
            return

        shards = parameters.get('shards', None)
        if shards is None:
            self.logger.error('parameters["shards"] was None')
            return
        shards = int(shards)

        export_dump_streaming(path, shards, self.size, self._dump_generator())

    @property
    def size(self):
        """Compute size (nr of elements in lmdb)"""
        with self.env.begin(write=False) as transaction:
            stats = transaction.stat()
            return stats['entries']

    def close(self) -> None:
        """Close the lmdb environment"""
        self.env.close()

    def _dump_generator(self):
        with self.env.begin(write=False) as transaction:
            cursor = transaction.cursor()
            cursor.iternext()
            iterator = cursor.iternext(keys=True, values=True)
            for it in iterator:
                id, data = it
                doc = Document(data)
                yield id.decode(), doc.embedding, LMDBIndexer._doc_without_embedding(
                    doc
                ).SerializeToString()

    @staticmethod
    def _doc_without_embedding(d):
        new_doc = Document(d, copy=True)
        new_doc.ClearField('embedding')
        return new_doc
