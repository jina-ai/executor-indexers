import os
import pickle
from typing import Dict

import lmdb
from jina import Executor, Document, DocumentArray, requests

from jina_commons import get_logger
from jina_commons.indexers.dump import export_dump_streaming


class LMDBDBMSIndexer(Executor):
    """An lmbd-based DBMS Indexer for Jina

    For more information on lmdb check their documentation: https://lmdb.readthedocs.io/en/release/
    """

    def __init__(
        self,
        map_size: int = 10485760,  # in bytes, 10 MB
        default_traversal_path: str = 'r',
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.default_traversal_path = default_traversal_path
        self.file = os.path.join(self.workspace, 'db.lmdb')
        if not os.path.exists(self.workspace):
            os.makedirs(self.workspace)
        # db for internal usage
        self.db = 'db'
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
    def index(self, docs: DocumentArray, parameters: Dict = None, **kwargs):
        """Add entries to the index

        :param docs: the documents to add
        :param parameters: parameters to the request
        """
        if parameters is None:
            parameters = {}

        trav_path = parameters.get('traversal_path', self.default_traversal_path)
        with self.env.begin(write=True) as t:
            for d in docs.traverse_flat(trav_path):
                t.put(d.id.encode(), d.SerializeToString())

    @requests(on='/update')
    def update(self, docs: DocumentArray, parameters: Dict = None, **kwargs):
        """Update entries from the index by id

        :param docs: the documents to update
        :param parameters: parameters to the request
        """
        if parameters is None:
            parameters = {}
        trav_path = parameters.get('traversal_path', self.default_traversal_path)
        with self.env.begin(write=True) as t:
            for d in docs.traverse_flat(trav_path):
                t.replace(d.id.encode(), d.SerializeToString())

    @requests(on='/delete')
    def delete(self, docs: DocumentArray, parameters: Dict = None, **kwargs):
        """Delete entries from the index by id

        :param docs: the documents to delete
        :param parameters: parameters to the request
        """
        if parameters is None:
            parameters = {}

        trav_path = parameters.get('traversal_path', self.default_traversal_path)
        with self.env.begin(write=True) as t:
            for d in docs.traverse_flat(trav_path):
                t.delete(d.id.encode())

    def _get(self, docs: DocumentArray, parameters: Dict = None, **kwargs):
        if parameters is None:
            parameters = {}

        trav_path = parameters.get('traversal_path', self.default_traversal_path)
        docs_to_get = docs.traverse_flat(trav_path)
        with self.env.begin(write=True) as t:
            for i, d in enumerate(docs_to_get):
                id = d.id
                docs[i] = Document(t.get(d.id.encode()))
                docs[i].id = id

    @requests(on='/dump')
    def dump(self, parameters: Dict = None, **kwargs):
        """Dump data from the index

        Requires
        - dump_path
        - shards
        to be part of `parameters`

        :param parameters: parameters to the request"""
        if parameters is None:
            self.logger.error('parameters was None. Expects dump_path and shards')
            return

        path = parameters.get('dump_path', None)
        if path is None:
            self.logger.error('parameters.dump_path was None')
            return

        shards = parameters.get('shards', None)
        if shards is None:
            self.logger.error('parameters.shards was None')
            return
        shards = int(shards)

        export_dump_streaming(path, shards, self.size, self._dump_generator())

    @property
    def size(self):
        """Compute size (nr of elements in lmdb)"""
        with self.env.begin(write=False) as t:
            stats = t.stat()
            return stats['entries']

    def close(self) -> None:
        """Close the lmdb environment"""
        self.env.close()

    def _dump_generator(self):
        with self.env.begin(write=False) as t:
            cursor = t.cursor()
            cursor.iternext()
            iterator = cursor.iternext(keys=True, values=True)
            for it in iterator:
                id, data = it
                doc = Document(data)
                yield id.decode(), doc.embedding, LMDBDBMSIndexer._doc_without_embedding(
                    doc
                ).SerializeToString()

    @staticmethod
    def _doc_without_embedding(d):
        new_doc = Document(d, copy=True)
        new_doc.ClearField('embedding')
        return new_doc
