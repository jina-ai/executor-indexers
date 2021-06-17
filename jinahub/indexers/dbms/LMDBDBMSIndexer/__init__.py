import os
from typing import Dict

from jina import Executor, Document, DocumentArray, requests

import lmdb


class LMDBDBMSIndexer(Executor):
    """An lmbd-based DBMS Indexer for Jina

    For more information on lmdb check their documentation: https://lmdb.readthedocs.io/en/release/
    """

    def __init__(
        self,
        map_size: int = 10485760,
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
        if parameters is None:
            parameters = {}

        trav_path = parameters.get('traversal_path', self.default_traversal_path)
        with self.env.begin(write=True) as t:
            for d in docs.traverse_flat(trav_path):
                t.put(d.id.encode(), d.SerializeToString())

    @requests(on='/update')
    def update(self, docs: DocumentArray, parameters: Dict = None, **kwargs):
        if parameters is None:
            parameters = {}
        trav_path = parameters.get('traversal_path', self.default_traversal_path)
        with self.env.begin(write=True) as t:
            for d in docs.traverse_flat(trav_path):
                t.replace(d.id.encode(), d.SerializeToString())

    @requests(on='/delete')
    def delete(self, docs: DocumentArray, parameters: Dict = None, **kwargs):
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

    @property
    def size(self):
        with self.env.begin(write=False) as t:
            stats = t.stat()
            return stats['entries']

    def close(self) -> None:
        self.env.close()
