import os

from jina import Executor, Document, DocumentArray, requests

import lmdb


class LMDBDBMSIndexer(Executor):
    """An lmbd-based DBMS Indexer for Jina

    For more information on lmdb check their documentation: https://lmdb.readthedocs.io/en/release/
    """

    def __init__(
        self,
        map_size: int = 10485760,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.file = os.path.join(self.workspace, 'db.lmdb')
        if not os.path.exists(self.workspace):
            os.makedirs(self.workspace)
        # db for internal usage
        self.db = 'db'
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
            max_dbs=0,
            max_spare_txns=1,
            lock=True,
        )

    def _get_transaction(self, write: bool):
        return self.env.begin(write=True)

    @requests(on='/index')
    def index(self, docs: DocumentArray, **kwargs):
        with self.env.begin(write=True) as t:
            for d in docs:
                t.put(d.id.encode(), d.SerializeToString())

    @requests(on='/update')
    def update(self, docs: DocumentArray, **kwargs):
        with self.env.begin(write=True) as t:
            for d in docs:
                t.replace(d.id.encode(), d.SerializeToString())

    @requests(on='/delete')
    def delete(self, docs: DocumentArray, **kwargs):
        with self.env.begin(write=True) as t:
            for d in docs:
                t.delete(d.id.encode())

    def get(self, docs: DocumentArray, **kwargs):
        with self.env.begin(write=True) as t:
            for i, d in enumerate(docs):
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
