import numpy as np

from jina import Document, DocumentArray

from .. import NumpyIndexer

runtime = {'pea_id': 0, 'replica_id': 0}


def test_query_vector(tmpdir):
    metas = {'workspace': str(tmpdir), 'name': 'dbms'}
    indexer = NumpyIndexer(dump_path='tests/dump1', metas=metas, runtime_args=runtime)
    docs = DocumentArray([Document(embedding=np.random.random(7))])
    TOP_K = 5
    indexer.search(docs, {'top_k': TOP_K})
    assert len(docs) == 1
    assert len(docs[0].matches) == TOP_K
    assert len(docs[0].matches[0].embedding) == 7


def test_empty_shard(tmpdir):
    metas = {'workspace': str(tmpdir), 'name': 'dbms'}
    indexer = NumpyIndexer(
        dump_path='tests/dump_empty', metas=metas, runtime_args=runtime
    )
    docs = DocumentArray([Document(embedding=np.random.random(7))])
    TOP_K = 5
    indexer.search(docs, {'top_k': TOP_K})
    assert len(docs) == 1
    assert len(docs[0].matches) == 0
