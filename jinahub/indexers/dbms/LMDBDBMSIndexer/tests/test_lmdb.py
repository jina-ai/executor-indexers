import os

import numpy as np
import pytest
from jina import Document, DocumentArray, Flow
from jina.logging.profile import TimeContext

from .. import LMDBDBMSIndexer

np.random.seed(0)
d_embedding = np.array([1, 1, 1, 1, 1, 1, 1])
c_embedding = np.array([2, 2, 2, 2, 2, 2, 2])


def get_documents(nr=10, index_start=0, emb_size=7, text='hello world'):
    docs = []
    for i in range(index_start, nr + index_start):
        with Document() as d:
            d.id = i
            d.text = f'{text} {i}'
            d.embedding = np.random.random(emb_size)
            d.tags['field'] = f'tag data {i}'
        docs.append(d)
    return DocumentArray(docs)


def test_lmdb_crud(tmpdir, nr_docs=10):
    docs = get_documents(nr=nr_docs)

    metas = {'workspace': str(tmpdir), 'name': 'dbms', 'pea_id': 0}

    # indexing
    indexer = LMDBDBMSIndexer(map_size=10485760 * 1000, metas=metas)
    indexer.index(docs)
    assert indexer.size == len(docs)

    query_docs = DocumentArray([Document(id=id) for id in [d.id for d in docs]])
    indexer._get(query_docs)
    for q, d in zip(query_docs, docs):
        assert d.id == q.id
        assert d.text == q.text
        np.testing.assert_equal(d.embedding, q.embedding)

    # getting size
    items = indexer.size

    # updating
    update_docs = get_documents(nr=nr_docs, text='hello there')
    indexer.update(update_docs)

    query_docs = DocumentArray([Document(id=id) for id in [d.id for d in docs]])
    indexer._get(query_docs)
    for q, d in zip(query_docs, update_docs):
        assert d.id == q.id
        assert d.text == q.text
        np.testing.assert_equal(d.embedding, q.embedding)

    # asserting...
    assert indexer.size == items

    indexer.delete(docs)
    assert indexer.size == 0


def test_lmdb_crud_flow(tmpdir):
    metas = {'workspace': str(tmpdir), 'name': 'dbms'}
    runtime_args = {'pea_id': 0, 'replica_id': None}

    def _get_flow() -> Flow:
        return Flow().add(
            uses={
                'jtype': 'LMDBDBMSIndexer',
                'with': {},
                'metas': metas,
            }
        )

    docs = get_documents(nr=10)
    update_docs = get_documents(nr=10, text='hello there')

    # indexing
    with _get_flow() as f:
        f.index(inputs=docs)

    # getting size
    with LMDBDBMSIndexer(metas=metas, runtime_args=runtime_args) as indexer:
        items = indexer.size

    # updating
    with _get_flow() as f:
        f.post(on='/update', inputs=update_docs)

    # asserting...
    with LMDBDBMSIndexer(metas=metas, runtime_args=runtime_args) as indexer:
        assert indexer.size == items


def _in_docker():
    """ Returns: True if running in a Docker container, else False """
    with open('/proc/1/cgroup', 'rt') as ifh:
        if 'docker' in ifh.read():
            print('in docker, skipping benchmark')
            return True
        return False


# benchmark only
@pytest.mark.skipif(
    _in_docker() or ('GITHUB_WORKFLOW' in os.environ),
    reason='skip the benchmark test on github workflow or docker',
)
def test_lmdb_bm(tmpdir):
    nr = 100000
    # Cristian: running lmdb benchmark with 10000 docs ...	running lmdb benchmark with 10000 docs takes 5 seconds (5.50s)
    # running lmdb benchmark with 20000 docs ...	running lmdb benchmark with 20000 docs takes 10 seconds (10.86s)
    # running lmdb benchmark with 100000 docs ...	running lmdb benchmark with 100000 docs takes 1 minute and 3 seconds (63.11s)
    with TimeContext(f'running lmdb benchmark with {nr} docs'):
        test_lmdb_crud(tmpdir, nr)
