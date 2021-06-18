import numpy as np
import os

import pytest
from jina import Document, DocumentArray, Flow
from jina.logging.profile import TimeContext

from jina_commons.indexers.dump import import_vectors, import_metas
from .. import FileDBMSIndexer

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


def assert_dump_data(dump_path, docs, shards, pea_id):
    size_shard = len(docs) // shards
    size_shard_modulus = len(docs) % shards
    ids_dump, vectors_dump = import_vectors(
        dump_path,
        str(pea_id),
    )
    if pea_id == shards - 1:
        docs_expected = docs[
            (pea_id) * size_shard : (pea_id + 1) * size_shard + size_shard_modulus
        ]
    else:
        docs_expected = docs[(pea_id) * size_shard : (pea_id + 1) * size_shard]
    # pea {pea_id} has {len(docs_expected)} docs

    # TODO these might fail if we implement any ordering of elements on dumping / reloading
    ids_dump = list(ids_dump)
    vectors_dump = list(vectors_dump)
    np.testing.assert_equal(ids_dump, [d.id for d in docs_expected])
    np.testing.assert_allclose(vectors_dump, [d.embedding for d in docs_expected])

    _, metas_dump = import_metas(
        dump_path,
        str(pea_id),
    )
    metas_dump = list(metas_dump)
    np.testing.assert_equal(
        metas_dump,
        [
            FileDBMSIndexer._doc_without_embedding(d).SerializeToString()
            for d in docs_expected
        ],
    )


def test_dbms_file_writer(tmpdir):
    docs = get_documents(nr=10)
    metas = {'workspace': str(tmpdir), 'name': 'dbms', 'pea_id': 0, 'replica_id': 0}
    with FileDBMSIndexer(index_filename='dbms', metas=metas) as indexer:
        indexer.add(docs)
        assert indexer.size == len(docs)
        indexer.dump({'dump_path': os.path.join(tmpdir, 'dump1'), 'shards': 2})

        # indexer.add(docs)
        # assert indexer.size == len(docs)

        # we can index and dump again in the same context
        docs2 = get_documents(nr=10, index_start=len(docs))

        indexer.add(docs2)
        assert indexer.size == len(docs) + len(docs2)
        indexer.dump({'dump_path': os.path.join(tmpdir, 'dump2'), 'shards': 3})

    for pea_id in range(2):
        assert_dump_data(os.path.join(tmpdir, 'dump1'), docs, 2, pea_id)

    for pea_id in range(3):
        assert_dump_data(os.path.join(tmpdir, 'dump2'), docs + docs2, 3, pea_id)

    new_docs = get_documents(nr=10)

    # assert contents update
    with FileDBMSIndexer(
        dump_on_exit=True, index_filename='dbms', metas=metas
    ) as indexer:
        indexer.update(new_docs)
        assert indexer.size == 2 * len(docs)
        dump_path = indexer.default_dump_path

    assert_dump_data(dump_path, docs2 + new_docs, 1, 0)

    # assert contents update
    with FileDBMSIndexer(
        dump_on_exit=True, index_filename='dbms', metas=metas
    ) as indexer:
        indexer.delete(docs)
        assert indexer.size == len(docs)
        dump_path = indexer.default_dump_path
        print("ALMOST FINISHED DELETING")

    assert_dump_data(dump_path, docs2, 1, 0)


def test_update_not_growing(tmpdir):
    docs = get_documents(nr=10)
    update_docs = get_documents(nr=10, text='hello there')

    metas = {'workspace': str(tmpdir), 'name': 'dbms', 'pea_id': 0, 'replica_id': 0}

    # indexing
    with FileDBMSIndexer(index_filename='dbms', metas=metas) as indexer:
        indexer.add(docs)

    # getting size
    with FileDBMSIndexer(index_filename='dbms', metas=metas) as indexer:
        size = indexer.physical_size()
        items = indexer.size

    # updating
    with FileDBMSIndexer(index_filename='dbms', metas=metas) as indexer:
        indexer.update(update_docs)

    # asserting...
    with FileDBMSIndexer(index_filename='dbms', metas=metas) as indexer:
        assert indexer.physical_size() == size
        assert indexer.size == items


def test_update_not_growing_flow(tmpdir):
    metas = {'workspace': str(tmpdir), 'name': 'dbms'}
    runtime_args = {'pea_id': 0, 'replica_id': None}

    def _get_flow() -> Flow:
        return Flow().add(
            uses={
                'jtype': 'FileDBMSIndexer',
                'with': {'index_filename': 'dbms'},
                'metas': metas,
            }
        )

    docs = get_documents(nr=10)
    update_docs = get_documents(nr=10, text='hello there')

    # indexing
    with _get_flow() as f:
        f.index(inputs=docs)

    # getting size
    with FileDBMSIndexer(
        index_filename='dbms', metas=metas, runtime_args=runtime_args
    ) as indexer:
        size = indexer.physical_size()
        items = indexer.size

    # updating
    with _get_flow() as f:
        f.post(on='/update', inputs=update_docs)

    # asserting...
    with FileDBMSIndexer(
        index_filename='dbms', metas=metas, runtime_args=runtime_args
    ) as indexer:
        assert indexer.physical_size() == size
        assert indexer.size == items


def _in_docker():
    """ Returns: True if running in a Docker container, else False """
    with open('/proc/1/cgroup', 'rt') as ifh:
        if 'docker' in ifh.read():
            print('in docker, skipping benchmark')
            return True
        return False


def test_filedbms_crud(tmpdir, nr_docs=10):
    docs = get_documents(nr=nr_docs)

    metas = {'workspace': str(tmpdir), 'name': 'dbms', 'pea_id': 0}

    # indexing
    indexer = FileDBMSIndexer(dump_on_exit=False, metas=metas)
    indexer.add(docs)
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


# benchmark only
@pytest.mark.skipif(
    _in_docker() or ('GITHUB_WORKFLOW' in os.environ),
    reason='skip the benchmark test on github workflow or docker',
)
def test_filedbms_bm(tmpdir):
    nr = 100000
    # Cristian: running filedbms benchmark with 10000 docs takes 12 seconds (12.42s)
    # running filedbms benchmark with 20000 docs takes 41 seconds (41.21s)
    with TimeContext(f'running filedbms benchmark with {nr} docs'):
        test_filedbms_crud(tmpdir, nr)
