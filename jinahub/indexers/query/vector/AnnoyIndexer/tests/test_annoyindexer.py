__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"

import os

import numpy as np
import pytest
from jina import Document, DocumentArray
from jina.executors.metas import get_default_metas
from jina_commons.indexers.dump import import_vectors

from jinahub.indexers.query.vector.AnnoyIndexer import AnnoyIndexer

# fix the seed here
np.random.seed(500)

docs = DocumentArray([Document(embedding=np.random.random(10)) for i in range(10)])
search_doc = DocumentArray([Document(embedding=np.random.random(10))])
DUMP_PATH = 'tests/dump1'
TOP_K = 5


@pytest.fixture(scope='function', autouse=True)
def metas(tmpdir):
    os.environ['TEST_WORKSPACE'] = str(tmpdir)
    metas = get_default_metas()
    metas['workspace'] = os.environ['TEST_WORKSPACE']
    yield metas
    del os.environ['TEST_WORKSPACE']


def test_simple_annoy():
    from annoy import AnnoyIndex
    _index = AnnoyIndex(5, 'angular')
    for j in range(3):
        _index.add_item(j, np.random.random((5,)))
    _index.build(4)
    idx1, _ = _index.get_nns_by_vector(np.random.random((5,)), 3, include_distances=True)
    assert len(idx1) == 3


def test_query_vector(tmpdir):
    metas = {'workspace': str(tmpdir), 'name': 'dbms', 'pea_id': 0, 'replica_id': 0}

    indexer = AnnoyIndexer(dump_path=DUMP_PATH, num_dim=7, top_k=TOP_K, metas=metas)
    docs = DocumentArray([Document(embedding=np.random.random(7))])
    indexer.search(docs)

    ids, vecs = import_vectors(DUMP_PATH, str(0))
    ids = np.array(list(ids))
    vecs = np.array(list(vecs))

    assert len(docs) == 1
    assert len(docs[0].matches) == TOP_K
    assert docs[0].matches[0].id in ids
    assert len(docs[0].matches[0].embedding) == 7
    assert docs[0].matches[0].embedding in vecs

    da = DocumentArray([Document(id=0),Document(id=1),Document(id=2)])
    indexer.fill_embedding(da)
    for i, doc in enumerate(da):
        assert list(doc.embedding)