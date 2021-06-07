__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"

import os

import numpy as np
import pytest
from jina import Document, DocumentArray
from jina.executors.metas import get_default_metas

from jinahub.indexers.query.vector.AnnoyIndexer import AnnoyIndexer

# fix the seed here
np.random.seed(500)

docs = DocumentArray([Document(embedding=np.random.random(10)) for i in range(10)])
search_doc = DocumentArray([Document(embedding=np.random.random(10))])


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


def test_annoy_indexer(metas):
    with AnnoyIndexer(metas=metas,num_dim=10) as indexer:
        indexer.delete()
        indexer.index(docs)

    with AnnoyIndexer(metas=metas, top_k=4, num_dim=10) as indexer:
        assert isinstance(indexer, AnnoyIndexer)
        indexer.search(search_doc)
        assert len(search_doc[0].matches) == 4
