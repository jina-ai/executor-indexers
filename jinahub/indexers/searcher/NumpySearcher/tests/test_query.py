import numpy as np
import pytest
from jina import Document, DocumentArray

from .. import NumpySearcher

TOP_K = 5


def test_query_vector(tmpdir):
    runtime = {
        'workspace': str(tmpdir),
        'name': 'searcher',
        'pea_id': 0,
        'replica_id': 0,
    }
    indexer = NumpySearcher(dump_path='tests/dump1', runtime_args=runtime)
    docs = DocumentArray([Document(embedding=np.random.random(7))])
    TOP_K = 5
    indexer.search(docs, {'top_k': TOP_K})
    assert len(docs) == 1
    assert len(docs[0].matches) == TOP_K
    assert len(docs[0].matches[0].embedding) == 7


@pytest.mark.parametrize(['metric', 'is_distance'],
                         [('cosine', True), ('euclidean', True),
                          ('cosine', False), ('euclidean', False)])
def test_metric(tmpdir, metric, is_distance):
    runtime = {
        'workspace': str(tmpdir),
        'name': 'searcher',
        'pea_id': 0,
        'replica_id': 0,
    }

    indexer = NumpySearcher(dump_path='tests/dump1', top_k=TOP_K, runtime_args=runtime, metric=metric,
                            is_distance=is_distance)
    docs = DocumentArray([Document(embedding=np.random.random(7))])
    indexer.search(docs, {})
    assert len(docs[0].matches) == TOP_K

    for i in range(len(docs[0].matches) - 1):
        if not is_distance:
            assert docs[0].matches[i].scores[metric].value >= docs[0].matches[i + 1].scores[metric].value
        else:
            assert docs[0].matches[i].scores[metric].value <= docs[0].matches[i + 1].scores[metric].value


def test_empty_shard(tmpdir):
    runtime = {
        'workspace': str(tmpdir),
        'name': 'searcher',
        'pea_id': 0,
        'replica_id': 0,
    }
    indexer = NumpySearcher(dump_path='tests/dump_empty', runtime_args=runtime)
    docs = DocumentArray([Document(embedding=np.random.random(7))])
    TOP_K = 5
    indexer.search(docs, {'top_k': TOP_K})
    assert len(docs) == 1
    assert len(docs[0].matches) == 0
