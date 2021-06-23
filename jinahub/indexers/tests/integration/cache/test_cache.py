import os

from jina import Flow, DocumentArray, Document

# noinspection PyUnresolvedReferences
from jinahub.indexers.cache import DocCache
from jinahub.indexers.indexer.LMDBIndexer import LMDBIndexer


def test_cache(tmpdir):
    os.environ['CACHE_WORKSPACE'] = os.path.join(tmpdir, 'cache')
    os.environ['DBMS_WORKSPACE'] = os.path.join(tmpdir, 'indexer')

    docs = [
        Document(id=1, content='a'),
        Document(id=2, content='a'),
        Document(id=3, content='a'),
    ]
    # to remove once https://github.com/jina-ai/jina/pull/2673 is merged
    for d in docs:
        d.update_content_hash()

    with Flow(return_results=True).add(uses='cache.yml').add(uses='dbms.yml') as f:
        response = f.post(
            on='/index',
            inputs=DocumentArray(docs),
        )
        assert len(response[0].docs) == 1

        dbms = LMDBIndexer(
            metas={'workspace': os.environ['DBMS_WORKSPACE'], 'name': 'indexer'},
            runtime_args={'pea_id': 0},
        )
        assert dbms.size == 1

        docs = [
            Document(id=1, content='b'),
            Document(id=2, content='b'),
            Document(id=3, content='b'),
        ]
        f.post(
            on='/update',
            inputs=DocumentArray(docs),
        )
        dbms = LMDBIndexer(
            metas={'workspace': os.environ['DBMS_WORKSPACE'], 'name': 'indexer'},
            runtime_args={'pea_id': 0},
        )
        assert dbms.size == 1

        f.post(
            on='/delete',
            inputs=DocumentArray(docs),
        )
        dbms = LMDBIndexer(
            metas={'workspace': os.environ['DBMS_WORKSPACE'], 'name': 'indexer'},
            runtime_args={'pea_id': 0},
        )
        assert dbms.size == 0
