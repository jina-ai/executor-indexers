from jina import Document, DocumentArray
from .. import FileSearcher


def test_query_keyvalue(tmpdir):
    metas = {
        'workspace': str(tmpdir),
        'name': 'query_indexer',
        'pea_id': 0,
        'replica_id': 0,
    }
    indexer = FileSearcher(
        dump_path='tests/dump1', index_filename='dbms', runtime_args=metas
    )
    docs = DocumentArray([Document(id=1), Document(id=42)])
    indexer.search(docs)
    print(docs)
    assert len(docs) == 2
    assert docs[0].text == 'hello world 1'
