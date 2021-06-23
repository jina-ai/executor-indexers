import numpy as np
import pytest
from jina import Document, DocumentArray
from jina.logging.profile import TimeContext

from .. import PostgreSQLIndexer
from ..postgreshandler import doc_without_embedding

d_embedding = np.array([1, 1, 1, 1, 1, 1, 1])
c_embedding = np.array([2, 2, 2, 2, 2, 2, 2])


@pytest.fixture(scope='function', autouse=True)
def patched_random_port(mocker):
    used_ports = set()
    from jina.helper import random_port

    def _random_port():

        for i in range(10):
            _port = random_port()

            if _port is not None and _port not in used_ports:
                used_ports.add(_port)
                return _port
        raise Exception('no port available')

    mocker.patch('jina.helper.random_port', new_callable=lambda: _random_port)


def get_documents(chunks, same_content, nr=10, index_start=0, same_tag_content=None):
    next_chunk_id = nr + index_start
    for i in range(index_start, nr + index_start):
        with Document() as d:
            d.id = i
            if same_content:
                d.text = 'hello world'
                d.embedding = np.random.random(d_embedding.shape)
            else:
                d.text = f'hello world {i}'
                d.embedding = np.random.random(d_embedding.shape)
            if same_tag_content:
                d.tags['field'] = 'tag data'
            elif same_tag_content is False:
                d.tags['field'] = f'tag data {i}'
            for j in range(chunks):
                with Document() as c:
                    c.id = next_chunk_id
                    if same_content:
                        c.text = 'hello world from chunk'
                        c.embedding = np.random.random(c_embedding.shape)
                    else:
                        c.text = f'hello world from chunk {j}'
                        c.embedding = np.random.random(c_embedding.shape)
                    if same_tag_content:
                        c.tags['field'] = 'tag data'
                    elif same_tag_content is False:
                        c.tags['field'] = f'tag data {next_chunk_id}'
                next_chunk_id += 1
                d.chunks.append(c)
        yield d


def validate_db_side(postgres_indexer, expected_data):
    ids, vecs, metas = zip(*expected_data)
    with postgres_indexer.handler as handler:
        cursor = handler.connection.cursor()
        cursor.execute(
            f'SELECT ID, VECS, METAS from {postgres_indexer.table} ORDER BY ID::int'
        )
        record = cursor.fetchall()
        for i in range(len(expected_data)):
            np.testing.assert_equal(ids[i], str(record[i][0]))
            np.testing.assert_equal(vecs[i], np.frombuffer(record[i][1]))
            np.testing.assert_equal(metas[i], bytes(record[i][2]))


def test_postgres(tmpdir):
    postgres_indexer = PostgreSQLIndexer()
    NR_DOCS = 10000
    original_docs = DocumentArray(
        list(get_documents(nr=NR_DOCS, chunks=0, same_content=False))
    )

    postgres_indexer.delete(original_docs, {})

    with TimeContext(f'### indexing {len(original_docs)} docs'):
        postgres_indexer.add(original_docs, {})
    np.testing.assert_equal(postgres_indexer.size, NR_DOCS)

    info_original_docs = [
        (doc.id, doc.embedding, doc_without_embedding(doc)) for doc in original_docs
    ]
    validate_db_side(postgres_indexer, info_original_docs)

    new_docs = DocumentArray(
        list(get_documents(chunks=False, nr=10, same_content=True))
    )
    postgres_indexer.update(new_docs, {})

    info_new_docs = [
        (doc.id, doc.embedding, doc_without_embedding(doc)) for doc in new_docs
    ]
    ids, vecs, metas = zip(*info_new_docs)
    expected_info = [(ids[0], vecs[0], metas[0])]
    validate_db_side(postgres_indexer, expected_info)

    postgres_indexer.delete(new_docs, {})
    np.testing.assert_equal(postgres_indexer.size, len(original_docs) - len(new_docs))
