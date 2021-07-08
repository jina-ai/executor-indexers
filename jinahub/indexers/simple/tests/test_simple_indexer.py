import numpy as np
from jina import Flow, Document

from jinahub.indexers.simple import SimpleIndexer


def test_simple_indexer():
    f = Flow().add(
        uses=SimpleIndexer,
        override_with={'index_file_name': 'name'},
        override_metas={'workspace': 'ws'},
    )

    with f:
        resp = f.post(
            on='/index',
            inputs=[Document(id='a', embedding=np.array([1]))],
            return_results=True,
        )
        print(f'{resp}')
        resp = f.post(
            on='/search',
            inputs=[Document(embedding=np.array([1]))],
            return_results=True,
            parameters={'top_k': 5},
        )
        assert resp[0].docs[0].matches[0].id == 'a'
