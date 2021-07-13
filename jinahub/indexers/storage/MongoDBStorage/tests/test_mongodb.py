import os
import time

import pytest
import numpy as np
from jina import Document, DocumentArray

from .. import MongoDBStorage
from .. import MongoHandler


@pytest.fixture(autouse=True)
def mongo_docker_compose():
    os.system(f"docker-compose --project-directory . up  --build -d --remove-orphans")
    time.sleep(5)
    yield
    os.system(f"docker-compose --project-directory . down --remove-orphans")


@pytest.fixture
def docs_to_index():
    docu_array = DocumentArray()
    for idx in range(0, 10):
        d = Document(text=f'hello {idx}')
        d.embedding = np.random.rand(1, 20)
        docu_array.append(d)
    return docu_array


def test_mongo_storage(docs_to_index, tmpdir):
    # add
    storage = MongoDBStorage()
    storage.add(docs=docs_to_index, parameters={})
    assert storage.size == 10
    # update & search
    doc_id_to_update = docs_to_index[0].id
    storage.update(
        docs=DocumentArray([Document(id=doc_id_to_update, text='hello test')])
    )
    docs_to_search = DocumentArray([Document(id=doc_id_to_update)])
    storage.search(docs=docs_to_search)
    assert docs_to_search[0].text == 'hello test'
    # delete
    doc_id_to_delete = docs_to_index[0].id
    storage.delete(docs=DocumentArray([Document(id=doc_id_to_delete)]))
    docs_to_search = DocumentArray([Document(id=doc_id_to_delete)])
    assert len(docs_to_search) == 1
    assert docs_to_search[0].text == ''  # find no result
    # test dump
    parameters = {'dump_path': os.path.join(str(tmpdir), 'dump.json'), 'shards': 2}
    storage.dump(parameters=parameters)
    assert os.path.exists(os.path.join(str(tmpdir), 'dump.json'))
