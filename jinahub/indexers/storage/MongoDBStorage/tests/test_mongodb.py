import os
import time

import pytest
from jina import Document, DocumentArray

from .. import MongoDBStorage


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
        docu_array.append(d)
    return docu_array


def test_add(docs_to_index):
    storage = MongoDBStorage()
    storage.add(docs=docs_to_index, parameters={})
    assert storage.size == 10
