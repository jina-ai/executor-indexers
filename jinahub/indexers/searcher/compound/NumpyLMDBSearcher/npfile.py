__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"

import copy
from typing import Dict

from jina import requests, DocumentArray, Executor

from jinahub.indexers.searcher.NumpySearcher import NumpySearcher
from jinahub.indexers.storage.LMDBStorage import LMDBStorage


class NumpyLMDBSearcher(Executor):
    def __init__(self, dump_path=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._vec_indexer = NumpySearcher(dump_path=dump_path, *args, **kwargs)
        self._kv_indexer = LMDBStorage(dump_path=dump_path, *args, **kwargs)

    @requests(on='/search')
    def search(self, docs: 'DocumentArray', parameters: Dict = None, **kwargs):
        self._vec_indexer.search(docs, parameters)
        kv_parameters = copy.deepcopy(parameters)

        kv_parameters['traversal_paths'] = [
            path + 'm' for path in kv_parameters.get('traversal_paths', ['r'])
        ]

        self._kv_indexer.search(docs, parameters=kv_parameters)
