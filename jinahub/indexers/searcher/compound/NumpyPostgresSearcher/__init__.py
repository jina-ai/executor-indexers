__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"

import copy
from typing import Dict, List, Union

from jina import requests, DocumentArray, Executor
from jina.helper import get_request_executor_parameter

from jina_commons import get_logger
from jinahub.indexers.searcher.keyvalue.PostgreSQLSearcher import (
    PostgreSQLSearcher,
)
from jinahub.indexers.searcher.vector.NumpySearcher import NumpySearcher


class NumpyPostgresSearcher(Executor):
    """A Compound Indexer made up of a NumpyIndexer (for vectors) and a Postgres Indexer"""

    def __init__(
        self,
        dump_path=None,
        default_traversal_paths: Union[str, List[str]] = 'r',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.default_traversal_paths = default_traversal_paths
        # when constructed from rolling update the dump_path is passed via a runtime_arg
        dump_path = dump_path or kwargs.get('runtime_args').get('dump_path')
        self.logger = get_logger(self)
        self._kv_indexer = None
        self._vec_indexer = None
        if dump_path:
            self._vec_indexer = NumpySearcher(dump_path=dump_path, **kwargs)
            self._kv_indexer = PostgreSQLSearcher(**kwargs)
        else:
            self.logger.warning(
                f'No dump path provided for {self}. Use .rolling_update() to re-initialize...'
            )

    @requests(on='/search')
    def search(self, docs: 'DocumentArray', parameters: Dict = None, **kwargs):
        inner_parameters = parameters.get(self.metas.name, parameters)
        if self._kv_indexer and self._vec_indexer:
            self._vec_indexer.search(docs, inner_parameters)
            kv_parameters = copy.deepcopy(inner_parameters)
            kv_parameters['traversal_paths'] = [
                path + 'm' for path in kv_parameters.get('traversal_paths', ['r'])
            ]
            self._kv_indexer.query(docs, kv_parameters)
        else:
            return
