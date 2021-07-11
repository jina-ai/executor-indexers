__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"

from typing import Optional, List, Union, Dict

import numpy as np
import hnswlib
from jina import Executor, requests, DocumentArray, Document

from jina_commons import get_logger
from jina_commons.indexers.dump import import_vectors


class HnswlibSearcher(Executor):
    """Hnswlib powered vector indexer

    For more information about the Hnswlib supported parameters, please consult:
        - https://github.com/nmslib/hnswlib

    .. note::
        Hnswlib package dependency is only required at the query time.
    """

    def __init__(
        self,
        default_top_k: int = 10,
        metric: str = 'euclidean',
        dump_path: Optional[str] = None,
        default_traversal_paths: List[str] = None,
        **kwargs,
    ):
        """
        Initialize an HnswlibSearcher

        :param default_top_k: get tok k vectors
        :param metric: Metric can be "angular", "euclidean", "manhattan", "hamming", or "dot"
        :param dump_path: the path to load ids and vecs
        :param traverse_path: traverse path on docs, e.g. ['r'], ['c']
        :param args:
        :param kwargs:
        """
        super().__init__(**kwargs)
        self.default_top_k = default_top_k
        self.metric = metric
        self.default_traversal_paths = default_traversal_paths or ['r']
        self.logger = get_logger(self)
        dump_path = dump_path or kwargs.get('runtime_args', {}).get('dump_path', None)
        if dump_path is not None:
            self.logger.info('Start building "HnswlibSearcher" from dump data')
            ids, vecs = import_vectors(dump_path, str(self.metas.pea_id))
            self._ids = np.array(list(ids))
            self._vecs = np.array(list(vecs))
            num_dim = self._vecs.shape[1]
            self._indexer = hnswlib.Index(space='cosine', dim=num_dim)
            self._indexer.init_index(max_elements=len(self._vecs), ef_construction=400, M=64)

            self._doc_id_to_offset = {}
            self._load_index(self._ids, self._vecs)
        else:
            self.logger.warning(
                'No data loaded in "HnswlibSearcher". Use .rolling_update() to re-initialize it...'
            )

    def _load_index(self, ids, vecs):
        for idx, v in enumerate(vecs):
            self._indexer.add_items(v.astype(np.float32),idx)
            self._doc_id_to_offset[ids[idx]] = idx
        self._indexer.set_ef(50)

    @requests(on='/search')
    def search(self, docs: DocumentArray, parameters: Dict, **kwargs):
        if not hasattr(self, '_indexer'):
            self.logger.warning('Querying against an empty index')
            return
        top_k = parameters.get('top_k', self.default_top_k)
        traversal_paths = parameters.get(
            'traversal_paths', self.default_traversal_paths
        )

        for doc in docs.traverse_flat(traversal_paths):
            indices, dists = self._indexer.knn_query(doc.embedding, k=top_k)
            for idx, dist in zip(indices[0], dists[0]):
                match = Document(id=self._ids[idx], embedding=self._vecs[idx])
                match.scores['distance'] = 1 / (1 + dist)
                doc.matches.append(match)

    @requests(on='/fill_embedding')
    def fill_embedding(self, query_da: DocumentArray, **kwargs):
        for doc in query_da:
            doc.embedding = np.array(
                self._indexer.get_items([int(self._doc_id_to_offset[str(doc.id)])])[0]
            )
