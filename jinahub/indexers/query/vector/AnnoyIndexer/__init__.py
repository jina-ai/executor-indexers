__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"

import numpy as np
from annoy import AnnoyIndex
from jina import Executor, requests, DocumentArray, Document
from jina.logging.logger import JinaLogger
from jina_commons.indexers.dump import import_vectors


class AnnoyIndexer(Executor):
    """Annoy powered vector indexer

    For more information about the Annoy supported parameters, please consult:
        - https://github.com/spotify/annoy

    .. note::
        Annoy package dependency is only required at the query time.
    """

    def __init__(
            self,
            top_k: int = 10,
            num_dim: int = 768,
            metric: str = 'euclidean',
            num_trees: int = 10,
            dump_path: str = None,
            traverse_path: list = ['r'],
            **kwargs,
    ):
        """
        Initialize an AnnoyIndexer

        :param top_k: get tok k vectors
        :param num_dim: dimension of the vector
        :param metric: Metric can be "angular", "euclidean", "manhattan", "hamming", or "dot"
        :param num_trees: builds a forest of n_trees trees. More trees gives higher precision when querying.
        :param traverse_path: traverse path on docs, e.g. ['r'], ['c']
        :param args:
        :param kwargs:
        """
        super().__init__(**kwargs)
        self.top_k = top_k
        self.metric = metric
        self.num_dim = num_dim
        self.num_trees = num_trees
        self.indexer = AnnoyIndex(self.num_dim, self.metric)
        self.traverse_path = traverse_path
        self.logger = JinaLogger(self.metas.name)
        if dump_path is not None:
            ids, vecs = import_vectors(dump_path, str(self.metas.pea_id))
            self._ids = np.array(list(ids))
            self._vecs = np.array(list(vecs))
            self._ids_to_idx = {}
            self._load_index(self._ids, self._vecs)
        else:
            self.logger.warning(
                'No data loaded in "AnnoyIndexer". Using .rolling_update() to re-initialize it...'
            )

    def _load_index(self, ids, vecs):
        for idx, v in enumerate(vecs):
            self.indexer.add_item(idx, v.astype(np.float32))
            self._ids_to_idx[idx] = ids
        self.indexer.build(self.num_trees)

    @requests(on='/search')
    def search(self, docs: DocumentArray, **kwargs):
        for doc in docs.traverse_flat(self.traverse_path):
            indices, dists = self.indexer.get_nns_by_vector(
                doc.embedding, self.top_k, include_distances=True
            )
            for idx, dist in zip(indices, dists):
                match = Document(id=self._ids_to_idx[idx], embedding=self._vecs[idx])
                match.score.value = 1 / (1 + dist)
                doc.matches.append(match)
