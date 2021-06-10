__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"

from typing import Optional

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
            metric: str = 'euclidean',
            num_trees: int = 10,
            dump_path: Optional[str] = None,
            traverse_path: list = ['r'],
            **kwargs,
    ):
        """
        Initialize an AnnoyIndexer

        :param top_k: get tok k vectors
        :param metric: Metric can be "angular", "euclidean", "manhattan", "hamming", or "dot"
        :param num_trees: builds a forest of n_trees trees. More trees gives higher precision when querying.
        :param traverse_path: traverse path on docs, e.g. ['r'], ['c']
        :param args:
        :param kwargs:
        """
        super().__init__(**kwargs)
        self.top_k = top_k
        self.metric = metric
        self.num_trees = num_trees
        self.traverse_path = traverse_path
        self.logger = JinaLogger(self.metas.name)
        dump_path = dump_path or kwargs.get('runtime_args').get('dump_path')
        if dump_path is not None:
            self.logger.info('Start building "AnnoyIndexer" from dump data')
            ids, vecs = import_vectors(dump_path, str(self.metas.pea_id))
            self._ids = np.array(list(ids))
            self._vecs = np.array(list(vecs))
            num_dim = self._vecs.shape[1]
            self.indexer = AnnoyIndex(num_dim, self.metric)
            self._docID_to_mapID = {}
            self._mapID_to_docID = {}
            self._load_index(self._ids, self._vecs)
        else:
            self.logger.warning(
                'No data loaded in "AnnoyIndexer". Use .rolling_update() to re-initialize it...'
            )

    def _load_index(self, ids, vecs):
        for idx, v in enumerate(vecs):
            self.indexer.add_item(idx, v.astype(np.float32))
            self._docID_to_mapID[ids[idx]] = idx
            self._mapID_to_docID[idx] = ids[idx]
        self.indexer.build(self.num_trees)

    @requests(on='/search')
    def search(self, docs: DocumentArray, **kwargs):
        for doc in docs.traverse_flat(self.traverse_path):
            indices, dists = self.indexer.get_nns_by_vector(
                doc.embedding, self.top_k, include_distances=True
            )
            for idx, dist in zip(indices, dists):
                match = Document(id=self._mapID_to_docID[idx], embedding=self._vecs[idx])
                match.score.value = 1 / (1 + dist)
                doc.matches.append(match)

    @requests(on='/fill_embedding')
    def fill_embedding(self, query_da: DocumentArray, **kwargs):
        for doc in query_da:
            doc.embedding = np.array(self.indexer.get_item_vector(int(self._docID_to_mapID[str(doc.id)])))
