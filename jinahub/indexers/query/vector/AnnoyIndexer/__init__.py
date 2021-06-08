__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"

import errno
import json
import os
from pathlib import Path

from annoy import AnnoyIndex
from jina import Executor, Document, DocumentArray, requests


class AnnoyIndexer(Executor):
    """Annoy powered vector indexer

    For more information about the Annoy supported parameters, please consult:
        - https://github.com/spotify/annoy

    .. note::
        Annoy package dependency is only required at the query time.
    """

    ANNOY_INDEX_FILE_NAME = 'annoy.ann'
    ANNOY_INDEX_MAPPING_NAME = (
        'map.json'  # this map stores int id and docid, annoy only allows add int as id.
    )

    def __init__(
        self,
        top_k: int = 10,
        num_dim: int = 768,
        metric: str = 'euclidean',
        num_trees: int = 10,
        traverse_path: list = ['r'],
        save_on_close: bool = True,
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
        self.id_docid_map = {}
        self.index_base_dir = f'{kwargs["metas"]["workspace"]}/annoy/'
        self.save_on_close = save_on_close
        self.index_path = self.index_base_dir + self.ANNOY_INDEX_FILE_NAME
        self.index_map_path = self.index_base_dir + self.ANNOY_INDEX_MAPPING_NAME
        self.indexer = AnnoyIndex(self.num_dim, self.metric)
        self.traverse_path = traverse_path
        if os.path.exists(self.index_path) and os.path.exists(self.index_map_path):
            self._load_index()

    def _load_index(self):
        self.indexer.load(self.index_path)
        with open(self.index_map_path, 'r') as f:
            self.id_docid_map = json.load(f)

    @requests(on='/delete')
    def delete(self, **kwargs):
        try:
            os.remove(self.index_path)
            os.remove(self.index_map_path)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

    @requests(on='/index')
    def index(self, docs: DocumentArray, **kwargs):
        for idx, doc in enumerate(docs.traverse_flat(self.traverse_path)):
            self.id_docid_map[idx] = doc.id
            self.indexer.add_item(idx, doc.embedding)

    @requests(on='/search')
    def search(self, docs: DocumentArray, **kwargs):
        for doc in docs.traverse_flat(self.traverse_path):
            indices, dists = self.indexer.get_nns_by_vector(
                doc.embedding, self.top_k, include_distances=True
            )
            for idx, dist in zip(indices, dists):
                match = Document(id=self.id_docid_map[str(idx)])
                match.score.value = 1 / (1 + dist)
                doc.matches.append(match)

    def close(self):
        if self.save_on_close:
            self.indexer.build(self.num_trees)
            Path(self.index_base_dir).mkdir(parents=True, exist_ok=True)
            self.indexer.save(self.index_path)
            with open(self.index_map_path, 'w') as f:
                json.dump(self.id_docid_map, f)