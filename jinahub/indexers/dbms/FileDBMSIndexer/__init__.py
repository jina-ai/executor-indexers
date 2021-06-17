__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"
import mmap

import pickle
import shutil
from pathlib import Path
from typing import List, Tuple, Generator, Optional, Iterable, Dict
import numpy as np
import os
from jina import Executor, requests, DocumentArray, Document
from jina.logging.logger import JinaLogger

from jina.helper import cached_property, get_readable_size

from jina_commons.indexers.dump import export_dump_streaming
from .file_writer import FileWriterMixin, _CloseHandler, _ReadHandler, _WriteHandler

HEADER_NONE_ENTRY = (-1, -1, -1)


class FileDBMSIndexer(Executor, FileWriterMixin):
    """
    A DBMS Indexer (no query method)

    :param index_filename: the name of the file for storing the index, when not given metas.name is used.
    :param key_length: the default minimum length of the key, will be expanded one time on the first batch
    :param args:  Additional positional arguments which are just used for the parent initialization
    :param kwargs: Additional keyword arguments which are just used for the parent initialization
    """

    def __init__(
        self,
        index_filename: Optional[str] = None,
        dump_path: Optional[str] = None,
        key_length: int = 36,
        dump_on_exit: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._index_filename = index_filename or self.metas.name
        self._dump_path = dump_path or self.default_dump_path

        self.key_length = key_length

        self.handler_mutex = True
        self.is_handler_loaded = False

        self._page_size = mmap.ALLOCATIONGRANULARITY
        self.logger = JinaLogger(getattr(self.metas, 'name', self.__class__.__name__))
        self._dump_on_exit = dump_on_exit

        if self.is_exist:
            self._size = self.query_handler.size()
            self._start = self.query_handler.total_bytes
        else:
            self._size = 0
            self._start = 0

    @cached_property
    def query_handler(self):
        """A readable and indexable object, could be dict, map, list, numpy array etc.

        :return: read handler

        .. note::
            :attr:`query_handler` and :attr:`write_handler` are by default mutex
        """
        r = None
        if not self.handler_mutex or not self.is_handler_loaded:
            r = self.get_query_handler()
            if r is None:
                self.logger.warning(
                    f'you can not query from {self} as its "query_handler" is not set. '
                    'If you are indexing data from scratch then it is fine. '
                    'If you are querying data then the index file must be empty or broken.'
                )
            else:
                self.logger.info(f'indexer size: {r.size()}')
                self.is_handler_loaded = True
        return r

    @property
    def is_exist(self) -> bool:
        """
        Check if the database exist or not

        :return: true if the absolute index path exists, else false
        """
        return os.path.exists(self.index_abspath)

    @cached_property
    def write_handler(self):
        """A writable and indexable object.

        :return: write handler

        .. note::
            :attr:`query_handler` and :attr:`write_handler` are by default mutex
        """

        # ! a || ( a && b )
        # =
        # ! a || b
        if not self.handler_mutex or not self.is_handler_loaded:
            r = self.get_add_handler() if self.is_exist else self.get_create_handler()

            if r is None:
                self.logger.warning(
                    '"write_handler" is None, you may not add data to this index, '
                    'unless "write_handler" is later assigned with a meaningful value'
                )
            else:
                self.is_handler_loaded = True
            return r

    @property
    def size(self) -> int:
        """
        The number of documents indexed.

        :return: size
        """
        return self._size

    @property
    def default_dump_path(self):
        """
        Path to dump to, if none is given to the constructor.

        :return: default dump path
        """
        return os.path.join(self.workspace, 'default_dump')

    def _delete_invalid_indices(self):
        # make sure the file is closed before querying.
        with _CloseHandler(handler=self.write_handler):
            pass

        keys = []
        vals = []
        # we read the valid values and write them to the intermediary file
        with _CloseHandler(
            handler=_ReadHandler(self.index_abspath, self.key_length)
        ) as close_handler:
            for key in close_handler.handler.header.keys():
                pos_info = close_handler.handler.header.get(key, None)
                if pos_info:
                    p, r, l = pos_info
                    with mmap.mmap(close_handler.handler.body, offset=p, length=l) as m:
                        keys.append(key)
                        vals.append(m[r:])
        if len(keys) == 0:
            return

        # intermediary file
        tmp_file = self.index_abspath + '-tmp'
        self._start = 0
        with _CloseHandler(handler=_WriteHandler(tmp_file, 'ab')) as close_handler:
            # reset size
            self._size = 0
            self._add(keys, vals, write_handler=close_handler.handler)

        # replace orig. file
        # and .head file
        head_path = self.index_abspath + '.head'
        os.remove(self.index_abspath)
        os.remove(head_path)
        os.rename(tmp_file, self.index_abspath)
        os.rename(tmp_file + '.head', head_path)

    def close(self):
        """Close all file-handlers and release all resources. """
        bytes_before = self.physical_size()
        self.logger.info(
            f'indexer size: {self.size} physical size: {get_readable_size(self.physical_size())}'
        )
        self._delete_invalid_indices()
        if bytes_before != bytes_before:
            self.logger.info(
                f'Clean up of deleting entries happend in .close(). New physical size: {bytes_before}'
            )
        if self._dump_on_exit:
            shutil.rmtree(self._dump_path, ignore_errors=True)
            self.dump({'dump_path': self._dump_path})
        if self.write_handler:
            self.write_handler.flush()
            self.write_handler.close()
        if self.query_handler:
            self.query_handler.close()
        super().close()

    def _filter_nonexistent_keys_values(
        self, keys: Iterable, values: Iterable, existent_keys: Iterable
    ) -> Tuple[Iterable, Iterable]:
        f = [(key, value) for key, value in zip(keys, values) if key in existent_keys]
        if f:
            return zip(*f)
        else:
            return None, None

    def _filter_nonexistent_keys(
        self, keys: Iterable, existent_keys: Iterable
    ) -> Iterable:
        return [key for key in keys if key in set(existent_keys)]

    @property
    def index_abspath(self) -> str:
        """
        Get the file path of the index storage

        :return: absolute path
        """
        os.makedirs(self.workspace, exist_ok=True)
        return os.path.join(self.workspace, self._index_filename)

    def _get_generator(
        self, ids: List[str]
    ) -> Generator[Tuple[str, np.array, bytes], None, None]:
        for id_ in ids:
            vecs_metas_bytes = super()._query([id_])[0]
            if vecs_metas_bytes is not None:
                vec, meta = pickle.loads(vecs_metas_bytes)
                yield id_, vec, meta

    @requests(on='/dump')
    def dump(self, parameters, *args, **kwargs) -> None:
        """Dump the index

        :param parameters: requests parameters
        :param args: not used
        :param kwargs: not used
        """
        if self.write_handler:
            self.write_handler.close()
        # noinspection PyPropertyAccess
        del self.write_handler
        self.handler_mutex = False
        ids = self.query_handler.header.keys()
        export_dump_streaming(
            parameters['dump_path'],
            shards=parameters.get('shards', 1),
            size=len(ids),
            data=self._get_generator(ids),
        )
        self.query_handler.close()
        self.handler_mutex = False
        # noinspection PyPropertyAccess
        del self.query_handler

    @requests(on='/index')
    def add(self, docs: DocumentArray, *args, **kwargs):
        """Add to the DBMS Indexer, both vectors and metadata

        :param docs: `DocumentArray` of to-be-added Documents
        :param args: not used
        :param kwargs: not used
        """

        ids, vecs, metas = self._unpack_docs(docs)
        if not any(ids):
            return

        vecs_metas = [pickle.dumps((vec, meta)) for vec, meta in zip(vecs, metas)]
        with self.write_handler as write_handler:
            self._add(ids, vecs_metas, write_handler)

    @requests(on='/update')
    def update(self, docs: DocumentArray, *args, **kwargs):
        """Update the DBMS Indexer, both vectors and metadata

        :param docs: `DocumentArray` of to-be-updated Documents
        :param args: not used
        :param kwargs: not used
        """
        ids, vecs, metas = self._unpack_docs(docs)
        vecs_metas = [pickle.dumps((vec, meta)) for vec, meta in zip(vecs, metas)]
        keys, vecs_metas = self._filter_nonexistent_keys_values(
            ids, vecs_metas, self.query_handler.header.keys()
        )
        del self.query_handler
        self.handler_mutex = False
        if keys:
            self._delete(keys)
            with self.write_handler as write_handler:
                self._add(keys, vecs_metas, write_handler)

    @requests(on='/delete')
    def delete(self, docs: DocumentArray, **kwargs):
        """Delete document from the database.

        :param docs: list of Documents
        """
        FileWriterMixin.delete(self, [doc.id for doc in docs])

    def _unpack_docs(self, docs: DocumentArray, *args, **kwargs) -> None:
        info = [
            (
                doc.id,
                doc.embedding,
                self._doc_without_embedding(doc).SerializeToString(),
            )
            for doc in docs
        ]
        if info:
            ids, vecs, metas = zip(*info)
            self.check_key_length(ids)
            return ids, vecs, metas

    @staticmethod
    def _doc_without_embedding(d):
        new_doc = Document(d, copy=True)
        new_doc.ClearField('embedding')
        return new_doc

    def check_key_length(self, val: Iterable[str]):
        """
        Check if the max length of val(e.g. doc id) is larger than key_length.

        :param val: The values to be checked
        """
        m_val = max(len(v) for v in val)
        if m_val > self.key_length:
            raise ValueError(
                f'BinaryPbDBMSIndexer allows only keys of length {self.key_length}, '
                f'but yours is {m_val}.'
            )

    def physical_size(self) -> int:
        """Return the size of the workspace in bytes

        :return: byte size of the given directory
        """
        root_directory = Path(self.workspace)
        return sum(f.stat().st_size for f in root_directory.glob('**/*') if f.is_file())

    def _get(self, docs: DocumentArray, **kwargs):
        """Get a document by its id

        :param docs: the documents
        """
        self._search(docs, is_update=False)

    def _search(self, docs: DocumentArray, is_update):
        for i, doc in enumerate(docs):
            vec, doc_bytes = pickle.loads(self._query([doc.id])[0])
            serialized_doc = Document(doc_bytes)
            serialized_doc.pop('content_hash')
            if is_update:
                doc.update(serialized_doc)
            else:
                doc = Document(serialized_doc, copy=True)
            doc.embedding = vec
            doc.update_content_hash()
            docs[i] = doc