#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2017 Snowflake Computing Inc. All right reserved.
#

from collections import namedtuple
from concurrent import futures
from logging import getLogger

from .errorcode import (ER_NO_ADDITIONAL_CHUNK, ER_CHUNK_DOWNLOAD_FAILED)
from .errors import (Error, OperationalError)

DEFAULT_REQUEST_TIMEOUT = 3600
DEFAULT_CLIENT_RESULT_PREFETCH_SLOTS = 2
DEFAULT_CLIENT_RESULT_PREFETCH_THREADS = 1

#MAX_RETRY_DOWNLOAD = 10
#MAX_WAIT = 120
#WAIT_TIME_IN_SECONDS = 10
MAX_RETRY_DOWNLOAD = 3
MAX_WAIT = 30
WAIT_TIME_IN_SECONDS = 1

SSE_C_ALGORITHM = u"x-amz-server-side-encryption-customer-algorithm"
SSE_C_KEY = u"x-amz-server-side-encryption-customer-key"
SSE_C_AES = u"AES256"

SnowflakeChunk = namedtuple('SnowflakeChunk', [
    'url',  # S3 bucket URL to download the chunk
    'row_count',  # number of rows in the chunk
    'result_data',  # pointer to the generator of the chunk
    'ready'  # True if ready to consume or False
])

logger = getLogger(__name__)

def hello(*args, **kwargs):
    logger.critical("HELLO %s %s", args, kwargs)
    return [123]

class SnowflakeChunkDownloader(object):
    u"""
    Large Result set chunk downloader class.
    """

    def _pre_init(self, chunks, connection, cursor, qrmk, chunk_headers,
                  prefetch_slots=DEFAULT_CLIENT_RESULT_PREFETCH_SLOTS,
                  prefetch_threads=DEFAULT_CLIENT_RESULT_PREFETCH_THREADS,
                  use_ijson=False, mp_mode=False, workers=48):
        self._use_ijson = use_ijson

        self._downloader_error = None

        self._connection = connection
        self._cursor = cursor
        self._qrmk = qrmk
        self._chunk_headers = chunk_headers

        self._prefetch_slots = prefetch_slots
        self._prefetch_threads = prefetch_threads

        self._chunk_size = len(chunks)
        self._chunks = {}
        self._chunk_futures = {}

        self._prefetch_threads *= 2

        self._effective_threads = min(self._prefetch_threads, self._chunk_size)
        if self._effective_threads < 1:
            self._effective_threads = 1

        self._num_chunks_to_prefetch = min(self._prefetch_slots,
                                           self._chunk_size)

        for idx, chunk in enumerate(chunks):
            logger.info(u"queued chunk %d: rowCount=%s" % (idx, chunk[u'rowCount']))
            self._chunks[idx] = SnowflakeChunk(
                url=chunk[u'url'],
                result_data=None,
                ready=False,
                row_count=int(chunk[u'rowCount']))

        logger.debug(u'prefetch slots: %s, '
                     u'prefetch threads: %s, '
                     u'number of chunks: %s, '
                     u'effective threads: %s',
                     self._prefetch_slots,
                     self._prefetch_threads,
                     self._chunk_size,
                     self._effective_threads)

        if mp_mode:
            logger.warn("Using Process Executor: %s workers", workers or "<ncpus>")
            Executor = futures.ProcessPoolExecutor
        else:
            logger.warn("Using Thread Executor: %s workers", workers or "<ncpus>")
            Executor = futures.ThreadPoolExecutor
        self._pool = Executor(max_workers=workers) #self._effective_threads)

        self._total_millis_downloading_chunks = 0
        self._total_millis_parsing_chunks = 0

        self._next_chunk_to_consume = 0

    def __init__(self, chunks, connection, cursor, qrmk, chunk_headers,
                 prefetch_slots=DEFAULT_CLIENT_RESULT_PREFETCH_SLOTS,
                 prefetch_threads=DEFAULT_CLIENT_RESULT_PREFETCH_THREADS,
                 **kwargs):
        self._pre_init(chunks, connection, cursor, qrmk, chunk_headers,
                       prefetch_slots=prefetch_slots,
                       prefetch_threads=prefetch_threads,
                       **kwargs)
        logger.info('Chunk Downloader in memory')
        for idx in range(self._num_chunks_to_prefetch):
            self._sched_chunk_download(idx)
        self._next_chunk_to_download = self._num_chunks_to_prefetch

    def next_chunk(self):
        """
        Gets and/or waits for the next chunk.
        """
        logger.debug(
            u'next_chunk_to_consume={next_chunk_to_consume}, '
            u'next_chunk_to_download={next_chunk_to_download}, '
            u'total_chunks={total_chunks}'.format(
                next_chunk_to_consume=self._next_chunk_to_consume + 1,
                next_chunk_to_download=self._next_chunk_to_download + 1,
                total_chunks=self._chunk_size))
        active_idx = self._next_chunk_to_consume
        if active_idx > 0:
            # clean up the previously fetched data and lock
            if self._next_chunk_to_download < self._chunk_size:
                self._sched_chunk_download(self._next_chunk_to_download)
                self._next_chunk_to_download += 1

        if active_idx >= self._chunk_size:
            Error.errorhandler_wrapper(
                self._connection, self._cursor,
                OperationalError,
                {
                    u'msg': u"expect a chunk but got None",
                    u'errno': ER_NO_ADDITIONAL_CHUNK})

        import time
        wait_start = time.perf_counter()
        for attempt in range(MAX_RETRY_DOWNLOAD):
            logger.debug(u'waiting for chunk %s/%s'
                         u' in %s/%s download attempt',
                         active_idx + 1,
                         self._chunk_size,
                         attempt + 1,
                         MAX_RETRY_DOWNLOAD)
            fchunk = self._chunk_futures[active_idx]
            for wait_counter in range(MAX_WAIT):
                try:
                    result = fchunk.result(timeout=WAIT_TIME_IN_SECONDS)
                except futures.TimeoutError:
                    logger.warn(u'chunk %s/%s is NOT ready to consume'
                                u' in %s/%s(s)',
                                active_idx + 1,
                                self._chunk_size,
                                (wait_counter + 1) * WAIT_TIME_IN_SECONDS,
                                MAX_WAIT * WAIT_TIME_IN_SECONDS)
                else:
                    now = time.perf_counter()
                    logger.critical("DOWNLOAD/RENDER WAITED %f, age %f seconds" % (now - wait_start,
                        now - fchunk._started))
                    logger.debug(u'chunk %s/%s is being consumed',
                                 active_idx + 1,
                                 self._chunk_size)
                    self._next_chunk_to_consume += 1
                    del self._chunk_futures[active_idx]
                    chunk = self._chunks.pop(active_idx)
                    return chunk._replace(result_data=iter(result), ready=True)
            else:
                logger.warn(
                    u'chunk %s/%s is still NOT ready. Restarting chunk '
                    u'download',
                    active_idx + 1,
                    self._chunk_size)
                self._sched_chunk_download(active_idx, restart=True)
        Error.errorhandler_wrapper(
            self._connection,
            self._cursor,
            OperationalError,
            {
                u'msg': u'The result set chunk download failed or hung for an '
                        u'unknown reason.',
                u'errno': ER_CHUNK_DOWNLOAD_FAILED
            })

    def terminate(self):
        """
        Terminates downloading the chunks.
        """
        futures = self._chunk_futures.values()
        self._futures = None
        pool = self._pool
        self._pool = None
        for f in futures:
            f.cancel()
        pool.shutdown()

    def __del__(self):
        try:
            self.terminate()
        except:
            # ignore all errors in the destructor
            pass

    def _fetch_chunk(self, url, headers):
        """
        Fetch the chunk from S3.
        """
        timeouts = (
            self._connection._connect_timeout,
            self._connection._connect_timeout,
            DEFAULT_REQUEST_TIMEOUT
        )
        data = self._connection.rest.fetch(u'get', url, headers,
            timeouts=timeouts, is_raw_binary=True,
            is_raw_binary_iterator=False, use_ijson=self._use_ijson)
        return [self._cursor.row_to_python(x) if x is not None else None
                for x in data]

    def _make_fetch_manifest(self, url, headers):
        """ Produce static multiprocessing safe worker for fetching data. """
        manifest = {
            "fetch_fn": self._connection.rest.fetch,
            "fetch_args": ('get', url, headers),
            "fetch_kwargs": {
                "timeouts": (
                    self._connection._connect_timeout,
                    self._connection._connect_timeout,
                    DEFAULT_REQUEST_TIMEOUT
                ),
                "is_raw_binary": True,
                "is_raw_binary_iterator": False
            },
            "chunk_rowtype": self._cursor._chunk_rowtype,
            "chunk_version": self._cursor._chunk_version,
            "converter": self._connection.converter
        }
        self._connection.rest._connection = 'nope' # XXX
        return manifest

    @staticmethod
    def _fetch_chunk_worker(manifest):
        """
        Executor worker thread/process that fetchs and renders a chunk.
        """
        m = manifest
        converters = m['converter'].make_converters(m['chunk_rowtype'],
                                                    m['chunk_version'])
        data = m['fetch_fn'](*m['fetch_args'], **m['fetch_kwargs'])
        return [[col if conv is None or col is None else conv(col)
                 for conv, col in zip(converters, row)]
                if row is not None else None
                for row in data]

    def _sched_chunk_download(self, idx, restart=False):
        """
        Submit a chunk download job into the executor pool and store the future
        for result processing.
        """
        if restart:
            logger.warn("Cancelling chunk %d", idx + 1)
            self._chunk_futures[idx].cancel()
        elif idx in self._chunk_futures and \
           not self._chunk_futures[idx].cancelled():
            raise RuntimeError('Attempt to reschedule active chunk.')
        logger.info(u'scheduling chunk %d/%d', idx + 1, self._chunk_size)
        headers = {}
        if self._chunk_headers is not None:
            headers = self._chunk_headers
        elif self._qrmk is not None:
            headers[SSE_C_ALGORITHM] = SSE_C_AES
            headers[SSE_C_KEY] = self._qrmk
        manifest = self._make_fetch_manifest(self._chunks[idx].url, headers)
        f = self._pool.submit(self._fetch_chunk_worker, manifest)
        self._chunk_futures[idx] = f
        import time
        assert not hasattr(f, '_started')
        f._started = time.perf_counter()
        return f
