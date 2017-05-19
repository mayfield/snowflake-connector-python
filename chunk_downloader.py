#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2017 Snowflake Computing Inc. All right reserved.
#

import contextlib
import json
import random
import threading
import time
import weakref
from concurrent import futures
from logging import getLogger

from .errorcode import ER_CHUNK_DOWNLOAD_FAILED
from .errors import (Error, OperationalError)
from .ssl_wrap_socket import set_proxies

MAX_RETRY_DOWNLOAD = 5
WAIT_TIME_IN_SECONDS = 120

# Scheduling ramp constants.  Use caution if changing these.
SCHED_ACTIVE_CEIL = 24  # Adjust down to reduce max memory usage.
SCHED_READY_PCT_CEIL = 0.8  # Maximum ready/total-pending ratio.
SCHED_RATE_WINDOW = slice(-2, None)  # Adjusts window size for rate est.
SCHED_GROWTH_FACTOR = 0.4  # Adjusts how fast concurrency scales.
SCHED_GROWTH_MIN = 1  # Minimum jump for concurrency growth.
SCHED_RATE_FLOOR_PCT = 0.80
SCHED_CONCUR_CEIL_PCT = 0.70
SCHED_ACTIVE_FLOOR = 2  # Minimum amount of concurrency for downloads.
SCHED_MIN_CHUNKS = 10  # Number of initial chunks before scheduler kicks in.

SSE_C_ALGORITHM = u"x-amz-server-side-encryption-customer-algorithm"
SSE_C_KEY = u"x-amz-server-side-encryption-customer-key"
SSE_C_AES = u"AES256"

logger = getLogger(__name__)
timer = getattr(time, 'perf_counter', time.time)


class ChunkTracker(object):
    u"""
    Individual stat for each download.  Used to track progress and completion.
    """

    def __init__(self, lock, length):
        self._lock = lock
        self.length = length
        self.started = timer()
        self.finished = None
        self.accum = 0

    def copy(self):
        obj = type(self)(None, None)
        obj.__dict__.update(self.__dict__)
        return obj

    def __enter__(self):
        return self

    def __exit__(self, *na):
        with self._lock:
            self.finished = timer()

    def set_accum(self, n):
        with self._lock:
            self.accum = n

    @property
    def done(self):
        with self._lock:
            return self.finished is not None

    @property
    def elapsed(self):
        with self._lock:
            end = self.finished if self.finished is not None else timer()
            return end - self.started


class ChunkStats(object):
    u"""
    Organize the stats for chunk downloads so questions about aggregate
    bandwidth can be asked in a reasonable way.
    """

    def __init__(self, trackers=None):
        self._trackers = trackers or []
        self._lock = threading.RLock()

    def track(self, length):
        tracker = ChunkTracker(self._lock, length)
        with self._lock:
            self._trackers.append(tracker)
        return tracker

    def __len__(self):
        with self._lock:
            return len(self._trackers)

    def __getitem__(self, rng):
        u"""
        Return the elements falling between the slice arg.  The values for the
        slice should be timestamps generated by `timer`.  Data with a partial
        intersection of the range are prorated.
        """
        assert isinstance(rng, slice), 'Only slices supported'
        assert rng.step is None, 'Step not supported'
        now = timer()
        start = rng.start if rng.start is None or rng.start >= 0 else \
            now + rng.start
        stop = rng.stop if rng.stop is None or rng.stop >= 0 else \
            now + rng.stop
        out = []
        with self._lock:
            for t in self._trackers:
                if (start is None or not t.done or start < t.finished) and \
                   (stop is None or stop > t.started):
                    partial = t.copy()
                    if start is not None and start > t.started:
                        partial.started = start
                    end = partial.finished if partial.done else now
                    if stop is not None and stop < end:
                        partial.finished = stop
                    partial.accum *= partial.elapsed / t.elapsed
                    out.append(partial)
        return type(self)(out)

    def walltime(self):
        u"""
        The amount of time between the first start and last finish of all our
        trackers.
        """
        with self._lock:
            if not self._trackers:
                return 0
            now = timer()
            first_start = min(x.started for x in self._trackers)
            last_finish = max(x.finished if x.done else now
                              for x in self._trackers)
            return last_finish - first_start

    def rate(self):
        u"""
        The overall rate (units per second) of the trackers in this collection.
        Returns `None` if no records are present.
        """
        with self._lock:
            walltime = self.walltime()
            if walltime:
                return sum(x.accum for x in self._trackers) / walltime

    def concurrency(self):
        u"""
        The average amount of concurrency achieved for the trackers in this
        collection.  E.g how many trackers were active at the same time.
        Returns `None` if no records are present.
        """
        with self._lock:
            walltime = self.walltime()
            if walltime:
                return sum(x.elapsed for x in self._trackers) / walltime


class SnowflakeChunkDownloader(object):
    u"""
    Large Result set chunk downloader class.
    """

    _sched_active_ceil = SCHED_ACTIVE_CEIL
    _sched_active_floor = SCHED_ACTIVE_FLOOR
    _sched_ready_pct_ceil = SCHED_READY_PCT_CEIL
    _sched_rate_window = SCHED_RATE_WINDOW
    _sched_growth_factor = SCHED_GROWTH_FACTOR
    _sched_growth_min = SCHED_GROWTH_MIN
    _sched_rate_floor_pct = SCHED_RATE_FLOOR_PCT
    _sched_concur_ceil_pct = SCHED_CONCUR_CEIL_PCT
    _sched_min_chunks = SCHED_MIN_CHUNKS

    def __init__(self, chunks, connection, cursor, qrmk, chunk_headers,
                 use_ijson=False):
        self._use_ijson = use_ijson
        self._connection = connection
        self._cursor = cursor
        self._qrmk = qrmk
        self._headers = chunk_headers
        self._manifests = chunks
        self._total = len(chunks)
        self._calling_thread = None
        self._consumed = 0
        self._sched_lock = threading.RLock()
        self._sched_work = {}
        self._sched_cursor = 0
        self._sched_active = 0
        self._sched_active_ideal = self._sched_active_floor
        self._sched_ready = 0
        self._stats = ChunkStats()
        self._stats_hist = []

    def __iter__(self):
        return self

    def next(self):
        self.assertFixedThread()
        idx = self._consumed
        if idx >= self._total:
            raise StopIteration()
        with self._sched_lock:
            if idx == self._sched_cursor:
                logger.warning(u'chunk downloader reached starvation')
                self.sched_next()
        for attempt in range(MAX_RETRY_DOWNLOAD + 1):
            if attempt:
                logger.warning(u'retrying chunk %d download (retry %d/%d)',
                               idx + 1, attempt, MAX_RETRY_DOWNLOAD)
                self._sched(idx, retry=attempt)
            with self._sched_lock:
                fut = self._sched_work.pop(idx)
            start_ts = timer()
            while True:
                try:
                    rows = fut.result(timeout=0.500)
                except futures.TimeoutError:
                    elapsed = timer() - start_ts
                    if elapsed < WAIT_TIME_IN_SECONDS:
                        # Not an error, just time to feed `sched_tick` if it
                        # wants to enqueue more download jobs.
                        self._sched_tick()
                    else:
                        logger.warning(
                            u'chunk %d download timed out after %g second(s)',
                            idx + 1, elapsed)
                        with self._sched_lock:
                            fut.cancel()
                            self._sched_active -= 1
                            break
                else:
                    self._consumed += 1
                    with self._sched_lock:
                        self._sched_ready -= 1
                    self._sched_tick()
                    return rows
        Error.errorhandler_wrapper(
            self._connection,
            self._cursor,
            OperationalError,
            {
                u'msg': u'The result set chunk download fails or hang for '
                        u'unknown reason.',
                u'errno': ER_CHUNK_DOWNLOAD_FAILED
            })

    __next__ = next

    def assertFixedThread(self):
        u"""
        Ensure threadsafety == 2 is enforced.
        https://www.python.org/dev/peps/pep-0249/#threadsafety
        """
        current = threading.current_thread()
        if self._calling_thread is None:
            self._calling_thread = weakref.ref(current)
        else:
            expected = self._calling_thread()
            assert current is expected, '%r is not %r' % (current, expected)

    def sched_next(self):
        with self._sched_lock:
            idx = self._sched_cursor
            if idx >= self._total:
                return None
            self._sched_chunk(idx)
            self._sched_cursor += 1
            return idx

    def _sched_chunk(self, idx, retry=None):
        u"""
        Schedule a download in a background thread.  Return a Future object
        that represents the eventual result.
        """
        future = futures.Future()
        with self._sched_lock:
            assert idx not in self._sched_work or \
                   self._sched_work[idx].cancelled()
            self._sched_work[idx] = future
        tname = 'ChunkDownloader_%d' % (idx + 1)
        if retry is not None:
            tname += '_retry_%d' % retry
        t = threading.Thread(name=tname,
                             target=self._fetch_chunk_worker_runner,
                             args=(future, self._manifests[idx]))
        t.daemon = True
        with self._sched_lock:
            self._sched_active += 1
        t.start()
        return future

    def _fetch_chunk_worker_runner(self, future, chunk):
        u"""
        Entry point for ChunkDownloader threads.  Thread safety rules apply
        from here out.
        """
        try:
            rows = self._fetch_chunk_worker(chunk)
        except BaseException as e:
            exc = e
        else:
            exc = None
        with self._sched_lock:
            if future.cancelled():
                if exc is None:
                    logger.warning(u"ignoring good result from cancelled work")
                return
            self._sched_active -= 1
            self._sched_ready += 1
            if exc is not None:
                future.set_exception(exc)
            else:
                future.set_result(rows)
        self._sched_tick()

    def _sched_tick(self):
        u"""
        Threadsafe scheduler for chunk queue management.  This is a variant of
        a hillclimbing algo that tries to hone in on the optimal amount of
        network concurrency.  Each "tick" monitors progress, makes any changes
        to the amount of maximum concurrency and may start new chunk downloads.
        """
        with self._sched_lock:
            if self._sched_cursor >= self._total:
                return
            if self._sched_cursor > self._sched_min_chunks:
                rates = self._stats[self._sched_rate_window]
                rate = rates.rate()
                if rate is not None:
                    reco = self._sched_recommend(rate, rates.concurrency())
                    if reco is not None:
                        self._sched_set_active_ideal(reco)
            if self._sched_ready <= max(1, self._sched_active_ideal *
                                        self._sched_ready_pct_ceil):
                add = int(round(self._sched_active_ideal)) - self._sched_active
                for i in range(add):
                    logger.info(u'<bgyellow><black>scheduling <u>%d</u>: active: %d, ready: %d, '
                                u'ideal: %f', i, self._sched_active,
                                self._sched_ready, self._sched_active_ideal)
                    self.sched_next()

    def _sched_set_active_ideal(self, recomendation):
        u"""
        Bounded update of the idealized active download count.
        """
        self._sched_active_ideal = min(self._sched_active_ceil,
                                       max(self._sched_active_floor,
                                           recomendation))

    def _sched_recommend(self, rate, concur):
        u"""
        Evaluate performance heuristics and make a recommendation about how
        much concurrency to use.
        """
        if self._stats_hist and self._stats_hist[-1] == (rate, concur):
            logger.warning("Nothing new to report!1!!!!! maybve nowt?")
            return
        self._stats_hist.append((rate, concur))
        best_rate, best_concur = max(self._stats_hist)
        good_rate = self._sched_rate_floor_pct * best_rate
        good_concur = min(c for rate, c in self._stats_hist
                          if rate >= good_rate)
        peak_concur = max(x[1] for x in self._stats_hist)
        logger.info("<i>C    PEAK:<red>%10f</red>  - BEST:<blue>%10f (%4d)</blue>   - "
                    "GOOD:<green>%10f (%4d)</green>  - CURRENT:<cyan>%10f (%4d)",
                    peak_concur, best_concur, best_rate // 2**17, good_concur,
                    good_rate // 2**17, concur, rate // 2**17)
        if best_concur > (peak_concur * self._sched_concur_ceil_pct):
            # We have not explored the upper limits enough.
            reco = peak_concur + max(peak_concur * self._sched_growth_factor,
                                     self._sched_growth_min)
            logger.info("<b><cyan>PUSH UP2! %f", reco)
        else:
            reco = random.randint(int(round(good_concur)),
                                  int(round(best_concur)))
            logger.info("<b><yellow>HOLD2 %f", reco)
        return reco

    def _fetch_chunk_worker(self, chunk):
        u"""
        Thread worker to fetch the chunk from S3.
        """
        if self._headers is not None:
            headers = self._headers
        else:
            headers = {}
            if self._qrmk is not None:
                headers[SSE_C_ALGORITHM] = SSE_C_AES
                headers[SSE_C_KEY] = self._qrmk
        rest = self._connection.rest
        proxies = set_proxies(rest._proxy_host, rest._proxy_port,
                              rest._proxy_user, rest._proxy_password)
        timeouts = rest._connect_timeout, rest._request_timeout
        buf = ['[']
        with rest._use_requests_session() as session:
            resp = session.get(chunk['url'], proxies=proxies, headers=headers,
                               timeout=timeouts, verify=True, stream=True)
            with contextlib.closing(resp):
                resp.raise_for_status()
                size = int(resp.headers.get('content-length'))
                with self._stats.track(size) as tracker:
                    for x in resp.iter_content(max(size // 100, 4096)):
                        tracker.set_accum(resp.raw.tell())
                        buf.append(x.decode())
        buf.append(']')
        return json.loads(''.join(buf))

    def terminate(self):
        u"""
        Terminates downloading the chunks.
        """
        with self._sched_lock:
            futures = list(self._sched_work.values())
            self._sched_work = None
        for f in futures:
            f.cancel()

    def __del__(self):
        try:
            self.terminate()
        except:
            pass
