#! /usr/bin/env python3
# coding=utf-8

import datetime
import numpy as np
import time
import threading
import queue
 
from pyiqfeed import QuoteConn, ConnConnector, SilentQuoteListener, FeedConn


class QuoteTracker(SilentQuoteListener):

    "Quote tracker runnning in background thread"

    def __init__(self, symbols, mode='trades', port=9000, fields=None):
        super().__init__('QuoteTracker')
        assert mode in ['trades', 'quotes']
        self._symbols = symbols
        self.mode = mode
        self.port = port
        self.requested_fields = fields
        self.fundamentals = dict()
        self.last_update = dict()
        self.errors = list()
        self.bad_symbols = list()
        self.good_symbols = list()
        self._last_timestamp_local_time = None
        self.last_timestamp = None
        self._q_to_thread = queue.Queue()
        self._q_from_thread = queue.Queue()
        self._thread = None
        self.connected = False
        self.start()

    def __getitem__(self, symbol):
        return self.last_update[symbol]

    def process_timestamp(self, msg: FeedConn.TimeStampMsg):
        ts = msg.date + np.timedelta64(msg.time, 'us')
        self.last_timestamp = ts
        self._last_timestamp_local_time = datetime.datetime.now()

    def seconds_idle(self):
        if not self.connected:
            return float("inf")
        return (datetime.datetime.now() - self._last_timestamp_local_time).microseconds / 10**6

    def feed_is_stale(self) -> None:
        self.connected = False

    def feed_is_fresh(self) -> None:
        self.connected = True

    def process_error(self, fields):
        self.errors += list(fields)
        if len(self.errors) > 1024:
            self.errors = self.errors[-1024:]

    def process_invalid_symbol(self, symbol):
        self.bad_symbols.append(symbol)

    def process_summary(self, summaries):
        self.process_update(summaries)

    def process_fundamentals(self, fundamentals):
        for f in fundamentals:
            decode = lambda s: s.decode('ascii') if isinstance(s, np.bytes_) else s
            d = dict(zip(map(decode, f.dtype.names), (decode(val) for val in f)))
            symbol = d.pop('Symbol')
            self.fundamentals[symbol] = d
            self.good_symbols.append(symbol)
 
    def process_update(self, updates: np.array):
        for update in updates:
            symbol = update[0].decode('utf-8')
            self.last_update[symbol] = np.copy(update)

    def _thread_entry(self):
        conn = QuoteConn('QuoteConn(%s)'%self._name, port=self.port)
        conn.add_listener(self)
        self._conn = conn
        with ConnConnector([conn]) as connector:
            if self.requested_fields:
                conn.select_update_fieldnames(self.requested_fields)
            watch = conn.watch if self.mode == 'quotes' else conn.trades_watch
            for t in self._symbols:
                watch(t)
            self._thread_loop()
            for s in self.good_symbols:
                conn.unwatch(s)
            conn.remove_listener(self)
            self.connected = False
            conn.disconnect()
        self.conn = None

    def _thread_loop(self):
        while True:
            time.sleep(1)
            try:
                msg = self._q_to_thread.get_nowait()
                if msg == 'stop':
                    self._q_from_thread.put('stopped')
                    return
            except queue.Empty:
                pass

    def start(self):
        if self._thread is None:
            self._thread = threading.Thread(target=self._thread_entry,
                                            name=self._name + '-background')
            self._thread.start()

    def stop(self):
        if self._thread:
            self._q_to_thread.put('stop')
            self._q_from_thread.get()
            self._thread = None
            self.good_symbols = list()
            self.bad_symbols = list()


if __name__ == '__main__':

    import sys

    if len(sys.argv) != 2:
        print('Usage: quote_tracker.py path_to_tickers')

    def with_suffixes(root, sfx, sep=' '):
        return [root] + [sep.join([root, s]) for s in sfx]

    roots_with_suffixes = {
        'Last':                 ('Date', 'Time', 'Size'),
        'Bid':                  ('Time',         'Size'),
        'Ask':                  ('Time',         'Size'),
        'Most Recent Trade':    ('Date', 'Time', 'Size', 'Conditions'),
    }

    default_fields = []
    for root, sfx in roots_with_suffixes.items():
        default_fields += with_suffixes(root, sfx)

    tickers = open(sys.argv[1]).read().strip().split(' ')
    print('Loaded %d tickers from %s. Connecting...' % (len(tickers), sys.argv[1]))
    qt = QuoteTracker(tickers, 'quotes', fields=default_fields)
    qt.start()

    dt = 0.5
    t = 0
    maxt = 3
    while not qt.connected and t < 3:
        time.sleep(dt)
        t += dt

    if not qt.connected:
        print('Could not connect within %d seconds. Exiting...' % t)
        sys.exit(-1)

    time.sleep(2)
    print('%d symbols found.' % len(qt.good_symbols))

    from textwrap import TextWrapper
    tw = TextWrapper(subsequent_indent=' '*4)
    print(tw.fill('Symbols not found: %s' % ' '.join(qt.bad_symbols)))

    print('Last quote from AAPL is %s', str(qt['AAPL']))
