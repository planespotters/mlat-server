# -*- mode: python; indent-tabs-mode: nil -*-

# Part of mlat-server: a Mode S multilateration server
# Copyright (C) 2026  Thomas Noack <thomas.noack@planespotters.net>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
Records all client input to a file so it can be replayed into a server later.

The file is tab separated text, one record per line (gzip compressed if the
filename ends in .gz):

  <utc>\t<conn>\t<type>\t<payload>

utc is the server time the input was processed, conn is a capture-local
connection id (0 for records not tied to a connection). Types:

  S  capture started, payload is JSON {"format": 1}
  C  client connected, empty payload
  H  handshake line as received (after any PROXY line)
  A  handshake accepted, payload is JSON {"user": ..., "udp_key": ... or null}
  D  handshake denied, payload is the reason
  M  message line, as received over TCP (after decompression)
  U  UDP datagram, payload is the datagram as hex
  X  client disconnected, empty payload
  E  capture ended, empty payload

The payload is the last field and may itself contain tabs, so readers should
split with a limit of 3. Capture starts with the server so that the handshakes
of all clients are recorded.
"""

import gzip
import logging
import time
import ujson

from mlat import util

glogger = logging.getLogger("capture")

FORMAT_VERSION = 1


class Capture(object):
    def __init__(self, filename, duration, loop):
        """filename may contain strftime() placeholders. duration is in
        seconds; capture stops after that, 0 means no limit."""
        self.filename = time.strftime(filename)
        self.duration = duration
        self.loop = loop
        self.f = None
        self._stop_handle = None
        self._next_id = 1
        self._udp_keys = {}  # udp key -> connection id

    def start(self):
        try:
            if self.filename.endswith('.gz'):
                self.f = gzip.open(self.filename, 'xt', compresslevel=1, encoding='utf-8')
            else:
                self.f = open(self.filename, 'x', encoding='utf-8', buffering=1024*1024)
        except OSError:
            glogger.exception("Could not open capture file, capture disabled")
            return util.completed_future

        glogger.warning("Capturing client input to {0}".format(self.filename))
        self._write(0, 'S', ujson.dumps({'format': FORMAT_VERSION}))
        if self.duration > 0:
            self._stop_handle = self.loop.call_later(self.duration, self.stop)
        return util.completed_future

    def stop(self):
        if self.f is None:
            return
        if self._stop_handle:
            self._stop_handle.cancel()
            self._stop_handle = None

        self._write(0, 'E', '')
        try:
            self.f.close()
        except OSError:
            glogger.exception("Could not close capture file")
        self.f = None
        glogger.warning("Capture to {0} stopped".format(self.filename))

    def close(self):
        self.stop()

    def wait_closed(self):
        return util.completed_future

    def _write(self, conn, rtype, payload):
        try:
            self.f.write(f'{time.time():.6f}\t{conn}\t{rtype}\t{payload}\n')
        except OSError:
            glogger.exception("Could not write capture file, capture stopped")
            f = self.f
            self.f = None
            try:
                f.close()
            except OSError:
                pass

    def new_connection(self):
        """Returns the id for a new client connection."""
        conn = self._next_id
        self._next_id += 1
        if self.f is not None:
            self._write(conn, 'C', '')
        return conn

    def handshake(self, conn, line):
        if self.f is not None:
            self._write(conn, 'H', line.rstrip('\r\n'))

    def accepted(self, conn, user, udp_key):
        if udp_key is not None:
            self._udp_keys[udp_key] = conn
        if self.f is not None:
            self._write(conn, 'A', ujson.dumps({'user': user, 'udp_key': udp_key}))

    def denied(self, conn, reason):
        if self.f is not None:
            self._write(conn, 'D', reason)

    def message(self, conn, line):
        if self.f is not None:
            self._write(conn, 'M', line.rstrip('\r\n'))

    def udp(self, key, data):
        if self.f is not None:
            conn = self._udp_keys.get(key)
            if conn is not None:
                self._write(conn, 'U', data.hex())

    def disconnected(self, conn, udp_key):
        if udp_key is not None:
            self._udp_keys.pop(udp_key, None)
        if self.f is not None:
            self._write(conn, 'X', '')
