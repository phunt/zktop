#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from optparse import OptionParser

import curses
import threading, Queue
import socket
import signal
import re, StringIO

import logging as LOG
LOG_FILENAME = 'zktop_log.out'
LOG.basicConfig(filename=LOG_FILENAME,level=LOG.DEBUG)

usage = "usage: %prog [options]"
parser = OptionParser(usage=usage)
parser.add_option("", "--servers", dest="servers",
                  default="localhost:2181", help="comma separated list of host:port (default localhost:2181)")

parser.add_option("-n", "--names",
                  action="store_true", dest="names", default=False,
                  help="resolve session name from ip (default False)")
parser.add_option("", "--fix_330",
                  action="store_true", dest="fix_330", default=False,
                  help="workaround for a bug in ZK 3.3.0")

(options, args) = parser.parse_args()

resized_sig = False

# threads to get server data
# UI class
# track current data and historical

class Session(object):
    def __init__(self, session, server_id):
        # allow both ipv4 and ipv6 addresses
        m = re.search('/([\da-fA-F:\.]+):(\d+)\[(\d+)\]\((.*)\)', session)
        self.host = m.group(1)
        self.port = m.group(2)
        self.server_id = server_id
        self.interest_ops = m.group(3)
        for d in m.group(4).split(","):
            k,v = d.split("=")
            self.__dict__[k] = v

class ZKServer(object):
    def __init__(self, server, server_id):
        self.server_id = server_id
        self.host, self.port = server.split(':')
        try:
            stat = send_cmd(self.host, self.port, 'stat\n')

            sio = StringIO.StringIO(stat)
            line = sio.readline()
            m = re.search('.*: (\d+\.\d+\.\d+)-.*', line)
            self.version = m.group(1)
            sio.readline()
            self.sessions = []
            for line in sio:
                if not line.strip():
                    break
                self.sessions.append(Session(line.strip(), server_id))
            for line in sio:
                attr, value = line.split(':')
                attr = attr.strip().replace(" ", "_").replace("/", "_").lower()
                self.__dict__[attr] = value.strip()

            self.min_latency, self.avg_latency, self.max_latency = self.latency_min_avg_max.split("/")

            self.unavailable = False
        except:
            self.unavailable = True
            self.mode = "Unavailable"
            self.sessions = []
            self.version = "Unknown"
            return

def send_cmd(host, port, cmd):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, int(port)))
    result = []
    try:
        s.sendall(cmd)

        # shutting down the socket write side helps ensure
        # that we don't end up with TIME_WAIT sockets
        if not options.fix_330:
            s.shutdown(socket.SHUT_WR)

        while True:
            data = s.recv(4096)
            if not data:
                break
            result.append(data)
    finally:
        s.close()

    return "".join(result)

q_stats = Queue.Queue()

p_wakeup = threading.Condition()

def wakeup_poller():
    p_wakeup.acquire()
    p_wakeup.notifyAll()
    p_wakeup.release()

def reset_server_stats(server):
    host, port = server.split(':')
    send_cmd(host, port, "srst\n")

server_id = 0
class StatPoller(threading.Thread):
    def __init__(self, server):
        self.server = server
        global server_id
        self.server_id = server_id
        server_id += 1
        threading.Thread.__init__(self)

    def run(self):
        p_wakeup.acquire()
        while True:
            s = ZKServer(self.server, self.server_id)
            q_stats.put(s)
            p_wakeup.wait(3.0)
        # no need - never hit here except exit - "p_wakeup.release()"
        # also, causes error on console

class BaseUI(object):
    def __init__(self, win):
        self.win = win
        global mainwin
        self.maxy, self.maxx = mainwin.getmaxyx()
        self.resize(self.maxy, self.maxx)
        
    def resize(self, maxy, maxx):
        LOG.debug("resize called y %d x %d" % (maxy, maxx))
        self.maxy = maxy
        self.maxx = maxx

    def addstr(self, y, x, line, flags = 0):
        LOG.debug("addstr with maxx %d" % (self.maxx))
        self.win.addstr(y, x, line[:self.maxx-1], flags)
        self.win.clrtoeol()
        self.win.noutrefresh()

class SummaryUI(BaseUI):
    def __init__(self, height, width, server_count):
        BaseUI.__init__(self, curses.newwin(1, width, 0, 0))
        self.session_counts = [0 for i in range(server_count)]
        self.node_counts = [0 for i in range(server_count)]
        self.zxids = [0 for i in range(server_count)]

    def update(self, s):
        self.win.erase()
        if s.unavailable:
            self.session_counts[s.server_id] = 0
            self.node_counts[s.server_id] = 0
            self.zxids[s.server_id] = 0
        else:
            self.session_counts[s.server_id] = len(s.sessions)
            self.node_counts[s.server_id] = int(s.node_count)
            self.zxids[s.server_id] = long(s.zxid, 16)
        nc = max(self.node_counts)
        zxid = max(self.zxids)
        sc = sum(self.session_counts)
        self.addstr(0, 0, "Ensemble -- nodecount:%d zxid:0x%x sessions:%d" %
                    (nc, zxid, sc))

class ServerUI(BaseUI):
    def __init__(self, height, width, server_count):
        BaseUI.__init__(self, curses.newwin(server_count + 2, width, 1, 0))

    def resize(self, maxy, maxx):
        BaseUI.resize(self, maxy, maxx)
        self.addstr(1, 0, "ID SERVER           PORT M    OUTST    RECVD     SENT CONNS MINLAT AVGLAT MAXLAT", curses.A_REVERSE)

    def update(self, s):
        if s.unavailable:
            self.addstr(s.server_id + 2, 0, "%-2s %-15s %5s %s" %
                        (s.server_id, s.host[:15], s.port, s.mode[:1].upper()))
        else:
            self.addstr(s.server_id + 2, 0, "%-2s %-15s %5s %s %8s %8s %8s %5d %6s %6s %6s" %
                        (s.server_id, s.host[:15], s.port, s.mode[:1].upper(),
                         s.outstanding, s.received, s.sent, len(s.sessions),
                         s.min_latency, s.avg_latency, s.max_latency))

class SessionUI(BaseUI):
    def __init__(self, height, width, server_count):
        BaseUI.__init__(self, curses.newwin(height - server_count - 3, width, server_count + 3, 0))
        self.sessions = [[] for i in range(server_count)]

    def update(self, s):
        self.win.erase()
        self.addstr(1, 0, "CLIENT           PORT S I   QUEUED    RECVD     SENT", curses.A_REVERSE)
        self.sessions[s.server_id] = s.sessions
        items = []
        for l in self.sessions:
            items.extend(l)
        items.sort(lambda x,y: int(y.queued)-int(x.queued))
        for i, session in enumerate(items):
            try:
                #ugh, need to handle if slow - thread for async resolver?
                if options.names:
                    session.host = socket.getnameinfo((session.host, int(session.port)), 0)[0]
                self.addstr(i + 2, 0, "%-15s %5s %1s %1s %8s %8s %8s" %
                            (session.host[:15], session.port, session.server_id, session.interest_ops,
                             session.queued, session.recved, session.sent))
            except:
                break

mainwin = None
class Main(object):
    def __init__(self, servers):
        self.servers = servers.split(",")

    def show_ui(self, stdscr):
        global mainwin
        mainwin = stdscr
        curses.use_default_colors()
        # w/o this for some reason takes 1 cycle to draw wins
        stdscr.refresh()

        signal.signal(signal.SIGWINCH, sigwinch_handler)

        TIMEOUT = 250
        stdscr.timeout(TIMEOUT)

        server_count = len(self.servers)
        maxy, maxx = stdscr.getmaxyx()
        uis = (SummaryUI(maxy, maxx, server_count),
               ServerUI(maxy, maxx, server_count),
               SessionUI(maxy, maxx, server_count))

        # start the polling threads
        pollers = [StatPoller(server) for server in self.servers]
        for poller in pollers:
            poller.setName("PollerThread:" + server)
            poller.setDaemon(True)
            poller.start()

        LOG.debug("starting main loop")
        global resized_sig
        flash = None
        while True:
            try:
                if resized_sig:
                    resized_sig = False
                    self.resize(uis)
                    wakeup_poller()

                while not q_stats.empty():
                    zkserver = q_stats.get_nowait()
                    for ui in uis:
                        ui.update(zkserver)

                ch = stdscr.getch()
                if 0 < ch <=255:
                    if ch == ord('q'):
                        return
                    elif ch == ord('h'):
                        flash = "Help: q:quit r:reset stats spc:refresh"
                        flash_count = 1000/TIMEOUT * 5
                    elif ch == ord('r'):
                        [reset_server_stats(server) for server in self.servers]
                        flash = "Server stats reset"
                        flash_count = 1000/TIMEOUT * 5
                        wakeup_poller()
                    elif ch == ord(' '):
                        wakeup_poller()

                stdscr.move(1, 0)
                if flash:
                    stdscr.addstr(1, 0, flash)
                    flash_count -= 1
                    if flash_count == 0:
                        flash = None
                stdscr.clrtoeol()

                curses.doupdate()

            except KeyboardInterrupt:
                break

    def resize(self, uis):
        curses.endwin()
        curses.doupdate()

        global mainwin
        mainwin.refresh()
        maxy, maxx = mainwin.getmaxyx()

        for ui in uis:
            ui.resize(maxy, maxx)

def sigwinch_handler(*nada):
    LOG.debug("sigwinch called")
    global resized_sig
    resized_sig = True

if __name__ == '__main__':
    LOG.debug("startup")

    ui = Main(options.servers)
    curses.wrapper(ui.show_ui)
