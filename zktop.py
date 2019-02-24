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

from optparse import OptionParser # TODO use argparse instead

import threading
import sys

IS_PYTHON2 = sys.version_info[0] == 2

if IS_PYTHON2:
    import Queue
    from StringIO import StringIO
    strToLong = lambda str, base = 10: long(str, base)
else: # Python 3
    import queue as Queue
    from io import StringIO
    strToLong = lambda str, base = 10: int(str, base)
import socket
import signal
import re
import logging as LOG
if sys.platform == 'win32':
  print("Cannot run natively on Windows due to missing curses package. Try running in a cygwin shell instead!"); sys.exit(1)
import curses
    

ZK_DEFAULT_PORT = 2181

usage = "usage: %prog [options]"
parser = OptionParser(usage=usage)
parser.add_option("", "--servers",
                  dest="servers", default="localhost:%s" % ZK_DEFAULT_PORT,
                  help="comma separated list of host:port (default localhost:%d)" % ZK_DEFAULT_PORT)
parser.add_option("-n", "--names",
                  action="store_true", dest="names", default=False,
                  help="resolve session name from ip (default False)")
parser.add_option("", "--fix_330",
                  action="store_true", dest="fix_330", default=False,
                  help="workaround for a bug in ZK 3.3.0")
parser.add_option("-w", "--wide",
                  action="store_true", dest="wide", default=False,
                  help="allow output to expand beyond 80 cols")
parser.add_option("-v", "--verbosity",
                  dest="verbosity", default="DEBUG",
                  help="log level verbosity (DEBUG, INFO, WARN(ING), ERROR, CRITICAL/FATAL)")
parser.add_option("-l", "--logfile",
                  dest="logfile", default=None,
                  help="directory in which to place log file, or empty for none")
parser.add_option("-c", "--config",
                  dest="configfile", default=None,
                  help="zookeeper configuration file to lookup servers from")
parser.add_option("-t", "--timeout",
                  dest="timeout", default=None,
                  help="connection timeout to zookeeper instance")

(options, args) = parser.parse_args()

if options.logfile:
    LOG.basicConfig(filename=options.logfile, level=getattr(LOG, options.verbosity))
else:
    LOG.disable(LOG.CRITICAL)

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
            setattr(self, k, v)

class ZKServer(object):
    def __init__(self, server, server_id):
        self.server_id = server_id
        if ':' in server:
            self.host, self.port = server.split(':')[0], int(server.split(':')[1])
        else: # fallback to default if user doesn't specify port number
            self.host, self.port = server, ZK_DEFAULT_PORT
        try:
            stat = send_cmd(self.host, self.port, b'stat\n')

            sio = StringIO(stat)
            line = sio.readline()
            m = re.search('.*: (\d+\.\d+\.\d+)-.*', line) # e.g. nodecount:0 zxid:0x0 sessions:0o att
            self.version = m.group(1) # raise Exception when stat response empty
            sio.readline()
            self.sessions = []
            for line in sio:
                if not line.strip():
                    break
                self.sessions.append(Session(line.strip(), server_id))
            for line in sio:
                attr, value = line.split(':')
                attr = attr.strip().replace(" ", "_").replace("/", "_").lower()
                setattr(self, attr, value.strip())

            self.min_latency, self.avg_latency, self.max_latency = self.latency_min_avg_max.split("/")

            self.unavailable = False
        except: # e.g., when server responds with '' (not reachable)
            self.unavailable = True
            self.mode = "Unavailable"
            self.sessions = []
            self.version = "Unknown"
            return

def send_cmd(host, port, cmd):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if options.timeout:
        s.settimeout(float(options.timeout))
    s.connect((host, port))
    result = []
    try:
        s.sendall(cmd)

        # shutting down the socket write side helps ensure that we don't end up with TIME_WAIT sockets
        if not options.fix_330:
            s.shutdown(socket.SHUT_WR)

        while True:
            data = s.recv(4096)
            if not data:
                break
            data = data.decode()
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
    send_cmd(host, port, b'srst\n')

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
            self.zxids[s.server_id] = strToLong(s.zxid, 16)
        nc = max(self.node_counts)
        zxid = max(self.zxids)
        sc = sum(self.session_counts)
        self.addstr(0, 0, "Ensemble -- nodecount:%d zxid:0x%x sessions:%d" %
                    (nc, zxid, sc))

class ServerUI(BaseUI):
    def __init__(self, height, width, server_count, wide=False):
        self.wide = wide
        BaseUI.__init__(self, curses.newwin(server_count + 2, width, 1, 0))

    def resize(self, maxy, maxx):
        BaseUI.resize(self, maxy, maxx)
        if self.wide:
            self.addstr(1, 0, "ID SERVER                          PORT M    OUTST    RECVD     SENT CONNS MINLAT AVGLAT MAXLAT", curses.A_REVERSE)
        else:
            self.addstr(1, 0, "ID SERVER           PORT M    OUTST    RECVD     SENT CONNS MINLAT AVGLAT MAXLAT", curses.A_REVERSE)

    def update(self, s):
        if self.wide:
            host_width = 30
            unavailable_format = "%-2s %-30s %5s %s"
            available_format = "%-2s %-30s %5s %s %8s %8s %8s %5d %6s %6s %6s"
        else:
            host_width = 15
            unavailable_format = "%-2s %-15s %5s %s"
            available_format = "%-2s %-15s %5s %s %8s %8s %8s %5d %6s %6s %6s"
        if s.unavailable:
            self.addstr(s.server_id + 2, 0, unavailable_format %
                        (s.server_id, s.host[:host_width], s.port, s.mode[:1].upper()))
        else:
            self.addstr(s.server_id + 2, 0, available_format %
                        (s.server_id, s.host[:host_width], s.port, s.mode[:1].upper(),
                         s.outstanding, s.received, s.sent, len(s.sessions),
                         s.min_latency, s.avg_latency, s.max_latency))

class SessionUI(BaseUI):
    def __init__(self, height, width, server_count):
        BaseUI.__init__(self, curses.newwin(height - server_count - 3, width, server_count + 3, 0))
        self.sessions = [[] for i in range(server_count)]
        self.ip_to_hostname = {}

    def hostname(self, session):
        if session.host not in self.ip_to_hostname:
            hostname = socket.getnameinfo((session.host, int(session.port)), 0)[0]
            self.ip_to_hostname[session.host] = hostname
        return self.ip_to_hostname[session.host]

    def update(self, s):
        self.win.erase()
        self.addstr(1, 0, "CLIENT                          PORT S I   QUEUED    RECVD     SENT", curses.A_REVERSE)
        self.sessions[s.server_id] = s.sessions
        items = []
        for l in self.sessions:
            items.extend(l)
        items.sort(key=lambda x: int(x.queued), reverse=True)
        for i, session in enumerate(items):
            try:
                #ugh, need to handle if slow - thread for async resolver?
                host = self.hostname(session) if options.names else session.host
                self.addstr(i + 2, 0, "%-30s %5s %1s %1s %8s %8s %8s" %
                            (host[:30], session.port, session.server_id, session.interest_ops,
                             session.queued, session.recved, session.sent))
            except:
                break

mainwin = None
class Main(object):
    def __init__(self, servers, wide_ui=False):
        self.wide_ui = wide_ui
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
               ServerUI(maxy, maxx, server_count, wide=self.wide_ui),
               SessionUI(maxy, maxx, server_count))

        # start the polling threads
        pollers = [StatPoller(server) for server in self.servers]
        for poller in pollers:
            poller.setName("PollerThread:" + poller.server)
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
                if 0 < ch <= 255:
                    if ch == ord('q'):
                        return
                    elif ch == ord('h'):
                        flash = "Help: q:quit r:reset stats spc:refresh"
                        flash_count = 1000/TIMEOUT * 5
                    elif ch == ord('r'):
                        for server in self.servers:
                            try:
                                reset_server_stats(server)
                            except:
                              pass

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

def read_zk_config(filename):
    config = {}
    f = open(filename, 'r')
    try:
        for line in f:
            if line.rstrip() and not line.startswith('#'):
                k, v = tuple(line.replace(' ', '').strip().split('=', 1))
                config[k] = v
    except IOError as e:
        print("Unable to open `{0}': I/O error({1}): {2}".format(filename, e.errno, e.strerror))
    finally:
        f.close()
        return config

def get_zk_servers(filename):
    if filename:
        config = read_zk_config(options.configfile)
        client_port = config['clientPort']
        return ','.join("%s:%s" % (v.split(':', 1)[0], client_port) for k, v in config.items() if k.startswith('server.'))
    else:
        return ','.join("%s:%s" % (s.strip(), ZK_DEFAULT_PORT) if not ':' in s else "%s" % s for s in options.servers.split(',', 1))

def main_func():
    LOG.debug("startup")
    ui = Main(get_zk_servers(options.configfile), wide_ui=options.wide)
    curses.wrapper(ui.show_ui)
    
if __name__ == '__main__':
    main_func()
