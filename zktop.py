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
import re
import telnetlib
import StringIO
import threading
import Queue

usage = "usage: %prog [options]"
parser = OptionParser(usage=usage)
parser.add_option("", "--servers", dest="servers",
                  default="localhost:2181", help="comma separated list of host:port (default localhost:2181)")

(options, args) = parser.parse_args()

# threads to get server data
# UI class
# track current data and historical

class Session(object):
    def __init__(self, session):
        m = re.search('/(\d+\.\d+\.\d+\.\d+):(\d+)\[(\d+)\]\((.*)\)', session)
        self.host = m.group(1)
        self.port = m.group(2)
        self.interest_ops = m.group(3)
        for d in m.group(4).split(","):
            k,v = d.split("=")
            self.__dict__[k] = v

class ZKServer(object):
    def __init__(self, server):
        self.host, self.port = server.split(':')
        try:
            stat = self.send_cmd('stat\n')

            sio = StringIO.StringIO(stat)
            line = sio.readline()
            m = re.search('.*: (\d+\.\d+\.\d+)-.*', line)
            self.version = m.group(1)
            sio.readline()
            self.sessions = []
            for line in sio:
                if not line.strip():
                    break
                self.sessions.append(Session(line.strip()))
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

    def send_cmd(self, cmd):
        tn = telnetlib.Telnet(self.host, self.port)

        tn.write(cmd)

        result = tn.read_all()
        tn.close()

        return result


q_stats = Queue.Queue()
p_wakeup = threading.Condition()

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
            s = ZKServer(self.server)
            s.server_id = self.server_id
            q_stats.put(s)
            p_wakeup.wait(3.0)
        p_wakeup.release()

class SummaryUI(object):
    def __init__(self, stdscr, width, server_count):
        self.width = width
        self.win = curses.newwin(1, width, 0, 0)
        self.stdscr = stdscr
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
        self.win.addstr(0, 0, "Ensemble -- nodecount:%d zxid:%x sessions:%d" %
                        (nc, zxid, sc))
        self.win.noutrefresh()

class ServerUI(object):
    def __init__(self, stdscr, width, server_count):
        self.width = width
        self.win = curses.newwin(server_count + 2, width, 1, 0)
        self.win.addstr(1, 0, "SERVER           PORT M      OUTST    RECVD     SENT CONNS MINLAT AVGLAT MAXLAT", curses.A_REVERSE)

    def update(self, s):
        if s.unavailable:
            self.win.addstr(s.server_id + 2, 0, "%-15s %5s %s" %
                            (s.host[:15], s.port, s.mode[:1].upper()))
        else:
            self.win.addstr(s.server_id + 2, 0, "%-15s %5s %s   %8s %8s %8s %5d %6s %6s %6s" %
                            (s.host[:15], s.port, s.mode[:1].upper(),
                             s.outstanding, s.received, s.sent, len(s.sessions),
                             s.min_latency, s.avg_latency, s.max_latency))
        self.win.noutrefresh()

def list_sessions(list_of_session_lists):
    for l in list_of_session_lists:
        for session in l:
            yield session
    return

class SessionUI(object):
    def __init__(self, stdscr, width, server_count):
        self.width = width

        self.win = curses.newwin(20, width, server_count + 3, 0)
        self.sessions = [[] for i in range(server_count)]

    def update(self, s):
        self.win.erase()
        self.win.addstr(1, 0, "CLIENT           PORT I   QUEUE RECVD  SENT", curses.A_REVERSE)
        self.sessions[s.server_id] = s.sessions
        items = []
        for l in self.sessions:
            items.extend(l)
        for i, session in enumerate(items):
            #ugh, handle if slow session.host = socket.getnameinfo((session.host, int(session.port)), 0)[0]
            self.win.addstr(i + 2, 0, "%-15s %5s %1s   %5s %5s %5s" %
                            (session.host[:15], session.port, session.interest_ops,
                             session.queued, session.recved, session.sent))
        self.win.noutrefresh()

class UI(object):
    def __init__(self, servers):
        self.servers = servers.split(",")

    def main(self, stdscr):
        # w/o this for some reason takes 1 cycle to draw wins
        stdscr.refresh()

        self.stdscr = stdscr

        self.max_y, self.max_x = stdscr.getmaxyx()
        stdscr.timeout(250)

        server_count = len(self.servers)
        uis = (SummaryUI(stdscr, self.max_x, server_count),
               ServerUI(stdscr, self.max_x, server_count),
               SessionUI(stdscr, self.max_x, server_count))

        # start the polling threads
        pollers = [StatPoller(server) for server in self.servers]
        for poller in pollers:
            poller.setName("PollerThread:" + server)
            poller.setDaemon(True)
            poller.start()

        while True:
            try:
                while not q_stats.empty():
                    zkserver = q_stats.get_nowait()
                    try:
                        for ui in uis:
                            ui.update(zkserver)
                    finally:
                        q_stats.task_done()

                ch = stdscr.getch()
                if 0 < ch <=255:
                    if ch == ord('q'):
                        return
                    elif ch == ord(' '):
                        p_wakeup.acquire()
                        p_wakeup.notifyAll()
                        p_wakeup.release()

                stdscr.move(1, 0)

                curses.doupdate()

            except KeyboardInterrupt:
                break

if __name__ == '__main__':
    ui = UI(options.servers)
    curses.wrapper(ui.main)
