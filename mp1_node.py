#!/usr/bin/env python3
import os.path
import socket
import sys
import threading
import heapq
from time import sleep
import time
import msg_pb2
import multicast
import transaction
import collections


class globals:
    def __init__(self, my_id):
        self.balance = collections.defaultdict(int)  # {name, balance}
        self.holdback = {}  # {(ori_priority, ori_node_id), transaction}: unordered transactions
        self.deliver_q = []  # trans waiting to be delivered; can be ordered or ordering
        self.sent = {}  # key are msgs sent by this node in ordering; values are set of nodes that have replied
        self.group = []  # all nodes' addr
        self.conn_node = {}  # connection-node pairs
        self.node_conn = {}  # node-connection pairs
        self.next_propose = 1  # next proposed priority
        self.my_id = my_id  # self node id
        self.r_multi = None
        self.connected = 0
        self.logger = open(str(my_id) + ".txt", "w")
        self.lock = threading.Lock()
        self.ready = False
        self.sec = 0
        self.pre_amount = 0
        self.recv_amount = 0
        self.err_node={}
        self.ignoreSet=set()
        self.start_time = time.time()

    def updatePriority(self, seenPriority=None):
        res = 0
        self.lock.acquire()
        if not seenPriority:
            res = self.next_propose
            self.next_propose += 1
        else:
            self.next_propose = max(self.next_propose, seenPriority + 1)
        self.lock.release()
        return res


def node_server(port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    srv_addr = ("0.0.0.0", port)
    sock.bind(srv_addr)
    sock.listen(5)
    while True:
        conn, addr = sock.accept()
        t = threading.Thread(target=server_thread, args=(conn, addr))
        t.start()
        glos.connected += 1


def server_thread(sock, addr):
    try:
        while True:
            if glos.ready:
                hd = sock.recv(2).decode()
                if hd:
                    size = int(hd)
                    msg = sock.recv(size)
                    if msg:
                        glos.recv_amount =+ 1
                        glos.r_multi.r_multicast_deliver(msg)
                else:
                    break
    except Exception as e:  # deal with node fail
        pass
    finally:
        sock.close()


def reply(msg):
    msg.priority = glos.updatePriority()
    msg.type = 1
    msg.node_id = glos.my_id
    s = msg.SerializeToString()
    try:
        if msg.trans_id.ori_node_id in glos.node_conn:
            conn = glos.node_conn[msg.trans_id.ori_node_id]
            conn.send(str(len(s)).encode() + s)
    except socket.error as e:    # set node as fail and delete it's trans after 10s             #faliure
        handleFail([conn])


def printBalance():
    # glos.logger.write(' '.join('{}: {}'.format(a, b) for a, b in sorted(glos.balance.items())))
    print('BALANCES',end=' ')
    print(' '.join('{}:{}'.format(a,b) for a,b in sorted(glos.balance.items())))


def deliverTrans():
    heapq.heapify(glos.deliver_q)
    while len(glos.deliver_q) > 0 and (glos.deliver_q[0].deliverable or glos.deliver_q[0].id[1] in glos.ignoreSet):
        tran = heapq.heappop(glos.deliver_q)
        if tran.id[1] in glos.ignoreSet and not tran.deliverable:
            pass
        else:
            if tran.trans_info.type == 0:
                glos.balance[tran.trans_info.to_whom] += tran.trans_info.amount
            elif tran.trans_info.from_whom in glos.balance and glos.balance[
                tran.trans_info.from_whom] >= tran.trans_info.amount:
                glos.balance[tran.trans_info.from_whom] -= tran.trans_info.amount
                glos.balance[tran.trans_info.to_whom] += tran.trans_info.amount
            printBalance()
            # msg_delay(tran)


def sendFinalPriority(ori_msg_id):
    msg = msg_pb2.Msg()
    msg.trans_id.ori_priority, msg.trans_id.ori_node_id = ori_msg_id
    msg.type = 2
    msg.priority = glos.holdback[ori_msg_id].priority
    msg.node_id = glos.holdback[ori_msg_id].node_id
    s = msg.SerializeToString()
    err_conn=glos.r_multi.send(str(len(s)).encode() + s)
    handleFail(err_conn)



def deliver(msg):
    if msg.type == 0:  # a unordered transaction
        msg_id = (msg.trans_id.ori_priority, msg.trans_id.ori_node_id)
        trans = transaction.Transaction(msg)
        glos.holdback[msg_id] = trans
        heapq.heappush(glos.deliver_q, trans)
        reply(msg)
    elif msg.type == 1:  # a propose seq# message
        if msg.trans_id.ori_node_id == glos.my_id:
            ori_msg_id = (msg.trans_id.ori_priority, msg.trans_id.ori_node_id)
            if ori_msg_id in glos.sent.keys():
                glos.holdback[ori_msg_id].set_priority(msg.priority, msg.node_id)
                glos.updatePriority(msg.priority)
                glos.sent[ori_msg_id].add(msg.node_id)
                if not glos.node_conn.keys() - glos.sent[ori_msg_id]:  # means all nodes has replied
                    glos.r_multi.messages.add((2, msg.trans_id.ori_priority, msg.trans_id.ori_node_id))
                    sendFinalPriority(ori_msg_id)  # multicast the final priority
                    glos.holdback[ori_msg_id].deliverable = True
                    del glos.sent[ori_msg_id]
                    del glos.holdback[ori_msg_id]
                    deliverTrans()  # try to deliver trans in the priority queue
    else:  # a final seq# message
        ori_msg_id = (msg.trans_id.ori_priority, msg.trans_id.ori_node_id)
        glos.holdback[ori_msg_id].set_priority(msg.priority, msg.node_id)
        glos.updatePriority(msg.priority)
        glos.holdback[ori_msg_id].deliverable = True
        del glos.holdback[ori_msg_id]
        deliverTrans()


def groupSetup(filename):
    with open(filename) as f:
        n = int(f.readline().rstrip())
        for _ in range(n):
            node = f.readline().split()
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            while s.connect_ex((node[1], int(node[2]))) != 0:
                sleep(2)
            glos.group.append(s)
            glos.conn_node[s] = int(node[0][4:])
            glos.node_conn[int(node[0][4:])] = s
    glos.ready = True


def sendMsg(msg):
    ori_msg_id = (msg.trans_id.ori_priority, msg.trans_id.ori_node_id)
    if msg.trans_id.ori_priority > 0:
        glos.sent[ori_msg_id] = set()
        tran = transaction.Transaction(msg)
        glos.holdback[ori_msg_id] = tran
        heapq.heappush(glos.deliver_q, tran)
        s = msg.SerializeToString()
        glos.r_multi.messages.add((0, msg.trans_id.ori_priority, msg.trans_id.ori_node_id))
        err_conn=glos.r_multi.send(str(len(s)).encode() + s)
        if err_conn:
            handleFail(err_conn)

def readTrans():
    while True:
        line = sys.stdin.readline()
        if line:
            msg = msg_pb2.Msg()
            msg.type = 0
            msg.priority = glos.updatePriority()
            msg.node_id = glos.my_id
            msg.trans_id.ori_priority = msg.priority
            msg.trans_id.ori_node_id = msg.node_id
            tran = line.split()
            if len(tran) == 3:
                msg.transaction.type = 0
                msg.transaction.to_whom = tran[1]
                msg.transaction.amount = int(tran[2])
            else:
                msg.transaction.type = 1
                msg.transaction.from_whom = tran[1]
                msg.transaction.to_whom = tran[3]
                msg.transaction.amount = int(tran[4])
            sendMsg(msg)


def handleFail(err_conn):
    for conn in err_conn:
        if conn in glos.conn_node:
            node=glos.conn_node[conn]
            glos.group.remove(conn)
            del glos.conn_node[conn]
            del glos.node_conn[node]
            failureHandler=threading.Thread(target=handler,args=(time.time(),node,))
            failureHandler.start()

def handler(t,node):
    while time.time()-t<10:
        continue
    glos.ignoreSet.add(node)
    values = glos.sent.keys()
    for msg in values:
        if msg in glos.sent.keys():
            fkmsg = msg_pb2.Msg()
            fkmsg.type = 1
            fkmsg.trans_id.ori_priority = msg[0]
            fkmsg.trans_id.ori_node_id = msg[1]
            fkmsg.node_id=node
            deliver(fkmsg)



def cnt_bandwidth():
    while True:
        sum = glos.r_multi.b_multicast.send_amount + glos.recv_amount
        bd = sum - glos.pre_amount
        glos.pre_amount = sum
        f = open(str(glos.my_id) + "_bandwidth.csv", "a")
        f.write('%d,%d\n' % (glos.sec, bd))
        f.close()
        print(glos.sec)
        glos.sec += 1
        time.sleep(1)
        if glos.sec > 200:
            break

def msg_delay(trans):
    gap = int(round((time.time() - glos.start_time) * 1000))
    if (gap <= 200000):
        id = str(trans.id[0]) + str(trans.id[1])
        f = open(str(glos.my_id) + "_msgDelay.csv", "a")
        f.write('%s,%d\n' % (id, gap))
        f.close()



if __name__ == "__main__":
    glos = globals(int(sys.argv[1][4:]))
    glos.r_multi = multicast.R_multicast(glos.group, glos.my_id, deliver)
    server = threading.Thread(target=node_server, args=(int(sys.argv[2]),))
    server.start()
    groupSetup(sys.argv[3])
    while len(glos.group) != glos.connected:
        sleep(1)
    # if os.path.exists(str(glos.my_id) + "_bandwidth.csv"):
    #     os.remove(str(glos.my_id) + "_bandwidth.csv")
    # band_log = threading.Thread(target=cnt_bandwidth)
    # band_log.setDaemon(True)
    # band_log.start()
    # if os.path.exists(str(glos.my_id) + "_msgDelay.csv"):
    #     os.remove(str(glos.my_id) + "_msgDelay.csv")
    glos.start_time = time.time()
    readTrans()
