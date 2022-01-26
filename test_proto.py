import sys
import threading

import msg_pb2
import transaction
import sched, time

message = msg_pb2.Msg()
message.type = msg_pb2.UNORDERED
message.priority = 1
message.node_id = 2
trans = message.transaction
trans.type = msg_pb2.DEPOSIT
trans.from_whom = "Alias"
trans.to_whom = "Bob"
trans.amount = 10

# test msg
if __name__ == '__main__':
    # # serialize
    serializeToString = message.SerializeToString()
    print(serializeToString, type(serializeToString))
    print(sys.getsizeof(serializeToString))
    # Deserialize
    message_ori = msg_pb2.Msg()
    message_ori.ParseFromString(serializeToString)
    trans_test = transaction.Transaction(message_ori)
    trans_test.deliverable = True

    print(hash(trans_test) == hash((1, 2)))
    a = set()
    a.add(serializeToString)
    serializeToString1 = message.SerializeToString()
    print(serializeToString1 in a)
    #
    # s = sched.scheduler(time.time, time.sleep)
    #
    #
    # def do_something(sc):
    #     print("Doing stuff...")
    #     # do your stuff
    #     s.enter(2, 1, do_something, (sc,))
    #
    #
    # s.enter(2, 1, do_something, (s,))
    # s.run()
    # print("after run")

    # def cnt_bandwidth():
    #     while True:
    #         print("yes")
    #         time.sleep(1)
    # band_log = threading.Thread(target=cnt_bandwidth)
    # band_log.setDaemon(True)
    # band_log.start()
    # n = 1
    # while True:
    #     print(time.time())


