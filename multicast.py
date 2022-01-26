import socket
import msg_pb2

class SockError(RuntimeError):
    def __init__(self, arg):
        self.sock = arg

class unicast:
    @classmethod
    def send(cls, message, member_sock):  # send the message to the specified member (use TCP
        member_sock.sendall(message)

    @classmethod
    def register_deliver(cls, b_multicast_deliver):
        cls.c_multicast_deliver = b_multicast_deliver

    @classmethod
    def deliver(cls, message):
        cls.b_multicast_deliver(message)


class B_multicast:
    def __init__(self, group, my_id, r_multicast_deliver):
        self.group = group
        self.id = my_id
        self.deliver = r_multicast_deliver
        unicast.register_deliver(self.b_multicast_deliver)
        self.send_amount = 0

    def send(self, message):
        err_conn=[]
        for member in self.group:
            try:
                unicast.send(message, member)
                self.send_amount += 1
            except socket.error:
                err_conn.append(member)
        return err_conn
    def b_multicast_deliver(self, message):
        self.deliver(message)


class R_multicast:
    def __init__(self, group, my_id, app_deliver):
        self.group = group
        self.id = my_id
        self.deliver = app_deliver  # this is application's deliver
        self.b_multicast = B_multicast(group, my_id, self.r_multicast_deliver)
        self.messages = set()

    def send(self, message):
        return self.b_multicast.send(message)

    def r_multicast_deliver(self, message):
        #donnot need to check sender==self because no such messages
        msg = msg_pb2.Msg()
        msg.ParseFromString(message)
        msg_id = (msg.type,msg.trans_id.ori_priority, msg.trans_id.ori_node_id)
        if msg.type==1:
            self.deliver(msg)
        elif msg_id not in self.messages:
            self.messages.add(msg_id)
            self.send(str(len(message)).encode()+message)
            self.deliver(msg)


if __name__ == '__main__':
    def deliver(msg):
        print(msg)


    uni = unicast
    r = R_multicast([0, 1], 0, deliver,[])
    r.send("msg")
