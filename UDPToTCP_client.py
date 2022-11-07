import socket
import sys
import threading
import json
import time
import select
from multiprocessing import Semaphore

stop_flag = 0
split_element = ":#:"
#slide window class
class packet_window(list):
    size: int
    time_record: list
    delay_time = 0.1
    def __init__(self, size:int):
        super().__init__()
        self.time_record = []
        for i in range(size):
            super().append(None)
            self.time_record.append(0)
        self.size = size
        self.seq = 0
        self.lock = threading.Lock()    #visit window lock
        self.empty_sem = Semaphore(size) #set window size
    def slide(self, len):
        size = self.size
        if len >  size:
            return
        for i in range(size - len):
            self[i] = self[i + len]
        for i in range(size - len, size):
            self[i] = None
        self.seq += len
    def add_to_queue(self, seq, msg):
        if (seq < self.seq or seq > self.seq + self.size):
            print("error")
            return
        index = seq - self.seq
        self[index] = msg
        self.time_record[index] = time.time() - self.delay_time
    def refresh(self, index):
        self.time_record[index] = time.time()
    def recv_window_slide(self, func):
        len = 0
        for pkt in self:
            if pkt != None:
                len += 1
            else:
                break
        for index in range(len):
            result = func(self[index])
            if result is not None:
                if result == -1:
                    return -1  #recieve stop
        self.slide(len)
        return len             #if larger than 0, indicating the number of those sliding out ofthe window

# connect_info class
class connect_info:
    heat_time = 10
    sock: socket.socket
    def __init__(self, sock, addr, win_size = 1000):
        self.sock = sock
        self.addr = addr
        self.seq = 0
        self.ack = 0
        self.win_size = win_size
        self.time_send = time.time()
        self.time_recv = time.time()
        self.recv_f = 0
        self.send_q = packet_window(win_size)
        self.recv_q = packet_window(win_size)
        self.file_open = None
    def process(self, pkt):
        res = self.client_process(pkt)
        return res
    def client_process(self, pkt:bytes):        
        return 0
    def server_process(self, pkt: bytes):
        name = pkt.split(split_element.encode("utf-8"))[2]
        if name == "end":
            if self.file_open != None:
                self.file_open.close()
            return 0
        elif name == "stop":
            self.file_open.close()
            return -1
        else:
            if self.file_open is None:
                self.file_open = open(name, "wb")
        data = pkt.split(split_element.encode("utf-8"), 4)[3]
        self.file_open.write(data)
        self.add_to_send_queue("ok".encode())
        return 0
    def resend(self):
        send_q = self.send_q
        self.send_q.lock.acquire()
        for msg in send_q:
            if (msg != None):
                index = send_q.index(msg)
                time_now = time.time()
                time_record = send_q.time_record[index]
                if time_now - time_record >= send_q.delay_time:
                    msg = self._encode_msg(index + send_q.seq, msg)
                    self.sock.sendto(msg, self.addr)
                    print("send:", end="")
                    print("seq:" + str(msg.split(split_element.encode("utf-8"))[0]), end="")
                    print("ack:" + str(msg.split(split_element.encode("utf-8"))[1]), end="")
                    print(time.asctime(time.localtime(time.time())))
                    send_q.refresh(index)
                    self.recv_f = 1
                    time.sleep(0.001)
        self.send_q.lock.release()
    def recv(self, msg):
        recv_start_seq = self.recv_q.seq
        dst_seq, dst_ack = self._decode_msg(msg)
        recv_index = dst_seq - recv_start_seq
        #if recv_index > window_size, than abandon
        if (recv_index > self.win_size):
            return 0
        # if recv_index<0, indicating the resent packages
        if (recv_index >= 0):
            self.recv_q.lock.acquire()
            self.recv_q[recv_index] = msg
            res = self.recv_q.recv_window_slide(self.process)
            self.ack = self.recv_q.seq
            self.recv_q.lock.release()
            if res == -1:
                return -1
        if (dst_ack > self.send_q.seq): 
            slide_size = dst_ack - self.send_q.seq
            self.send_q.lock.acquire()
            self.send_q.slide(slide_size)
            print("size" + str(slide_size))
            self.send_q.lock.release()
            for i in range(slide_size):
                self.send_q.empty_sem.release() #release new windows
        return 0
    def _encode_msg(self, seq, msg:bytes):
        head = str(seq) + split_element + str(self.ack) + split_element
        msg = head.encode("utf-8") + msg
        return msg
    def _decode_msg(self, msg : bytes):
        seq = int(msg.split(split_element.encode("utf-8"))[0])
        ack = int(msg.split(split_element.encode("utf-8"))[1])
        return seq, ack
    def add_to_send_queue(self, msg: bytes):  #
        self.send_q.empty_sem.acquire()  
        self.send_q.lock.acquire()
        self.send_q.add_to_queue(self.seq, msg)
        self.send_q.lock.release()
        self.seq += 1
        return
    def send_file_block(self, file_block: bytes, file_name):
        msg = (file_name + split_element).encode("utf-8") + file_block
        self.add_to_send_queue(msg)
    def send_str(self, data):
        msg = data.encode("utf-8")
        self.add_to_send_queue(msg)

def send_thread(connect:connect_info):
    print("send_thread start\n", end="")
    while 1:
        connect.resend()
        if (stop_flag == 1):
            break
    return

def recv_thread(connect:connect_info):
    maxsize = 1024
    client = connect.sock
    while connect.recv_f == 0:
        if (stop_flag == 1):
            return
        pass
    print("recv_thread start\n", end="")
    print(client)
    while 1:
        readable, writable, exceptional = select.select([client], [], [client], 1)
        for c in readable:
            if c == client:   
                try:
                    msg = client.recv(maxsize)
                except ConnectionResetError:
                    print("diconnect from server")
                    globals()["stop_flag"] = 1
                    return
                connect.recv(msg)
        
        if (stop_flag == 1):
                break

if __name__ == "__main__":
    ip_portno = ('127.0.0.1', 8080)

    client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    print(client)
    connect = connect_info(client, ip_portno)
    t1 = threading.Thread(target=send_thread, args=(connect,))
    t2 = threading.Thread(target=recv_thread, args=(connect,))
    t1.start()
    t2.start()
    while 1:
        file_name = input('file to send:\n')
        if file_name == "stop":
            connect.send_str("stop")
            stop_flag = 1
            time.sleep(1)
            break

        if file_name == "":
            continue
        try:
            f = open(file_name, "rb")
        except:
            print("open file %s fail" % file_name)
            continue
        f.seek(0, 2)
        file_size = f.tell()
        f.seek(0, 0)
        while file_size > 0:
            if file_size < 1000:
                read_len = file_size
            else:
                read_len = 1000
            file_block = f.read(read_len)  #max len 1500
            connect.send_file_block(file_block, file_name)
            file_size -= read_len
        connect.send_str("stop")
        #connect.send_str("end")
    t1.join()
    t2.join()
    '''Close client'''
    client.close()
