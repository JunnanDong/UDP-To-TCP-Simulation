import socket
import sys
import threading
import time
import json
import select
from multiprocessing import Semaphore

stop_flag = 0
split_element = ":#:"
#滑动窗口类
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
        self.lock = threading.Lock()    #访问窗口锁
        self.empty_sem = Semaphore(size) #设置信号量为窗口大小
    def slide(self, len):
        size = self.size
        if len >  size:
            return
        for i in range(size - len):
            self[i] = self[i + len]
        for i in range(size - len, size):
            self[i] = None
            self.time_record[i] = 0
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
                    return - 1  #表示收到stop命令
        self.slide(len)
        # print("slide %d" % (len))
        return len  #大于零表示划出的窗口有多少个
        

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
        self.process_q = list()
        self.file_open = None
    def process(self, pkt):
        res = self.server_process(pkt)
        return res
    def client_process(self, pkt:bytes):        
        return 0
    def server_process(self, pkt: bytes):
        seq = pkt.split(split_element.encode("utf-8"))[0].decode("utf-8")
        name = pkt.split(split_element.encode("utf-8"))[2].decode("utf-8")
        if name == "end":
            if self.file_open != None:
                self.file_open.close()
                self.file_open = None
            return 0
        elif name == "stop":
            self.file_open.close()
            self.file_open = None
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
                    print(msg, end="")
                    print(time.asctime(time.localtime(time.time())))
                    send_q.refresh(index)
                    self.recv_f = 1
        self.send_q.lock.release()
    def recv(self, msg):
        recv_start_seq = self.recv_q.seq
        dst_seq, dst_ack = self._decode_msg(msg)
        recv_index = dst_seq - recv_start_seq
        #If recv_index > window_size, abandon
        if (recv_index > self.win_size):
            return 0
        # If recv_index<0 the packet
        if (recv_index >= 0):
            self.recv_q.lock.acquire()
            self.recv_q[recv_index] = msg
            res = self.recv_q.recv_window_slide(self.process_q.append)
            self.ack = self.recv_q.seq
            self.recv_q.lock.release()
            if res == -1:
                return -1
        if (dst_ack > self.send_q.seq): 
            slide_size = dst_ack - self.send_q.seq
            self.send_q.lock.acquire()
            self.send_q.slide(slide_size)
            self.send_q.lock.release()
            for i in range(slide_size):
                self.send_q.empty_sem.release() #Relase Semaphore, release the window
        return 0
    def _encode_msg(self, seq, msg:bytes):
        head = str(seq) + split_element + str(self.ack) + split_element
        msg = head.encode("utf-8") + msg
        return msg
    def _decode_msg(self, msg : bytes):
        seq = int(msg.split(split_element.encode("utf-8"))[0])
        ack = int(msg.split(split_element.encode("utf-8"))[1])
        return seq, ack
    def add_to_send_queue(self, msg: bytes):  #发包队列统一接口
        self.send_q.empty_sem.acquire()  #请求一个信号量, 占用一个空的窗口位
        self.send_q.lock.acquire()
        self.send_q.add_to_queue(self.seq, msg)
        self.send_q.lock.release()
        self.seq += 1
        return
    def send_file_block(self, file_block: bytes, file_name): #发送文件数据块
        msg = (file_name + split_element).encode("utf-8") + file_block
        self.add_to_send_queue(msg)
    def send_str(self, data):
        msg = data.encode("utf-8")
        self.add_to_send_queue(msg)

class connect_list(list):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    def find(self, addr):
        for connect in self:
            if connect.addr == addr:
                return connect
    def if_new_connect(self, addr):
        for connect in self:
            if connect.addr == addr:
                return 0
        return 1

# 线程->数据库->前端     

def send_thread(connect_l: connect_list):
    print("send_thread start\n", end="")
    while 1:
        for connect in connect_l:
            connect.resend()
        if (stop_flag == 1):
            break
    print("send_thread end")
    return

def recv_thread(ser_sock, connect_l: connect_list):
    print("recv_thread start\n", end="")
    while 1:  
        readable, writable, exceptional = select.select([ser_sock], [], [ser_sock], 1)
        for fd in readable:
            if fd == ser_sock:
                try:
                    msg, addr = ser.recvfrom(maxsize)
                except ConnectionResetError:
                    print("diconnect from client")
                    connect = connect_l.find(addr)
                    connect_l.remove(connect)
                    continue
                if connect_l.if_new_connect(addr):
                    print("connect to", end="")
                    print(addr)
                    connect = connect_info(ser_sock, addr)
                    connect_l.append(connect)
                connect = connect_l.find(addr)
                res = connect.recv(msg)
                if res == -1:
                    print("disconnect from", end="")
                    print(connect.addr)
                    connect_l.remove(connect)
        if (stop_flag == 1):
            break
    print("recv_thread end")
    return

def process_thread(connect_l: connect_list):
    print("process_thread start\n", end="")
    connect: connect_info
    while 1:
        if (stop_flag == 1):
            break
        for connect in connect_l:
            if connect.process_q.__len__() > 0:
                pkt = connect.process_q.pop(0)
                connect.process(pkt)
    return

if __name__ == "__main__":
    ip_portno = ('127.0.0.1',8080)
    maxclient = 30
    maxsize = 1024  #sys.maxsize
    # Create a server
    ser = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ser.bind(ip_portno)
    connect_l = connect_list()
    t1 = threading.Thread(target=send_thread, args=(connect_l,))
    t2 = threading.Thread(target=recv_thread, args=(ser, connect_l))
    t3 = threading.Thread(target=process_thread, args=(connect_l,))
    print("server start\n", end="")
    t1.start()
    t2.start()
    t3.start()
    while 1:
        cmd = input("ser:")
        if cmd == "stop":
            stop_flag = 1
            time.sleep(1)
            break
    t1.join()
    t2.join()
    t3.join()
    ser.close()