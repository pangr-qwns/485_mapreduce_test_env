import socket

"""
A socket that receives udp connections on localhost port 7999
"""


sock = socket.socket(  # create socket
    socket.AF_INET,  # IPv4
    socket.SOCK_DGRAM,  # UDP
)

sock.bind(("localhost", 7999))  # bind socket to port_num - 1

while True:
    data, addr = sock.recvfrom(1024)
    print("Data received: ")
    print(data, addr)
