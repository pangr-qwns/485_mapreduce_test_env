import socket

sock = socket.socket(
    socket.AF_INET, # Internet
    socket.SOCK_DGRAM
) # UDP

sock.sendto("Hello world".encode('utf-8'), ("localhost", 7999))

