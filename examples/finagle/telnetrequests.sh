(echo -en "GET /table/0x1?key=apple HTTP/1.1\n\nGET /table/0x1?key=apple HTTP/1.1\n\nGET /table/0x1?key=apple HTTP/1.1\n\nGET /table/0x1?key=apple HTTP/1.1\n\n"; sleep 60) | telnet localhost 7070
