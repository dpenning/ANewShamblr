import socket
import json
import queue
import threading
import random
import sys
import os

hash_table = set()
if os.path.isfile('SET_SAVE.data'):
	for a in open('SET_SAVE.data','r').readlines():
		hash_table.add(a.strip('\n'))

frontier_queue = queue.Queue()
if os.path.isfile('QUEUE_SAVE.data'):
	for a in open('QUEUE_SAVE.data','r').readlines():
		frontier_queue.put(a.strip('\n'))

api_key_queue = queue.Queue()
if os.path.isfile('API_KEY_LIST.data'):
	for a in open('API_KEY_LIST.data','r').readlines():
		api_key_queue.put(a.strip('\n'))


host = ""
port = 8888

def worker(conn):
	try:
		data = bytes([])
		while True:
			new_data = conn.recv(1024)
			if not new_data: break
			data += new_data
			if '\n' in str(new_data,'UTF-8'): break
		data = str(data,'UTF-8')
		print (data)
		json_data = json.loads(data)
		if "request_type" in json_data:
			if json_data["request_type"] == "queue_blogs":
				if "blog_list" in json_data:
					for a in json_data["blog_list"]:
						if a not in hash_table:
							hash_table.add(a)
							frontier_queue.put(a)
			elif json_data["request_type"] == "new_blog_request":
				try:
					new_blog = str(frontier_queue.get(timeout=.25))
					send_data = {	"worked":True,
									"request_type":"new_blog",
									"new_blog":new_blog,
								}
				except Exception as e:
					send_data = {	"worked":False,
									"request_type":"new_blog",
								}
					pass
				conn.send(str.encode(json.dumps(send_data)))
				conn.shutdown(socket.SHUT_WR)
			elif json_data["request_type"] == "status_report":	

				current_hash_table_size = len(hash_table)
				current_queue_size = int(frontier_queue.qsize())
				send_data = {	"worked":True,
								"request_type":"status_report",
								"hash_table_size":current_hash_table_size,
								"queue_size":current_queue_size,
								}
				conn.send(str.encode(json.dumps(send_data)))
				conn.shutdown(socket.SHUT_WR)
			elif json_data["request_type"] == "api_key_get":
				try:
					new_api_key = str(api_key_queue.get(timeout=.25))
					send_data = {	"worked":True,
									"request_type":"api_key_get",
									"new_api_key":new_api_key,
								}
					api_key_queue.put(new_api_key)
				except Exception as e:
					send_data = {	"worked":False,
									"request_type":"api_key_get",
								}
					pass
				conn.send(str.encode(json.dumps(send_data)))
				conn.shutdown(socket.SHUT_WR)
			else:
				send_data = {"worked":False,"request_type":"NOT RECOGNIZED",}
				conn.send(str.encode(json.dumps(send_data)))
				conn.shutdown(socket.SHUT_WR)
	finally:
		conn.close()

if __name__ == "__main__":
	try:
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		try:
			s.bind((host, port))
		except Exception as e:
			sys.exit()
		s.listen(10)
		
		while True:
			conn,addr = s.accept()
			print ("WOW")
			t = threading.Thread(target=worker, args = (conn,))
			t.start()
	except Exception as e:
		"do nothing"
	finally:
		print ("closing_socket")
		s.close()

