import psycopg2
import socket
import json
import random
import threading
import sys
import os
import time

host = ""
port = 6667

def worker(conn):
	try:
		data = bytes([])
		while True:
			new_data = conn.recv(1024)
			if not new_data: break
			data += new_data
			if '\n' in str(new_data,'UTF-8'): break
		data = str(data,'UTF-8')
		json_data = json.loads(data)
		if "request_type" in json_data:
			if json_data["request_type"] == "save_blogs":
				# get the blogs and the links from the request
				try:
					insert_values = []
					blog_list = json_data["blogs"]
					link_list = json_data["links"]
					if len(blog_list) != len(link_list):
						raise Exception
					
					#append these together into a list
					for a in range(len(blog_list)):
						insert_values.append({"name":blog_list[a],"link":link_list[a]})
					#now build the db stuff and insert into the db
					conn_string = "host='localhost' dbname='cs585' user='cs585' "
					db_conn = psycopg2.connect(conn_string)
					cursor = db_conn.cursor()
					for a in insert_values:
						try:
							cursor.execute("insert into blog values(%s,%s);",(a["name"],a["link"]))
							db_conn.commit()
						except Exception as e:
							db_conn.rollback()
							pass
					db_conn.commit()
					cursor.close()
					db_conn.close()
					send_data = {	"worked":True,
								"request_type":"save_blogs",
								}
				except Exception as e:
					print ("WOW: " + str(e))
					send_data = {	"worked":False,
								"request_type":"save_blogs",
								}
			elif json_data["request_type"] == "save_posts":
				# get the blogs and the links from the request
				try:
					insert_values = []
					post_list = json_data["posts"]
					#now build the db stuff and insert into the db
					conn_string = "host='localhost' dbname='cs585' user='cs585' "
					db_conn = psycopg2.connect(conn_string)
					cursor = db_conn.cursor()
					for a in post_list:
						try:
							t = time.gmtime(int(a["timestamp"]))
							if a["title"] != None:
								a["title"] = a["title"][:100]
							cursor.execute("insert into post values(%s,%s,%s,%s,%s,%s,%s,%s);",
									(	a["post_id"],
										a["post_link"],
										a["blog_name"],
										a["type"],
										a["content"][:500],
										psycopg2.Timestamp(t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec),
										a["note_count"],
										a["title"],
									)
								)
							db_conn.commit()
						except Exception as e:
							print ("DB Fail - ",e)
							db_conn.rollback()
							pass
						if "tags" in a:
							for b in a["tags"]:
								try:
									cursor.execute("insert into tag values(%s,%s);",(b,a["post_id"]))
									db_conn.commit()
								except Exception as e:
									db_conn.rollback()
									pass
					db_conn.commit()
					cursor.close()
					db_conn.close()
					send_data = {	"worked":True,
								"request_type":"save_blogs",
								}

				except Exception as e:
					print ("WOW: " + str(e))
					send_data = {	"worked":False,
								"request_type":"save_blogs",
								}

			elif json_data["request_type"] == "save_notes":
				
				# get the blogs and the links from the request
				try:
					insert_values = []
					note_list = json_data["notes"]

					#now build the db stuff and insert into the db
					conn_string = "host='localhost' dbname='cs585' user='cs585' "
					db_conn = psycopg2.connect(conn_string)
					cursor = db_conn.cursor()
					for a in note_list:
						try:
							t = time.gmtime(int(a["timestamp"]))
							cursor.execute("insert into note values(%s,%s,%s,%s);",
									(	a["post_id"],
										a["type"],
										psycopg2.Timestamp(t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec),
										a["blog_name"]
									)
								)
							db_conn.commit()
						except Exception as e:
							db_conn.rollback()
							pass
					db_conn.commit()
					cursor.close()
					db_conn.close()
					send_data = {	"worked":True,
								"request_type":"save_blogs",
								}

				except Exception as e:
					print ("WOW: " + str(e))
					send_data = {	"worked":False,
								"request_type":"save_blogs",
								}


			#make sure we catch all shitty requests
			else:
				#build the json for the return string
				send_data = {	"worked":True,
								"request_type":"NOT RECOGNIZED",
								}
			#make sure we catch all shitty requests
		else:
			#build the json for the return string
			send_data = {"worked":False,"request_type":"NOT RECOGNIZED",}
		#send the message
		conn.send(str.encode(json.dumps(send_data)))
	finally:
		conn.close()

if __name__ == "__main__":
	try:
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		try:
			s.bind((host, port))
		except Exception as e:
			print (e)
			sys.exit()
		s.listen(10)
		
		while True:
			conn,addr = s.accept()
			t = threading.Thread(target=worker, args = (conn,))
			t.start()
	except Exception as e:
		print (e)
	finally:
		print ("closing_socket")
		s.close()

