from kafka import KafkaProducer
import time, json, uuid, pymysql # mariadb import 제거
import threading
from datetime import datetime

# 수정한 부분 - bootstrap_servers 의 주소를 localhost 에서 kafka-docker의 IPv4 주소로 변경
# producer = KafkaProducer(bootstrap_servers=["172.20.0.101:9092"])
producer = KafkaProducer(bootstrap_servers=["127.0.0.1:9092"])

config = {
    'host': '127.0.0.1', # 수정한 부분 - 내 mysql 의 IPv4 주소로 변경
    'port': 13306,
    'user': 'root',
    'password': '', # 비밀 번호 EMPTY 로 했으므로 삭제
    'database': 'mydb',
    'charset': 'utf8'
}
conn = pymysql.connect(**config) # mariadb에서 pymysql로 변경
cursor = conn.cursor()

sql = '''SELECT * FROM users'''
sql2 = '''SELECT * FROM festival'''
sql3 = '''SELECT * FROM store'''
sql4 = '''SELECT * FROM menu'''
sql5 = '''SELECT * FROM orders'''
sql6 = '''SELECT * FROM order_detail'''


def insert_latest_info(next_call_in):
    # 10초에 한번씩 전송
    next_call_in += 10

    # consumer로 데이터 전송하는 함수 send
    cursor.execute(sql)
    data = cursor.fetchall()
    producer.send('my_topic_users', value=json.dumps(data, ensure_ascii=False, default=str).encode())
    producer.flush()

    cursor.execute(sql2)
    data = cursor.fetchall()
    producer.send('my_topic_festival', value=json.dumps(data, ensure_ascii=False, default=str).encode())
    producer.flush()

    cursor.execute(sql3)
    data = cursor.fetchall()
    producer.send('my_topic_store', value=json.dumps(data, ensure_ascii=False, default=str).encode())
    producer.flush()

    cursor.execute(sql4)
    data = cursor.fetchall()
    producer.send('my_topic_menu', value=json.dumps(data, ensure_ascii=False, default=str).encode())
    producer.flush()

    cursor.execute(sql5)
    data = cursor.fetchall()
    producer.send('my_topic_orders', value=json.dumps(data, ensure_ascii=False, default=str).encode())
    producer.flush()
    
    cursor.execute(sql6)
    data = cursor.fetchall()
    producer.send('my_topic_order_detail', value=json.dumps(data, ensure_ascii=False, default=str).encode())
    producer.flush()

    threading.Timer(next_call_in - time.time(), insert_latest_info, [next_call_in]).start()
    
next_call_in = time.time()
insert_latest_info(next_call_in)