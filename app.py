
from flask import Flask, jsonify, render_template , request , jsonify, redirect, url_for
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
import threading
from kafka import KafkaConsumer
import json
import time
import redis
import kafka
import random
from kafka.errors import KafkaError
from datetime import datetime

from redis.exceptions import ConnectionError

KAFKA_BROKER = "localhost:9092"
app = Flask(__name__)


# Initialize Redis connection
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# clearing the cache
redis_client.flushdb()
KAFKA_BROKER = "localhost:9092"
topic = "test"
last_processed_offset = -1

    
storedCacheData = {}
last_updated = {}
topic_messages = []

# Function to update cache
@app.route("/updateCache" , methods=["POST"])
def updateCache():
    GetDataFromKafka()
    return jsonify({"status" : "updated data"})


# Function to fetch data from Cache
@app.route('/getData_from_cache',methods=["POST"])
def GetDataFromCache():

    data = request.get_json()
    topic = data.get("topic")
    no_of_records = data.get("no_of_records")
    draw = int(data.get("draw", 1))
    start = int(data.get("start", 0))
    length = int(data.get("length", 10))
    search_value = data.get("search", {}).get("value", "")
    topic = request.json.get('topic')
    print("topic : " , topic)

    if not topic:
        return jsonify({"data": [], "recordsTotal": 0, "recordsFiltered": 0, "draw": draw})
                
    start_time = time.time()

    # if data already found in storedCacheData
    if(topic in storedCacheData) :
        elapsed_time = time.time() - start_time
        if no_of_records is None or no_of_records == '' :
            messages = storedCacheData[topic]
        else:
            messages = storedCacheData[topic][:int(no_of_records)]
        paginated_messages = messages[start : start + length]
        return jsonify({
            "draw": draw,
            "recordsTotal": len(messages),
            "recordsFiltered": len(messages),
            "data": paginated_messages,
            "Time_taken": elapsed_time
        })

    messages = []
    try:

        keys = list(redis_client.scan_iter(match="*"))
        sorted_topic_keys = sorted(keys, key=lambda key: int(key.decode()), reverse=True)        
        cached_data = redis_client.mget(sorted_topic_keys)

        # if not topic_keys:
        if not keys :
            return jsonify({"data": [], "recordsTotal": 0, "recordsFiltered": 0, "draw": draw})

        messages = [json.loads(data) for data in cached_data if data]

        if no_of_records is not None or no_of_records != '' :
            messages = messages[:int(no_of_records)]

    except ConnectionError as e :
        print("Redis is down ....")
        messages = storedCacheData[topic]
        if no_of_records is not None or no_of_records != '' :
            messages = messages[:int(no_of_records)]

    except Exception as e :
        print("error occured : " , e)
        return jsonify({"data": [], "recordsTotal": 0, "recordsFiltered": 0, "draw": draw})


    elapsed_time = time.time() - start_time  # Calculate execution time

    # print("Cached Data" , cached_data)
    paginated_messages = messages[start : start + length]

    if no_of_records is None and topic not in storedCacheData:
        storedCacheData[topic] = messages
    

    return jsonify({
        "draw": draw,
        "recordsTotal": len(messages),
        "recordsFiltered": len(messages),
        "data": paginated_messages,
        "Time_taken": elapsed_time
    })


# Function to fetch data from kafka
@app.route("/getData_from_kafka",methods=["POST"])
def GetDataFromKafka():

    data = request.get_json()
    topic = data.get("topic")
    no_of_records = data.get("no_of_records")
    draw = int(data.get("draw", 1))
    start = int(data.get("start", 0))
    length = int(data.get("length", 10))
    search_value = data.get("search", {}).get("value", "")

    print("topic : " , topic)
    if not topic:
        return jsonify({"data": [], "recordsTotal": 0, "recordsFiltered": 0, "draw": draw})

    start_time = time.time()

    kafka_consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        enable_auto_commit=False,
        auto_offset_reset='earliest',
        consumer_timeout_ms = 500
    )

    partition = kafka.TopicPartition(topic, 0)
    start_offset = kafka_consumer.beginning_offsets([partition])[partition]
    end_offset = kafka_consumer.end_offsets([partition])[partition]

    student_messages = []

    for msg in kafka_consumer:
        redis_key = f"{msg.offset}"
        msg.value["id"] = msg.offset
        student_messages.append(msg.value)

    if no_of_records is not None and int(no_of_records) < end_offset:
        student_messages = student_messages[:int(no_of_records)]

    student_messages = sorted(student_messages, key=lambda x: x['id'],reverse=True)

    if no_of_records is None or int(no_of_records) > end_offset:
        storedCacheData[topic] = student_messages

    paginated_messages = student_messages[start:start + length]
    elapsed_time = time.time() - start_time  # Calculate execution time 
    print(elapsed_time)
    
    return jsonify({
        "draw": draw,
        "recordsTotal": len(student_messages),
        "recordsFiltered": len(student_messages),
        "data": paginated_messages,
        "Time_taken": elapsed_time
    })


# Function which preloads the data from topic into kafka and then to redis cache in background
def preload_topic_data(topic):
    
    global last_processed_offset
    global topic_messages
    global last_updated
    
    """Fetch entire data for a single topic and store in Redis asynchronously."""
    kafka_consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        enable_auto_commit=False,
        auto_offset_reset='earliest',
        consumer_timeout_ms = 500
    )

    partition = kafka.TopicPartition(topic, 0)
    kafka_consumer.assign([partition])
    start_offset = kafka_consumer.beginning_offsets([partition])[partition]
    end_offset = kafka_consumer.end_offsets([partition])[partition]

    start_time = time.time()

    redis_down = False
    print("End Offset : " , end_offset)
    for msg in kafka_consumer:
        redis_key = f"{msg.offset}"
        msg.value["id"] = msg.offset
        student_data = json.dumps(msg.value)
        try:
            redis_client.set(redis_key, student_data)
            redis_down = False
        except ConnectionError as e :
            redis_down = True
        redis_key = msg.offset
        topic_messages.append(student_data)

    if(redis_down == False) :
        last_processed_offset = end_offset
        
    # print("last Proccessed : " , last_processed_offset)

    parsed_data = [json.loads(item) for item in topic_messages]
    topic_messages = sorted(parsed_data, key=lambda x: x['id'], reverse=True)
    # print("topic_messages : ",topic_messages)

    elapsed_time = time.time() - start_time
    print(f"Time taken to preload topic '{topic}': {elapsed_time:.4f} seconds")

threading.Thread(target=preload_topic_data, args=(topic,) , daemon=True).start()


# Routes which renders templates

# home route
@app.route('/')
def index():
    return render_template("index.html")

# Render Redis cache page
@app.route("/fetch_from_redis/<int:no_of_records>")
def fetch_from_redis(no_of_records):
    return render_template("cache_page.html" , data = no_of_records)

# Render Kafka cache page
@app.route("/fetch_from_kafka/<int:no_of_records>")
def fetch_from_kafka(no_of_records):
    return render_template("kafka_page.html" , data = no_of_records)

if __name__ == '__main__':
    app.run(debug=True)
