from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse
import uuid
import uvicorn
import psycopg2
import asyncpg
from contextlib import asynccontextmanager
from connect_db import db_instance
from fastapi import APIRouter, Depends, Request, status
import db
import json
import base64
from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer
import asyncio
from config import cfg
from connect_db import s3
from connect_db import get_connection, stop_connection
from aiokafka import AIOKafkaProducer
import asyncio
from aiomultiprocess import Process
from asyncpg.pool import PoolConnectionProxy
import cv2
from PIL import Image
from io import BytesIO

        
async def consume():
    consumer = AIOKafkaConsumer(
        "runner",
        bootstrap_servers='localhost:29092')
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            await start_process(msg.value.decode())
            ##
            
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()



processes={}
async def start_process(id):
    await produce(id, str(1))
    process=Process(target=get_video, args=(id,),)
    processes[id]=process
    process.start()




async def get_video(id):
        try:
            video=s3.presigned_get_object(bucket_name='videos', object_name=f'{id}.mp4')  
            cap = cv2.VideoCapture(video)
            cnt = 0
            amount=0
            while True:
                    ok, img=cap.read()
                    cnt+=1
                    fps = int(cap.get(cv2.CAP_PROP_FPS))
                    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))        
                    if ok and cnt % (10*fps) == 1:
                            amount+=1
                            img_bytes = cv2.imencode(".jpg", img)[1].tobytes()
                            img_base64 = base64.b64encode(img_bytes).decode('utf-8') 
                            frame_message = {"id": id, "frame_id": str(amount), "frame": img_base64}
                            await producer(frame_message)
                    if not ok:
                        db_conn: PoolConnectionProxy = await get_connection()
                        await db.insert_total_frames(db_conn, id, amount)
                        await stop_connection(db_conn)
                        print('Frames process finished')
                        break
            cap.release()
        except:
            await produce(id, str(0))

def serializer(value):
    return json.dumps(value).encode(encoding="utf-8")


async def producer(frame_message):
    producer = AIOKafkaProducer(
        bootstrap_servers=cfg.kafka_host+':'+str(cfg.kafka_port), 
                    value_serializer=serializer)
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        # Produce message
        print('sending frame to inference')
        await producer.send_and_wait("runner-inference", frame_message)
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()


async def produce(id, state):
    producer = AIOKafkaProducer(
        bootstrap_servers=cfg.kafka_host+':'+str(cfg.kafka_port))
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        # Produce message
        await producer.send_and_wait('run', bytes(str(id)+' '+str(state), 'utf-8'))
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()
        

if __name__=='__main__':
    asyncio.run(consume())