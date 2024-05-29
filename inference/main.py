from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse
import uuid
import uvicorn
import psycopg2
import asyncpg
from contextlib import asynccontextmanager
from fastapi import APIRouter, Depends, Request, status
import db
from db import write_result
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
import numpy as np
from imageai.Detection import ObjectDetection
from aiomultiprocess import Process
from multiprocessing import  Queue
from asyncpg.pool import PoolConnectionProxy
import cv2
from PIL import Image
from io import BytesIO
from ultralytics import YOLO

        

model = YOLO('yolov9c.pt')

async def consume():
    consumer = AIOKafkaConsumer(
        "runner-inference",
        bootstrap_servers='localhost:29092')
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        
        # Consume messages
        async for msg in consumer:
             await start_process(json.loads(msg.value.decode())['id'], json.loads(msg.value.decode())['frame_id'], json.loads(msg.value.decode())['frame'])
            #await start_process(msg.value.decode())
            ##
            
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()




async def produce(id, frame_id, state):
    producer = AIOKafkaProducer(
        bootstrap_servers=cfg.kafka_host+':'+str(cfg.kafka_port))
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        # Produce message
        await producer.send_and_wait('inference', bytes(str(id)+' '+str(frame_id) +' '+ str(state), 'utf-8'))
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()


processes={}
queues={}
async def start_process(id, frame_id, frame):
    try:
        process=processes.get(int(id))
        if process and process.is_alive():
            queues[int(id)].put((frame_id, frame))
        elif not process:
            queue = Queue()
            queue.put((frame_id, frame))
            queues[int(id)]=queue
            print('starting process: ', id)
            process=Process(target=get_predict, args=(id,queues[int(id)],),)
            processes[int(id)]=process
            process.start()
    except:
        await produce(id, frame_id,0)


async def get_predict(id, queue):
    frame_id=0
    try:
        while True:
            frame_id, frame=queue.get()
            await produce(id, frame_id,1)
            #raise Exception
            img_bytes = base64.b64decode(frame)
            img = Image.open(BytesIO(img_bytes))
            results = model([img], verbose=False)
            await send_to_bd(id, frame_id, results[0].verbose())
            await collect_amount(id)
            print(f"id: {id}, frame_id: {frame_id}, results: {results[0].verbose()}")
            
    except:
        print(f'failed id: {id}, frame_id: {frame_id}')
        await produce(id, frame_id,0)
        


async def send_to_bd(id, a, res):
    try:
        db_conn=await get_connection()
        await write_result(db_conn, int(id), int(a), res)
        await stop_connection(db_conn)
        await db_conn.close()
    except:
        print("can't put the result into db")
async def collect_amount(id):
    try:
        db_conn=await get_connection()
        await db.write_amount(db_conn, int(id))
        await stop_connection(db_conn)
        await db_conn.close()
    except:
        print("can't put the amount into db")


if __name__=='__main__':
    asyncio.run(consume())