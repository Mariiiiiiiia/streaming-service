from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse
import uuid
import uvicorn
import psycopg2
import asyncpg
from contextlib import asynccontextmanager
from fastapi import APIRouter, Depends, Request, status
import db
from config import cfg
from connect_db import get_connection, stop_connection
from aiokafka import AIOKafkaConsumer
import asyncio
from aiokafka import AIOKafkaProducer
from asyncpg.pool import PoolConnectionProxy



async def write_status(topic:str, request_id: str, state:str):
        db_conn: PoolConnectionProxy = await get_connection()
        try:
            if int(state)==0:
               await db.insert_state(db_conn, int(request_id), f"{topic} failed")
            else:
                await db.insert_state(db_conn, int(request_id), f"{topic} finished")
            await stop_connection(db_conn)
        except ValueError as e:
            return {"detail": str(e)}
        
async def consume_api():
    consumer = AIOKafkaConsumer(
        "api",
        bootstrap_servers='localhost:29092')
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            print('message from api, id: ', msg.value.decode().split(' ')[0])
            await write_status(msg.topic, msg.value.decode().split(' ')[0], msg.value.decode().split(' ')[1])
            await produce_runner(msg.value.decode().split(' ')[0])
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()

async def produce_runner(id):
    producer = AIOKafkaProducer(
        bootstrap_servers=cfg.kafka_host+':'+str(cfg.kafka_port))
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        # Produce message
        print('runner starting')
        await producer.send_and_wait("runner", bytes(str(id), 'utf-8'))
    finally:
        # Wait for all pending messages to be delivered or expire.
        
        await producer.stop()







async def write_status_run(topic:str, request_id: str, state:str):
        db_conn: PoolConnectionProxy = await get_connection()
        try:
            if int(state)==0:
               await db.insert_state(db_conn, int(request_id), f"{topic} failed")
            else:
                await db.insert_state(db_conn, int(request_id), f"{topic} started")
            await stop_connection(db_conn)
        except ValueError as e:
            return {"detail": str(e)}

async def consume_run():
    consumer = AIOKafkaConsumer(
        'run',
        bootstrap_servers='localhost:29092')
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            print('message from runner, id ', msg.value.decode().split(' ')[0])
            await write_status_run('runner', msg.value.decode().split(' ')[0], msg.value.decode().split(' ')[1])
            if msg.value.decode().split(' ')[1]==str(0):
                print('error')
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()

async def write_status_inf(topic:str, request_id: str, frame_id:str, state:str):
        db_conn: PoolConnectionProxy = await get_connection()
        try:
            if int(state)==0:
               await db.insert_state(db_conn, int(request_id), f"{topic} frame_id {frame_id} failed") 
            else:
                await db.insert_state(db_conn, int(request_id), f"{topic} frame_id {frame_id} started")
            await stop_connection(db_conn)
        except ValueError as e:
            print( {"detail": str(e)})
        
async def consume_inference():
    consumer = AIOKafkaConsumer(
        'inference',
        bootstrap_servers='localhost:29092')
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            print('message from inference, id ', msg.value.decode().split(' ')[0])
            await write_status_inf('inference', msg.value.decode().split(' ')[0], msg.value.decode().split(' ')[1],msg.value.decode().split(' ')[2])
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()   


async def main():
    await asyncio.gather(
        consume_run(),
        consume_api(),
        consume_inference()
    )

asyncio.run(main())