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
from config import cfg
from connect_db import s3
from connect_db import get_connection, stop_connection
from aiokafka import AIOKafkaProducer
import asyncio



from asyncpg.pool import PoolConnectionProxy
@asynccontextmanager
async def lifespan(app: FastAPI):
    db = db_instance
    await db.connect()
    app.state.db = db
    yield
    await db.disconnect()

app = FastAPI(lifespan=lifespan)


@app.post("/upload")
async def upload_video(video: UploadFile = File(...),
    db_conn: PoolConnectionProxy = Depends(get_connection)):
        try:
            new_id: int = await db.insert_request(db_conn)
            s3.put_object(
                        "videos",
                        f"{new_id}.mp4",
                        video.file,
                        length=video.size,
                    )
            print('put video into s3')
           # await stop_connection(db_conn)
            await send_msg(new_id, 1)
            return {"id": str(new_id)}
        except Exception as e:
            await send_msg(new_id, 0)
            return {"detail": str(e)}


@app.get("/status/{request_id}")
async def get_status(request_id: int,
    db_conn: PoolConnectionProxy = Depends(get_connection)):
        try:
            result = await db.get_status(db_conn, request_id)
           # await stop_connection(db_conn)
            res = await db.get_pred(db_conn, request_id)
            print('sending results for id:', request_id)
            return {"frames_done": result[0][1], "frames_total": result[0][0], 'results': res}
        except Exception as e:
            return {"detail": str(e)}
        
async def send_msg(id, state):
    producer = AIOKafkaProducer(
        bootstrap_servers=cfg.kafka_host+':'+str(cfg.kafka_port))
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        # Produce message
        await producer.send_and_wait("api", bytes(str(id)+' '+str(state), 'utf-8'))
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()


if __name__=='__main__':
    uvicorn.run(app, host='127.0.0.1', port=9999)