import logging
import asyncpg as pg
from typing import Any, AsyncGenerator, Generator
from asyncpg.pool import PoolConnectionProxy
from config import Config, cfg
from fastapi import FastAPI
from minio import Minio
from io import BytesIO
import boto3


class Database:

    def __init__(self, cfg: Config, retry: int = 1):
        self.dsn = cfg.build_postgres_dsn
        self.retry = retry

    async def connect(self):

        pool = await pg.create_pool(dsn=self.dsn)
        if pool is None:
            for _ in range(self.retry):
                pool = await pg.create_pool(dsn=self.dsn)
                if pool is not None:
                    break
        if pool is None:
            raise Exception(f"can't connect to db in {self.retry} retries")
        self.pool = pool

    async def disconnect(self):
        await self.pool.close()
  


db_instance = Database(cfg)

async def get_connection() -> AsyncGenerator[PoolConnectionProxy, None]:
    async with db_instance.pool.acquire() as connection:
        try:
            yield connection
        finally:
            await connection.close() 

async def stop_connection(connection):
        await connection.close()



s3 = Minio(
    cfg.s3_host,
    access_key=cfg.access_key,
    secret_key=cfg.secret_key,
    secure=False,
)
if not s3.bucket_exists("videos"):
    s3.make_bucket("videos", "eu-west-1", True)

