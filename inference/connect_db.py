import logging
import asyncpg as pg
from typing import Any, AsyncGenerator, Generator
from asyncpg.pool import PoolConnectionProxy
from config import Config, cfg
from fastapi import FastAPI
from minio import Minio
from io import BytesIO
import boto3
import asyncpg


  




async def get_connection():
        return await asyncpg.connect(
            user=cfg.postgres_user,
            password=cfg.postgres_password,
            database=cfg.postgres_db,
            host=cfg.postgres_host,
            port=cfg.postgres_port
        )
async def stop_connection(connection):
        await connection.close()


s3 = Minio(
    cfg.s3_host,
    access_key=cfg.access_key,
    secret_key=cfg.secret_key,
    secure=False,
)

