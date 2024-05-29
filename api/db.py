from asyncpg.pool import PoolConnectionProxy
import datetime

# взаимодействие с бд
async def insert_request(db: PoolConnectionProxy):
    """Добавление новой строки"""
    query = 'INSERT INTO requests(frames_total, frames_done) VALUES(0, 0) RETURNING id;'
    result = await db.fetchval(query)
    return result

async def get_status(db: PoolConnectionProxy, id: int):
    """Добавление новой строки"""
    query = f'select frames_total, frames_done from requests where id={id}'
    result = await db.fetch(query)
    return result

async def get_pred(db: PoolConnectionProxy, id: int):
    query = f'select frame_id, prediction from results where request_id={id}'
    result = await db.fetch(query)
    return result
