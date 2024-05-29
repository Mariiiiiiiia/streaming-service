from asyncpg.pool import PoolConnectionProxy
import datetime

# взаимодействие с бд
async def write_result(db: PoolConnectionProxy, id:int, frame_id, res):
    """Добавление новой строки"""
    query = f"INSERT INTO results(frame_id, request_id, prediction) VALUES({frame_id}, {id}, '{res}');"
    await db.fetchval(query)

async def write_amount(db, id):
    query = f'select frames_done from requests where id={id}'
    amount=await db.fetchval(query)
    query = f"UPDATE requests set frames_done={int(amount)+1} where id={id};"
    await db.fetchval(query)

