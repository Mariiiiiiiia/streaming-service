from asyncpg.pool import PoolConnectionProxy
import datetime

# взаимодействие с бд
async def insert_state(db: PoolConnectionProxy, id:int, status: str):
    """Добавление новой строки"""
    query = f"INSERT INTO state_machine(fk_request, state) VALUES({id}, '{status}')"
    await db.fetchval(query)

async def get_smth(db: PoolConnectionProxy, id: int):
    """Добавление новой строки"""
    query = f'select frames_total, frames_done from requests where id={id}'
    result = await db.fetch(query)
    return result

