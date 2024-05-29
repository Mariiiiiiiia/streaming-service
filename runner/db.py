from asyncpg.pool import PoolConnectionProxy
import datetime

# взаимодействие с бд
async def insert_total_frames(db: PoolConnectionProxy, id:int, total:int):
    """Добавление новой строки"""
    query = f"UPDATE requests set frames_total={total} where id={id};"
    await db.fetchval(query)



