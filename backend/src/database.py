import os
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase,sessionmaker
url=os.getenv('DATABASE_URL','mysql+pymysql://quiet:quiet-demo@db:3306/quiet')
# READ COMMITTED是MySQL语义；SQLite（本地验证）不支持该隔离级别参数。
opts={'pool_pre_ping':True}|({}if url.startswith('sqlite')else{'isolation_level':'READ COMMITTED'})
engine=create_engine(url,**opts)
class Base(DeclarativeBase): pass
Session=sessionmaker(engine,expire_on_commit=False)
def session():
    with Session() as db:
        try: yield db
        except Exception:
            db.rollback()
            raise
