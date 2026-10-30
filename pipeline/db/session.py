import os
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session

from pipeline.config import settings

_engine: Engine | None = None
_pid: int | None = None


def get_engine() -> Engine:
    """One engine per process; prefork children build their own after fork."""
    global _engine, _pid
    if _engine is None or _pid != os.getpid():
        url = settings.database_url
        if url.startswith("sqlite"):
            _engine = create_engine(url, connect_args={"check_same_thread": False})
        else:
            _engine = create_engine(url, pool_pre_ping=True, pool_size=10, max_overflow=20)
        _pid = os.getpid()
    return _engine


def set_engine(engine: Engine) -> None:
    global _engine, _pid
    _engine, _pid = engine, os.getpid()


@contextmanager
def session_scope():
    """One transaction: commit on success, roll back on any exception."""
    session = Session(get_engine(), expire_on_commit=False)
    try:
        yield session
        session.commit()
    except BaseException:
        session.rollback()
        raise
    finally:
        session.close()
