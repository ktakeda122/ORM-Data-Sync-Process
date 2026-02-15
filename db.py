import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# MySQL (Source)
MYSQL_URL = "mysql+mysqlconnector://root:password@localhost/sakila"
# SQLite (Target)
SQLITE_URL = "sqlite:///analytics.db"

# Engine
source_engine = create_engine(MYSQL_URL, echo=False) # echo=True to get SQL log
target_engine = create_engine(SQLITE_URL, echo=False)

# Session
SourceSession = sessionmaker(bind=source_engine)
TargetSession = sessionmaker(bind=target_engine)

# thread safe
def get_source_session():
    return SourceSession()

def get_target_session():
    return TargetSession()