from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from utils.config_manager import ConfigManager

config = ConfigManager()


USER, PASS, PORT, NAME = config.get_config('Database', {}), 'USER', 'PASS', 'PORT', 'NAME'

SQLALCHEMY_DATABASE_URL = f"postgresql://{USER}:{PASS}@db:{PORT}/{NAME}"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
