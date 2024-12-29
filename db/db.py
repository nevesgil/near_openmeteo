from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Database Configuration
DATABASE_URL = "postgresql://admin:admin@localhost:5432/database"

# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Base class for models
Base = declarative_base()

# Weather Data Model
class WeatherData(Base):
    __tablename__ = "weather_data"

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False)
    temperature = Column(Float)
    windspeed = Column(Float)
    winddirection = Column(Float)
    weathercode = Column(Integer)

# Kafka Offset Model
class KafkaOffset(Base):
    __tablename__ = "kafka_offsets"

    id = Column(Integer, primary_key=True)
    topic = Column(String, nullable=False)
    partition = Column(Integer, nullable=False)
    offset = Column(BigInteger, nullable=False)
    timestamp = Column(DateTime, nullable=False)

# Create tables
Base.metadata.create_all(engine)

# Session factory
Session = sessionmaker(bind=engine)
