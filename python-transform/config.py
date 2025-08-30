import os
from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()

class Config:
    POSTGRES_URI = os.getenv("POSTGRES_URI")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

    # Example table name for transformed data
    TRANSFORMED_TABLE = os.getenv("TRANSFORMED_TABLE", "transformed_data")
