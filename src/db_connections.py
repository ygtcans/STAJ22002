from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker as sessionMaker
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm.session import Session
from dotenv import load_dotenv
from minio import Minio
import os
from abc import ABC, abstractmethod

# Load the .env file
load_dotenv()

class BaseDBConnection(ABC):
    """
    Abstract base class for database connections.
    """
    def _init_(self):
        pass

    @abstractmethod
    def connect(self):
        """
        Abstract method to connect to the database.
        """
        pass

class PostgreSQLDB(BaseDBConnection):
    """
    Class for PostgreSQL database connection.
    """
    def _init_(self):
        """
        Initializes PostgreSQLDB object and fetches connection details from environment variables.
        """
        self.host = os.getenv("POSTGRES_HOST")
        self.port = os.getenv("POSTGRES_PORT")
        self.db = os.getenv("POSTGRES_DB")
        self.user = os.getenv("POSTGRES_USER")
        self.password = os.getenv("POSTGRES_PASSWORD")

        # Initialize SQLAlchemy Engine and Session objects
        self.engine: Engine = None
        self.Session: Session = None

    def connect(self):
        """
        Connects to the PostgreSQL database.
        
        Returns:
            Engine: SQLAlchemy Engine object representing the database connection.
            Session: SQLAlchemy Session object representing the database session.
        """
        try:
            self.engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}')
            self.Session = sessionMaker(bind=self.engine)
            print("Successfully connected to: ", self.engine.url)
        except Exception as e:
            print("Connection failed: ", e)
        return self.engine, self.Session

    def disconnect(self):
        """
        Disconnects from the PostgreSQL database.
        """
        try:
            if self.engine:
                self.engine.dispose()
                print("Disconnected from: ", self.engine.url)
            else:
                print("Already disconnected.")    
        except Exception as e:
            print("Disconnection failed: ", e)

    def execute_query(self, query):
        """
        Executes a SQL query on the PostgreSQL database.
        
        Args:
            query (str): SQL query to execute.
        
        Returns:
            ResultProxy: ResultProxy object representing the result of the query.
        """
        try: 
            with self.engine.connect() as connection:
                result = connection.execute(text(query))
                print("Successfully executed.")
                connection.commit()
                return result
        except Exception as e:
            print("Failed: ", e)
            
class MinioClient(BaseDBConnection):
    """
    Class for MinIO client connection.
    """
    def _init_(self):
        """
        Initializes MinioClient object and fetches connection details from environment variables.
        """
        self.endpoint = os.getenv("MINIO_ENDPOINT")
        self.access_key = os.getenv("MINIO_ACCESS_KEY")
        self.secret_key = os.getenv("MINIO_SECRET_KEY")
        self.client: Minio = None
        self.secure: bool = False

    def connect(self):
        """
        Connects to the MinIO server.
        
        Returns:
            Minio: Minio object representing the connection to the server.
        """
        try:
            self.client = Minio(
                self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure
            )
            print(f"Connected to MinIO at {self.endpoint}")
            return self.client
        except Exception as e:
            print(f"Error connecting to MinIO: {e}")

    def create_bucket(self, bucket_name):
        """
        Creates a new bucket on the MinIO server.
        
        Args:
            bucket_name (str): Name of the bucket to create.
        
        Returns:
            None
        """
        if not self.client:
            print("Not connected to MinIO. Please call connect() first.")
            return
        try: 
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                print(f"Bucket '{bucket_name}' created successfully!")
            else:
                print(f"Bucket '{bucket_name}' already exists")
        except Exception as e:
            print(f"{e}")

    def list_buckets(self):
        """
        Lists all buckets on the MinIO server.
        
        Returns:
            None
        """
        if not self.client:
            print("Not connected to MinIO. Please call connect() first.")
            return
        try:
            buckets = self.client.list_buckets()
            for bucket in buckets:
                print(f"{bucket.name} - {bucket.creation_date}")
        except Exception as e:
            print("Error: ", e)