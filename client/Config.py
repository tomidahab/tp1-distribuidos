import os
import dotenv
dotenv.load_dotenv()

class Config:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        # Only initialize once
        if self._initialized:
            return
            
        self.MOVIES = os.getenv("MOVIES")
        self.RATINGS = os.getenv("RATINGS")
        self.CREDITS = os.getenv("CREDITS")
        if not self.MOVIES or not self.RATINGS or not self.CREDITS:
            raise ValueError("MOVIES, RATINGS, and CREDITS must be set in the environment")
        self.BATCH_SIZE = int(os.getenv("BATCH_SIZE") or 1024 * 1024)  # 1MB
        if not isinstance(self.BATCH_SIZE, int) or self.BATCH_SIZE <= 0:
            raise ValueError("BATCH_SIZE must be a positive integer")
        self.HOST = os.getenv("HOST")
        self.PORT = int(os.getenv("PORT"))
        if not self.HOST or not self.PORT:
            raise ValueError("HOST and PORT must be set in the environment")
        self.EOF = os.getenv("EOF_MARKER")
        if not self.EOF:
            raise ValueError("EOF_MARKER must be set in the environment")
        self.SIGTERM = os.getenv("SIGTERM","SIGTERM")
        
        self._initialized = True
        
    def get_movies(self):
        return self.MOVIES
    def get_ratings(self):
        return self.RATINGS
    def get_credits(self):
        return self.CREDITS
    def get_batch_size(self):
        return self.BATCH_SIZE
    def get_host(self):
        return self.HOST
    def get_port(self):
        return self.PORT
    def get_EOF(self):
        return self.EOF
    def get_all(self):
        return {
            "MOVIES": self.MOVIES,
            "RATINGS": self.RATINGS,
            "CREDITS": self.CREDITS,
            "BATCH_SIZE": self.BATCH_SIZE,
            "HOST": self.HOST,
            "PORT": self.PORT,
            "EOF_MARKER": self.EOF
        }