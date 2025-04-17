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
        self.CREW = os.getenv("CREW")
        if not self.MOVIES or not self.RATINGS or not self.CREW:
            raise ValueError("MOVIES, RATINGS, and CREW must be set in the environment")
        self.QUERY1 = os.getenv("QUERY1")
        self.QUERY2 = os.getenv("QUERY2")
        self.QUERY3 = os.getenv("QUERY3")
        self.QUERY4 = os.getenv("QUERY4")
        self.QUERY5 = os.getenv("QUERY5")
        if not self.QUERY1 or not self.QUERY2 or not self.QUERY3 or not self.QUERY4 or not self.QUERY5:
            raise ValueError("QUERY1 and QUERY2 must be set in the environment")
        self.BATCH_SIZE = int(os.getenv("BATCH_SIZE") or 1024 * 1024)  # 1MB
        if not isinstance(self.BATCH_SIZE, int) or self.BATCH_SIZE <= 0:
            raise ValueError("BATCH_SIZE must be a positive integer")
        self.HOST = os.getenv("HOST")
        self.PORT = int(os.getenv("PORT"))
        if not self.HOST or not self.PORT:
            raise ValueError("HOST and PORT must be set in the environment")
        
        self._initialized = True
        
    def get_movies(self):
        return self.MOVIES
    def get_ratings(self):
        return self.RATINGS
    def get_crew(self):
        return self.CREW
    def get_query1(self):
        return self.QUERY1
    def get_query2(self):
        return self.QUERY2
    def get_query3(self):
        return self.QUERY3
    def get_query4(self):
        return self.QUERY4
    def get_query5(self):
        return self.QUERY5
    def get_batch_size(self):
        return self.BATCH_SIZE
    def get_host(self):
        return self.HOST
    def get_port(self):
        return self.PORT
    def get_all(self):
        return {
            "MOVIES": self.MOVIES,
            "RATINGS": self.RATINGS,
            "CREW": self.CREW,
            "QUERY1": self.QUERY1,
            "QUERY2": self.QUERY2,
            "QUERY3": self.QUERY3,
            "QUERY4": self.QUERY4,
            "QUERY5": self.QUERY5,
            "BATCH_SIZE": self.BATCH_SIZE
        }