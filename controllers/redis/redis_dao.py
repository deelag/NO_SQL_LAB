import os
import redis

COMPLETED = "COMPLETED"
IN_PROGRESS = "IN_PROGRESS"

class RedisDAO:
    def __init__(self, identifier, strategy):
        self.status_key = f"{identifier}_{strategy}_status"
        self.redis_client = redis.StrictRedis(host=os.getenv("REDIS__HOST"), port=os.getenv("REDIS__PORT", 6380),
                                              password=os.getenv("REDIS__PASSWORD"), ssl=True)
    
    def set_in_progress_status(self):
        self.redis_client.set(self.status_key, IN_PROGRESS)

    def set_completed_status(self):
        self.redis_client.set(self.status_key, COMPLETED)

    def already_processed(self):
        redis_value = self.redis_client.get(self.status_key)
        if not redis_value:
            return False

        status = redis_value.decode()
        if status != COMPLETED:
            return False
        return True