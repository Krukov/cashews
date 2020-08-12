from locust import task, between
import random
from locust.contrib.fasthttp import FastHttpUser


class Run(FastHttpUser):
    wait_time = between(1, 5)

    @task
    def rank(self):
        self.client.get("/rank", headers={"Accept-language": random.choice(["en", "es", "th"])})
