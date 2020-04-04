from locust import TaskSet, task, between
import random
from locust.contrib.fasthttp import FastHttpLocust


class Cache(TaskSet):
    @task()
    def rank(self):
        self.client.get("/rank", headers={"Accept-language": random.choice(["en", "es", "th"])})


class Run(FastHttpLocust):
    task_set = Cache
    wait_time = between(1, 5)
