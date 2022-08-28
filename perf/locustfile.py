from locust import between, task
from locust.contrib.fasthttp import FastHttpUser


class Run(FastHttpUser):
    wait_time = between(1, 5)

    @task
    def simple(self):
        self.client.get("/")

    @task
    def early(self):
        self.client.get("/early")

    @task
    def hit(self):
        self.client.get("/hit")
