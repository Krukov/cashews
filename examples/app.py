import asyncio
import random
import string
import hashlib
import time

from operator import attrgetter
from datetime import datetime, timedelta
import ujson

from fastapi import FastAPI, Depends, HTTPException, Header
from starlette.requests import Request
from fastapi.security import http
from starlette.responses import JSONResponse
from starlette.status import HTTP_403_FORBIDDEN
from pydantic import BaseModel

from cashews import (
    cache,
    mem,
    CircuitBreakerOpen,
    RateLimitException,
    context_cache_detect,
    check_speed,
    LockedException,
)

import databases
import orm
import sqlalchemy
import jwt

database = databases.Database("sqlite:///db.sqlite")
metadata = sqlalchemy.MetaData()
app = FastAPI()


cache.setup("redis://?hash_key=test&safe=True&maxsize=20&create_connection_timeout=0.01")


SECRET_KEY = "test"
ALGORITHM = "HS256"


class Auth(BaseModel):
    username: str
    password: str


class User(orm.Model):
    __tablename__ = "user"
    __metadata__ = metadata
    __database__ = database

    id = orm.Integer(primary_key=True)
    name = orm.String(max_length=100, unique=True, index=True)
    password = orm.String(max_length=100)
    language = orm.String(max_length=10)
    is_active = orm.Boolean()

    @classmethod
    async def create(cls, name: str, password: str, **kwargs):
        password = hashlib.sha256(password.encode()).hexdigest()
        return await cls.objects.create(name=name, password=password, **kwargs)


class UserRelations(orm.Model):
    __tablename__ = "relations"
    __metadata__ = metadata
    __database__ = database

    id = orm.Integer(primary_key=True)
    owner = orm.ForeignKey(User)
    target = orm.ForeignKey(User)
    kind = orm.String(max_length=10, index=True, default="friend")


def get_current_user_id(token=Depends(http.HTTPBearer())):
    try:
        payload = jwt.decode(
            token.credentials,
            SECRET_KEY,
            algorithms=[ALGORITHM],
            options={"verify_exp": True, "verify_aud": False},
            verify=True,
        )
    except jwt.DecodeError as exc:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": str(exc)},
        )
    return payload["id"]


@cache(ttl=timedelta(minutes=5))
async def get_current_user(user_id: str = Depends(get_current_user_id)):
    return await User.objects.get(id=user_id)


def check_password(user_password, input_password) -> bool:
    return hashlib.sha256(input_password.encode()).hexdigest() == user_password


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.perf_counter()
    response = await call_next(request)
    process_time = time.perf_counter() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response


@app.middleware("http")
async def add_from_cache_headers(request: Request, call_next):
    context_cache_detect.start()
    response = await call_next(request)
    keys = context_cache_detect.get()
    if keys:
        key = list(keys.keys())[0]
        response.headers["X-From-Cache"] = key
        expire = await mem.get_expire(key)
        if expire == -1:
            expire = await cache.get_expire(key)
        response.headers["X-From-Cache-Expire-In-Seconds"] = str(expire)
        response.headers["X-From-Cache-TTL"] = str(keys[key]["ttl"].total_seconds())
        if "exc" in keys[key]:
            response.headers["X-From-Cache-Exc"] = str(type(keys[key]["exc"]))
    return response


@app.exception_handler(RateLimitException)
async def rate_limit_handler(request, exc: RateLimitException):
    return JSONResponse(status_code=429, content=ujson.dumps({"error": "too much"}),)


@app.get("/")
async def _check():
    result = await check_speed.run(cache, 500)
    result["mem"] = mem._target.store
    result["tasks"] = len(asyncio.all_tasks())
    return result


@app.post("/token")
async def get_token(auth: Auth):
    user: User = await User.objects.filter(name=auth.username).limit(1).all()
    if user and check_password(user[0].password, auth.password):
        return {
            "token": jwt.encode(
                {"id": user[0].id, "exp": datetime.utcnow() + timedelta(minutes=60)},
                key=SECRET_KEY,
                algorithm=ALGORITHM,
            )
        }
    raise HTTPException(
        status_code=HTTP_403_FORBIDDEN, detail="Could not validate credentials", headers={"WWW-Authenticate": ""},
    )


@app.get("/me")
@cache(ttl=timedelta(minutes=10), func_args={"user": attrgetter("name")})
async def get_me(user: User = Depends(get_current_user)):
    friends = await database.fetch_all(
        "SELECT name, relations.kind FROM user JOIN relations ON relations.target = user.id WHERE relations.owner = :owner",
        {"owner": user.id},
    )
    return {"name": user.name, "friends": friends}


class RedisDownException(Exception):
    pass


@app.get("/rank")
@cache.fail(
    ttl=timedelta(minutes=10),
    exceptions=(CircuitBreakerOpen, RateLimitException, LockedException),
    func_args=("accept_language"),
)
@mem.fail(
    ttl=timedelta(minutes=5),
    exceptions=(CircuitBreakerOpen, RateLimitException, LockedException, RedisDownException),
    func_args=("accept_language"),
)
@cache.rate_limit(limit=100, period=timedelta(seconds=2), ttl=timedelta(minutes=1), func_args=())
@mem.circuit_breaker(errors_rate=10, period=timedelta(minutes=10), ttl=timedelta(minutes=1), func_args=())
@cache.early(ttl=timedelta(minutes=1), func_args=("accept_language"))
@cache.locked(func_args=("accept_language"))
async def get_rang(accept_language: str = Header("en"), user_agent: str = Header("No")):
    if await cache.ping() is None:
        raise RedisDownException()
    rank = 0
    for user in await User.objects.filter(language=accept_language).all():
        for relation in await UserRelations.objects.filter(owner=user).all():
            rank += {"friend": 10, "dude": 3, "relative": 15}[relation.kind]
    return {"rank": rank, "language": accept_language}


async def create_data():
    user = await User.create("test", "test", is_active=True, language="en")
    users = [
        user,
    ]
    for i in range(10000):
        name = "".join([random.choice(string.ascii_letters) for _ in range(10)])
        language = random.choice(["en", "es", "th"])
        users.append(await User.create(name, name, is_active=True, language=language))
    for i in range(5000):
        await UserRelations.objects.create(
            owner=random.choice(users), target=random.choice(users), kind=random.choice(["friend", "relative", "dude"])
        )


if __name__ == "__main__":
    engine = sqlalchemy.create_engine(str(database.url))
    metadata.create_all(engine)
    # asyncio.run(create_data())
    # asyncio.run(get_rang("en"))
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
