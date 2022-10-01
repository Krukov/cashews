import asyncio
import hashlib
import random
import string
import time
from datetime import datetime, timedelta

import databases
import jwt
import orm
import sqlalchemy
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.security import http
from pydantic import BaseModel
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.status import HTTP_403_FORBIDDEN

from cashews import LockedError, RateLimitError, cache, utils

database = databases.Database("sqlite:///db.sqlite")
metadata = sqlalchemy.MetaData()
app = FastAPI()

cache.setup("redis://", client_side=True)
cache.setup("mem://1", prefix="fail")

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
    with cache.detect as detector:
        response = await call_next(request)
        if request.method.lower() != "get":
            return response
        response.headers["X-Detect-Debug"] = str(detector.calls)
        response.headers["X-From-Cache-keys"] = ";".join(detector.calls.keys())
    return response


@app.exception_handler(RateLimitError)
async def rate_limit_handler(request, exc: RateLimitError):
    return JSONResponse(
        status_code=429,
        content={"error": "too much"},
    )


@app.exception_handler(LockedError)
async def lock_handler(request, exc: LockedError):
    return JSONResponse(
        status_code=500,
        content={"error": "LOCK"},
    )


@app.get("/")
@cache(ttl=timedelta(minutes=1), prefix="test")
async def root():
    await asyncio.sleep(1)
    return "".join([random.choice(string.ascii_letters) for _ in range(10)])


@app.get("/check")
async def _check():
    result = await utils.check_speed(cache, 500)
    result["mem"] = cache._target._local_cache.store
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
        status_code=HTTP_403_FORBIDDEN,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": ""},
    )


@app.get("/me")
@cache(ttl=timedelta(minutes=10), key="me:{user.name}")
async def get_me(user: User = Depends(get_current_user)):
    friends = await database.fetch_all(
        "SELECT name, relations.kind FROM user JOIN relations ON relations.target = user.id WHERE relations.owner = :owner",  # noqa: E501
        {"owner": user.id},
    )
    return {"name": user.name, "friends": friends}


class RedisDownError(Exception):
    pass


@app.get("/rank")
@cache.failover(ttl=timedelta(minutes=5), key="{accept_language}")
@cache.rate_limit(limit=100, period=timedelta(seconds=2), ttl=timedelta(minutes=1))
@cache.early(ttl=timedelta(minutes=2), early_ttl=timedelta(minutes=1), key="{accept_language}")
async def get_rang(accept_language: str = Header("en"), user_agent: str = Header("No")):
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
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
