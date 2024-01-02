import asyncio

from cashews import cache, default_formatter

cache.setup("mem://")


class T:
    @classmethod
    @cache.early(ttl="1h", early_ttl="45m", key="user:{cls:get_user_id({token})}")
    async def cached_class_method(cls, token: str):
        # Do something with token
        return {"hello": "world"}

    @classmethod
    def get_user_id(cls, token):
        return f"{cls.__name__}:user_id:{token}"


class D(T):
    pass


@default_formatter.register("get_user_id", preformat=False)
def get_user_id(cls, token):
    # Do something with token
    return cls.get_user_id(token)


print(asyncio.run(D.cached_class_method("token_value")))
