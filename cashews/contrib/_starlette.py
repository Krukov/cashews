from starlette.responses import StreamingResponse

from cashews.backends.interface import Backend
from cashews.serialize import register_type


async def encode_streaming_response(
    value: StreamingResponse, backend: Backend, key: str, expire: int, **kwargs
) -> bytes:
    value.body_iterator = set_iterator(backend, key, value.body_iterator, expire)
    serialized_value = b""
    serialized_value += bytes(str(value.status_code), "utf-8") + b":"
    for header, header_value in value.raw_headers:
        serialized_value += header + b"=" + header_value + b";"
    return serialized_value


async def decode_streaming_response(value: bytes, backend: Backend, key: str, **kwargs) -> StreamingResponse:
    status_code, headers = value.split(b":")
    status_code = int(status_code)
    raw_headers = []
    for header in headers.split(b";"):
        if not header:
            continue
        header_name, header_value = header.split(b"=")
        raw_headers.append((header_name, header_value))

    content = get_iterator(backend, key)
    resp = StreamingResponse(content=content, status_code=status_code)
    resp.raw_headers = raw_headers
    return resp


async def set_iterator(backend: Backend, key: str, iterator, expire: int):
    chunk_number = 0
    async for chunk in iterator:
        await backend.set(f"{key}:chunk:{chunk_number}", chunk, expire=expire)
        yield chunk
        chunk_number += 1


async def get_iterator(backend: Backend, key: str):
    chunk_number = 0
    while True:
        chunk = await backend.get(f"{key}:chunk:{chunk_number}")
        if not chunk:
            return
        yield chunk
        chunk_number += 1


register_type(StreamingResponse, encode_streaming_response, decode_streaming_response)
