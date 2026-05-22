from __future__ import annotations

import asyncio
import importlib
import json
import logging
from collections.abc import AsyncIterator

import grpc
import orjson
from google.protobuf.json_format import MessageToDict, ParseDict
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_never,
    wait_exponential_jitter,
)

from .config import Settings

LOGGER = logging.getLogger(__name__)


class StreamDisconnectedError(RuntimeError):
    pass


class ShredstreamClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def _build_request(self, messages_module: object) -> object:
        request_cls = getattr(messages_module, self._settings.grpc_request_class)
        request = request_cls()

        request_payload = json.loads(self._settings.grpc_request_json)
        if request_payload:
            ParseDict(request_payload, request)
        return request

    def _message_to_dict(self, message: object) -> dict:
        if hasattr(message, "DESCRIPTOR"):
            return MessageToDict(
                message,
                preserving_proto_field_name=True,
                use_integers_for_enums=False,
            )
        if isinstance(message, dict):
            return message
        return {"raw": str(message)}

    @retry(
        retry=retry_if_exception_type(StreamDisconnectedError),
        wait=wait_exponential_jitter(initial=1, max=20),
        stop=stop_never,
        before_sleep=before_sleep_log(LOGGER, logging.WARNING),
        reraise=True,
    )
    async def stream(self) -> AsyncIterator[dict]:
        messages_module = importlib.import_module(self._settings.grpc_messages_module)
        stub_module = importlib.import_module(self._settings.grpc_stub_module)

        stub_cls = getattr(stub_module, self._settings.grpc_stub_class)
        stream_method_name = self._settings.grpc_stream_method

        target = self._settings.shredstream_grpc_target
        LOGGER.info("connecting_to_shredstream target=%s", target)

        try:
            async with grpc.aio.insecure_channel(target) as channel:
                stub = stub_cls(channel)
                request = self._build_request(messages_module)
                stream_method = getattr(stub, stream_method_name)

                async for message in stream_method(request):
                    yield self._message_to_dict(message)
        except grpc.aio.AioRpcError as exc:
            LOGGER.warning("grpc_stream_disconnected code=%s details=%s", exc.code(), exc.details())
            raise StreamDisconnectedError(str(exc)) from exc
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.exception("unexpected_stream_error")
            raise StreamDisconnectedError(str(exc)) from exc


def payload_to_json(payload: dict) -> str:
    return orjson.dumps(payload).decode("utf-8")
