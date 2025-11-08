import json
import logging
import ssl
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional
import websocket


CallbackType = Callable[[dict], None]


@dataclass
class _ConnectionState:
    stream_name: str
    url: str
    payload: Optional[str]
    callback: CallbackType
    is_live: bool
    should_reconnect: bool = True
    retries: int = 0
    thread: Optional[threading.Thread] = None
    ws_app: Optional[websocket.WebSocketApp] = None
    lock: threading.Lock = field(default_factory=threading.Lock)


class AsterSocketManager:
    _INITIAL_DELAY = 0.1
    _MAX_DELAY = 10
    _FACTOR = 2.0
    _MAX_RETRIES = 10
    _PING_INTERVAL = 300
    _PING_TIMEOUT = 5
    _RECONNECT_ERROR_PAYLOAD = {"e": "error", "m": "Max reconnect retries reached"}

    def __init__(self, stream_url):
        if not stream_url.startswith("wss://"):
            raise ValueError("expected wss:// URL prefix")

        self.stream_url = stream_url.rstrip("/")
        self._conns: Dict[str, _ConnectionState] = {}
        self._sslopt = {"cert_reqs": ssl.CERT_REQUIRED}

    def _start_socket(
        self, stream_name, payload, callback, is_combined=False, is_live=True
    ):
        if stream_name in self._conns:
            return False

        if is_combined:
            target_url = f"{self.stream_url}/stream"
        else:
            target_url = f"{self.stream_url}/ws"

        payload_text: Optional[str] = None
        if payload is not None:
            payload_text = payload.decode("utf8")

        if not is_live and payload_text:
            payload_obj = json.loads(payload_text)
            params = payload_obj["params"]
            if is_combined:
                target_url = f"{target_url}?streams={params}"
            else:
                target_url = f"{target_url}/{params}"
            payload_text = None

        logging.info("Connection with URL: %s", target_url)

        state = _ConnectionState(
            stream_name=stream_name,
            url=target_url,
            payload=payload_text,
            callback=callback,
            is_live=is_live,
        )

        thread = threading.Thread(
            target=self._run_connection,
            name=f"AsterWS-{stream_name}",
            args=(state,),
            daemon=True,
        )
        state.thread = thread
        self._conns[stream_name] = state
        thread.start()
        return True

    def _run_connection(self, state: _ConnectionState):
        delay = self._INITIAL_DELAY
        while state.should_reconnect:
            ws_app = self._create_websocket_app(state)

            with state.lock:
                state.ws_app = ws_app

            try:
                ws_app.run_forever(
                    sslopt=self._sslopt,
                    ping_interval=self._PING_INTERVAL,
                    ping_timeout=self._PING_TIMEOUT,
                )
            except Exception as exc:
                logging.exception(
                    "Unexpected error during websocket run for stream %s: %s",
                    state.stream_name,
                    exc,
                )

            if not state.should_reconnect:
                break

            state.retries += 1
            if state.retries > self._MAX_RETRIES:
                logging.error(
                    "Max reconnect retries reached for stream %s",
                    state.stream_name,
                )
                self._invoke_callback(state, self._RECONNECT_ERROR_PAYLOAD)
                break

            delay = min(
                self._INITIAL_DELAY * (self._FACTOR ** (state.retries - 1)),
                self._MAX_DELAY,
            )
            logging.info(
                "Retrying connection for stream %s in %.2f seconds (attempt %d)",
                state.stream_name,
                delay,
                state.retries + 1,
            )
            time.sleep(delay)

        with state.lock:
            state.ws_app = None
        self._conns.pop(state.stream_name, None)

    def _create_websocket_app(self, state: _ConnectionState):
        def on_open(ws):
            state.retries = 0
            logging.info("Server connected")
            if state.payload:
                logging.info("Sending message to Server: %s", state.payload)
                ws.send(state.payload)

        def on_message(ws, message):
            if isinstance(message, bytes):
                try:
                    message = message.decode("utf8")
                except UnicodeDecodeError:
                    logging.warning("Failed to decode binary message from server")
                    return

            try:
                payload_obj = json.loads(message)
            except ValueError:
                logging.debug("Received non-JSON message: %s", message)
                return

            self._invoke_callback(state, payload_obj)

        def on_error(ws, error):
            if state.should_reconnect:
                logging.error(
                    "Can't connect to server. Reason: %s. Retrying: %d",
                    error,
                    state.retries + 1,
                )
            else:
                logging.info("WebSocket error after close request: %s", error)

        def on_close(ws, close_status_code, close_msg):
            logging.warning(
                "WebSocket connection closed: %s, code: %s, clean: %s, reason: %s",
                close_msg,
                close_status_code,
                close_status_code == 1000,
                close_msg,
            )
            # on_close is invoked after on_error, so retries are handled in _run_connection loop.

        return websocket.WebSocketApp(
            state.url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

    def _invoke_callback(self, state: _ConnectionState, payload):
        try:
            state.callback(payload)
        except Exception:
            logging.exception(
                "Error while executing callback for stream %s", state.stream_name
            )

    def stop_socket(self, conn_key):
        state = self._conns.get(conn_key)
        if not state:
            return

        state.should_reconnect = False
        with state.lock:
            ws_app = state.ws_app

        if ws_app is not None:
            try:
                ws_app.close()
            except Exception:
                logging.exception("Error while closing websocket for %s", conn_key)

        if state.thread and state.thread.is_alive():
            state.thread.join(timeout=5)

        self._conns.pop(conn_key, None)

    def close(self):
        keys = list(self._conns.keys())
        for key in keys:
            self.stop_socket(key)
