# bybit_proxy.py — единый контролируемый прокси-транспорт для Bybit (HTTP REST + WS) с устойчивыми fallback

# 🔸 Импорты
import os
import ssl
import json
import base64
import socket
import asyncio
import logging
import urllib.parse
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple
from contextlib import asynccontextmanager

import httpx
import websockets

# 🔸 Логгер
log = logging.getLogger("BYBIT_PROXY")

# 🔸 ENV (управление прокси)
BYBIT_PROXY_ENABLED = (os.getenv("BYBIT_PROXY_ENABLED", "false").lower() == "true")
BYBIT_PROXY_TYPE = (os.getenv("BYBIT_PROXY_TYPE", "http").strip().lower() or "http")  # http | socks5
BYBIT_PROXY_HTTP_URL = (os.getenv("BYBIT_PROXY_HTTP_URL", "") or "").strip()
BYBIT_PROXY_SOCKS5_URL = (os.getenv("BYBIT_PROXY_SOCKS5_URL", "") or "").strip()
BYBIT_PROXY_EXPORT_ENV = (os.getenv("BYBIT_PROXY_EXPORT_ENV", "false").lower() == "true")

# 🔸 Fallback на env QuotaGuard (если BYBIT_PROXY_HTTP_URL не задан)
QUOTAGUARD_HTTP_FALLBACK_KEYS = ("QUOTAGUARDSTATIC_URL", "QUOTAGUARD_URL")

# 🔸 Таймауты туннеля (сек)
TUNNEL_CONNECT_TIMEOUT_SEC = float(os.getenv("BYBIT_PROXY_TUNNEL_TIMEOUT", "8"))


# 🔸 Модель конфигурации прокси
@dataclass(frozen=True)
class ProxyConfig:
    enabled: bool
    proxy_type: str           # http | socks5
    proxy_url: Optional[str]  # полный URL (с кредами), или None
    export_env: bool


# 🔸 Публичный API: получить актуальную конфигурацию прокси
def get_proxy_config() -> ProxyConfig:
    proxy_type = BYBIT_PROXY_TYPE if BYBIT_PROXY_TYPE in ("http", "socks5") else "http"
    proxy_url = _resolve_proxy_url(proxy_type)
    enabled = bool(BYBIT_PROXY_ENABLED and proxy_url)

    # если включатель true, но URL пуст — считаем прокси выключенным (устойчивый фолбэк)
    return ProxyConfig(
        enabled=enabled,
        proxy_type=proxy_type,
        proxy_url=proxy_url if enabled else None,
        export_env=bool(BYBIT_PROXY_EXPORT_ENV),
    )


# 🔸 Публичный API: инициализация прокси (логи + опциональный экспорт в стандартные ENV)
def init_bybit_proxy() -> ProxyConfig:
    cfg = get_proxy_config()

    # суммарный лог (один раз на старт процесса)
    log.info(
        "BYBIT_PROXY init: enabled=%s type=%s export_env=%s url=%s",
        "true" if cfg.enabled else "false",
        cfg.proxy_type,
        "true" if cfg.export_env else "false",
        _mask_proxy_url(cfg.proxy_url) if cfg.proxy_url else "<none>",
    )

    # экспорт в ENV (если включено)
    if cfg.enabled and cfg.export_env:
        _apply_proxy_env(cfg)

    return cfg


# 🔸 Публичный API: kwargs для httpx.AsyncClient (контролируемо: trust_env=False)
def httpx_client_kwargs(timeout: float = 10.0) -> Dict[str, Any]:
    cfg = get_proxy_config()

    # контролируемый режим: по умолчанию не читаем внешние proxy env
    kwargs: Dict[str, Any] = {
        "timeout": timeout,
        "trust_env": False,
    }

    # прокси для REST
    if cfg.enabled and cfg.proxy_url:
        # socks5 в httpx требует socksio; если нет — fallback на http-прокси (если он есть)
        if cfg.proxy_type == "socks5" and not _httpx_socks_supported():
            http_fallback = _resolve_proxy_url("http")
            if http_fallback:
                kwargs["proxy"] = http_fallback
                log.info("BYBIT_PROXY: socks5 для httpx недоступен (нет socksio) → fallback на http proxy")
            else:
                log.info("BYBIT_PROXY: socks5 для httpx недоступен (нет socksio) → REST будет без прокси")
        else:
            # httpx 0.28+ использует параметр proxy
            kwargs["proxy"] = cfg.proxy_url

    return kwargs


# 🔸 Публичный API: контекстный менеджер для httpx.AsyncClient
@asynccontextmanager
async def httpx_async_client(timeout: float = 10.0, **overrides: Any):
    base_kwargs = httpx_client_kwargs(timeout=timeout)
    base_kwargs.update(overrides or {})
    async with httpx.AsyncClient(**base_kwargs) as client:
        yield client


# 🔸 Публичный API: WebSocket connect с прокси (устойчивый fallback через туннель)
def ws_connect(uri: str, **ws_kwargs: Any):
    cfg = get_proxy_config()

    # прокси выключен → обычный connect
    if not cfg.enabled or not cfg.proxy_url:
        return websockets.connect(uri, **ws_kwargs)

    # через туннель (HTTP CONNECT или SOCKS5) → дальше websockets сам делает TLS для wss://
    tunnel_type = "socks5" if cfg.proxy_type == "socks5" else "http"
    return _WsConnectViaTunnel(uri, proxy_url=cfg.proxy_url, tunnel_type=tunnel_type, ws_kwargs=ws_kwargs)


# 🔸 Публичный API: urllib opener (если нужно точечно для urllib.request)
def urllib_opener():
    import urllib.request

    cfg = get_proxy_config()
    if not cfg.enabled or not cfg.proxy_url:
        return urllib.request.build_opener()

    # urllib поддерживает http/https прокси через ProxyHandler
    if cfg.proxy_type != "http":
        # для socks5 в urllib нужен внешний handler; устойчивый fallback — без прокси
        log.info("BYBIT_PROXY: urllib_opener: socks5 не поддержан → urllib пойдёт без прокси")
        return urllib.request.build_opener()

    proxy_map = {"http": cfg.proxy_url, "https": cfg.proxy_url}
    handler = urllib.request.ProxyHandler(proxy_map)
    return urllib.request.build_opener(handler)


# 🔸 Внутреннее: выбрать URL прокси из ENV (и фолбэков)
def _resolve_proxy_url(proxy_type: str) -> Optional[str]:
    # proxy_type: http | socks5
    if proxy_type == "socks5":
        if BYBIT_PROXY_SOCKS5_URL:
            return BYBIT_PROXY_SOCKS5_URL
        # fallback: если socks5 не задан, но есть http — можно использовать http (устойчивость)
        if BYBIT_PROXY_HTTP_URL:
            return BYBIT_PROXY_HTTP_URL
        # fallback: попытка взять из QuotaGuard
        for k in QUOTAGUARD_HTTP_FALLBACK_KEYS:
            v = (os.getenv(k, "") or "").strip()
            if v:
                return v
        return None

    # http proxy
    if BYBIT_PROXY_HTTP_URL:
        return BYBIT_PROXY_HTTP_URL

    # fallback: QuotaGuard
    for k in QUOTAGUARD_HTTP_FALLBACK_KEYS:
        v = (os.getenv(k, "") or "").strip()
        if v:
            return v

    return None


# 🔸 Внутреннее: экспорт прокси в стандартные переменные окружения
def _apply_proxy_env(cfg: ProxyConfig) -> None:
    if not cfg.proxy_url:
        return

    # условия достаточности
    url = cfg.proxy_url

    if cfg.proxy_type == "http":
        os.environ["HTTP_PROXY"] = url
        os.environ["HTTPS_PROXY"] = url
    else:
        # для socks чаще используют ALL_PROXY
        os.environ["ALL_PROXY"] = url

    log.info(
        "BYBIT_PROXY exported to env: type=%s http_proxy=%s https_proxy=%s all_proxy=%s",
        cfg.proxy_type,
        "set" if os.getenv("HTTP_PROXY") else "none",
        "set" if os.getenv("HTTPS_PROXY") else "none",
        "set" if os.getenv("ALL_PROXY") else "none",
    )


# 🔸 Внутреннее: проверка поддержки socks5 в httpx (нужен socksio)
def _httpx_socks_supported() -> bool:
    try:
        import socksio  # noqa: F401
        return True
    except Exception:
        return False


# 🔸 Внутреннее: маскирование пароля в URL для логов
def _mask_proxy_url(url: Optional[str]) -> str:
    if not url:
        return "<none>"
    try:
        p = urllib.parse.urlparse(url)
        if not p.hostname:
            return "<invalid>"
        user = p.username or ""
        # пароль не показываем
        auth = f"{user}:***@" if user else ""
        port = f":{p.port}" if p.port else ""
        scheme = p.scheme or "http"
        return f"{scheme}://{auth}{p.hostname}{port}"
    except Exception:
        return "<invalid>"


# 🔸 Внутреннее: WS connect через готовый TCP-туннель (HTTP CONNECT / SOCKS5)
class _WsConnectViaTunnel:
    def __init__(self, uri: str, proxy_url: str, tunnel_type: str, ws_kwargs: Dict[str, Any]):
        self._uri = uri
        self._proxy_url = proxy_url
        self._tunnel_type = tunnel_type  # http | socks5
        self._ws_kwargs = dict(ws_kwargs or {})

        self._sock: Optional[socket.socket] = None
        self._inner_cm = None
        self._ws = None

    async def __aenter__(self):
        # создаём туннельный сокет в executor, чтобы не блокировать event loop
        loop = asyncio.get_running_loop()
        self._sock = await loop.run_in_executor(None, self._build_tunnel_socket_blocking)

        # условия достаточности
        if not self._sock:
            raise ConnectionError("tunnel socket not created")

        # для wss:// важно дать ssl context; websockets создаст дефолтный, если ssl не задан
        # но при sock=... лучше передать ssl явно для устойчивости
        ssl_ctx = None
        try:
            parsed = urllib.parse.urlparse(self._uri)
            if parsed.scheme == "wss":
                ssl_ctx = ssl.create_default_context()
        except Exception:
            ssl_ctx = None

        # создаём внутренний connect с готовым sock
        if ssl_ctx is not None:
            self._inner_cm = websockets.connect(self._uri, sock=self._sock, ssl=ssl_ctx, **self._ws_kwargs)
        else:
            self._inner_cm = websockets.connect(self._uri, sock=self._sock, **self._ws_kwargs)

        self._ws = await self._inner_cm.__aenter__()
        return self._ws

    async def __aexit__(self, exc_type, exc, tb):
        # закрываем websocket (если поднят)
        try:
            if self._inner_cm is not None:
                await self._inner_cm.__aexit__(exc_type, exc, tb)
        finally:
            # гарантированно закрываем сырой сокет (на всякий случай)
            try:
                if self._sock is not None:
                    self._sock.close()
            except Exception:
                pass
        return False

    def _build_tunnel_socket_blocking(self) -> socket.socket:
        # распарсить цель из ws uri
        target_host, target_port = _parse_ws_target(self._uri)

        # распарсить прокси
        p = urllib.parse.urlparse(self._proxy_url)
        proxy_host = p.hostname
        proxy_port = p.port or (443 if (p.scheme or "").lower() == "https" else 80)
        proxy_user = urllib.parse.unquote(p.username) if p.username else None
        proxy_pass = urllib.parse.unquote(p.password) if p.password else None

        if not proxy_host:
            raise ValueError("proxy_url missing hostname")

        # соединиться с прокси
        s = socket.create_connection((proxy_host, proxy_port), timeout=TUNNEL_CONNECT_TIMEOUT_SEC)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        # установить туннель
        if self._tunnel_type == "socks5":
            _socks5_handshake(
                sock=s,
                target_host=target_host,
                target_port=target_port,
                username=proxy_user,
                password=proxy_pass,
            )
        else:
            _http_connect_handshake(
                sock=s,
                target_host=target_host,
                target_port=target_port,
                username=proxy_user,
                password=proxy_pass,
            )

        # перевести в non-blocking для asyncio
        s.setblocking(False)
        return s


# 🔸 Внутреннее: разобрать хост/порт из ws:// или wss:// URI
def _parse_ws_target(uri: str) -> Tuple[str, int]:
    p = urllib.parse.urlparse(uri)
    host = p.hostname
    if not host:
        raise ValueError("ws uri missing hostname")
    if p.port:
        port = int(p.port)
    else:
        port = 443 if p.scheme == "wss" else 80
    return host, port


# 🔸 Внутреннее: HTTP CONNECT туннель
def _http_connect_handshake(
    *,
    sock: socket.socket,
    target_host: str,
    target_port: int,
    username: Optional[str],
    password: Optional[str],
) -> None:
    # собрать заголовки CONNECT
    auth_header = ""
    if username is not None and password is not None:
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        auth_header = f"Proxy-Authorization: Basic {token}\r\n"

    req = (
        f"CONNECT {target_host}:{target_port} HTTP/1.1\r\n"
        f"Host: {target_host}:{target_port}\r\n"
        f"{auth_header}"
        f"Proxy-Connection: keep-alive\r\n"
        f"Connection: keep-alive\r\n"
        f"\r\n"
    ).encode("utf-8")

    sock.sendall(req)

    # прочитать ответ до конца заголовков
    buf = b""
    sock.settimeout(TUNNEL_CONNECT_TIMEOUT_SEC)
    try:
        while b"\r\n\r\n" not in buf:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buf += chunk
            if len(buf) > 64 * 1024:
                break
    finally:
        sock.settimeout(None)

    head = buf.split(b"\r\n\r\n", 1)[0].decode("utf-8", "ignore")
    first = head.split("\r\n", 1)[0].strip()
    # ожидаем HTTP/1.1 200 Connection established
    if " 200 " not in first and not first.endswith(" 200"):
        raise ConnectionError(f"proxy CONNECT failed: {first}")


# 🔸 Внутреннее: SOCKS5 туннель (с поддержкой user/pass)
def _socks5_handshake(
    *,
    sock: socket.socket,
    target_host: str,
    target_port: int,
    username: Optional[str],
    password: Optional[str],
) -> None:
    sock.settimeout(TUNNEL_CONNECT_TIMEOUT_SEC)
    try:
        # greeting: methods
        if username is not None and password is not None:
            # 0x02 = username/password
            sock.sendall(b"\x05\x01\x02")
        else:
            # 0x00 = no auth
            sock.sendall(b"\x05\x01\x00")

        ver_method = sock.recv(2)
        if len(ver_method) != 2 or ver_method[0] != 0x05:
            raise ConnectionError("SOCKS5: bad method response")
        method = ver_method[1]
        if method == 0xFF:
            raise ConnectionError("SOCKS5: no acceptable auth method")

        # username/password auth
        if method == 0x02:
            u = (username or "").encode("utf-8")
            p = (password or "").encode("utf-8")
            if len(u) > 255 or len(p) > 255:
                raise ValueError("SOCKS5: username/password too long")
            sock.sendall(b"\x01" + bytes([len(u)]) + u + bytes([len(p)]) + p)
            auth_resp = sock.recv(2)
            if len(auth_resp) != 2 or auth_resp[0] != 0x01 or auth_resp[1] != 0x00:
                raise ConnectionError("SOCKS5: auth failed")

        # CONNECT request
        host_bytes = target_host.encode("utf-8")
        if len(host_bytes) > 255:
            raise ValueError("SOCKS5: hostname too long")

        port_bytes = int(target_port).to_bytes(2, "big")
        req = b"\x05\x01\x00" + b"\x03" + bytes([len(host_bytes)]) + host_bytes + port_bytes
        sock.sendall(req)

        # reply: VER REP RSV ATYP ...
        resp = sock.recv(4)
        if len(resp) != 4 or resp[0] != 0x05:
            raise ConnectionError("SOCKS5: bad connect response")
        rep = resp[1]
        atyp = resp[3]
        if rep != 0x00:
            raise ConnectionError(f"SOCKS5: connect failed rep={rep}")

        # consume BND.ADDR and BND.PORT
        if atyp == 0x01:
            sock.recv(4)
        elif atyp == 0x03:
            ln = sock.recv(1)[0]
            sock.recv(ln)
        elif atyp == 0x04:
            sock.recv(16)
        else:
            raise ConnectionError("SOCKS5: unknown ATYP")
        sock.recv(2)

    finally:
        sock.settimeout(None)