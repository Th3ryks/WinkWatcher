import asyncio
from typing import Any, Dict, List, Optional, Tuple

import sys
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from loguru import logger
import aiohttp
from dotenv import load_dotenv
import aiosqlite
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import BufferedInputFile
from aiogram import types

logger.remove()
logger.add(
    sys.stdout,
    format="| <magenta>{time:YYYY-MM-DD HH:mm:ss}</magenta> | <cyan><level>{level: <8}</level></cyan> | {message}",
    level="INFO",
    colorize=True,
)
logger.add(
    "bot.log",
    format="| {time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
    level="SUCCESS",
    rotation="10 MB",
    retention="7 days",
    compression="zip",
)



def _build_headers() -> Dict[str, str]:
    return {"Accept": "application/json"}


async def fetch_json(
    session: aiohttp.ClientSession, url: str, params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    for attempt in range(3):
        try:
            async with session.get(url, params=params, headers=_build_headers()) as resp:
                if resp.status == 200:
                    return await resp.json()
                text = await resp.text()
                logger.warning(f"Non-200 response {resp.status} for {url}: {text}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"Request error on attempt {attempt + 1} for {url}: {e}")
        await asyncio.sleep(1.0 * (attempt + 1))
    return {}

async def post_json(
    session: aiohttp.ClientSession, url: str, payload: Dict[str, Any]
) -> Any:
    for attempt in range(3):
        try:
            async with session.post(
                url,
                json=payload,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "Origin": "https://og.rarible.com",
                    "Referer": "https://og.rarible.com/winkdiscover/items",
                },
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                text = await resp.text()
                logger.warning(f"Non-200 response {resp.status} for {url}: {text}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"Request error on attempt {attempt + 1} for {url}: {e}")
        await asyncio.sleep(1.0 * (attempt + 1))
    return {}

async def extract_image_url(item: Dict[str, Any]) -> Optional[str]:
    props = item.get("properties") or {}
    entries: List[Dict[str, Any]] = props.get("mediaEntries") or []
    preferred_order = ("ORIGINAL", "BIG", "PREVIEW")
    if isinstance(entries, list) and entries:
        for rep in preferred_order:
            for m in entries:
                if (m.get("contentType") or "").upper() == "IMAGE" and (m.get("sizeType") or "").upper() == rep and m.get("url"):
                    return _normalize_ipfs(str(m.get("url")))
        for m in entries:
            u = m.get("url")
            if isinstance(u, str) and u:
                return _normalize_ipfs(u)
    meta = item.get("meta") or {}
    contents: List[Dict[str, Any]] = meta.get("content") or []
    image_contents = [c for c in contents if (c.get("@type") or "").upper() == "IMAGE"]
    for rep in ("ORIGINAL", "BIG", "PREVIEW", "PORTRAIT"):
        for c in image_contents:
            if (c.get("representation") or "").upper() == rep and c.get("url"):
                return _normalize_ipfs(str(c.get("url")))
    for c in contents:
        if c.get("url"):
            return _normalize_ipfs(str(c.get("url")))
    uri = meta.get("originalMetaUri") or item.get("image") or item.get("preview")
    return _normalize_ipfs(uri) if isinstance(uri, str) else None


async def extract_name(item: Dict[str, Any]) -> Optional[str]:
    props = item.get("properties") or {}
    meta = item.get("meta") or {}
    name = props.get("name") or meta.get("name") or item.get("name") or item.get("title")
    return name if isinstance(name, str) else None


def _safe_decimal_str(value: Any, decimals: int = 18) -> Optional[str]:
    try:
        raw = int(str(value))
    except Exception:
        return None
    if decimals <= 0:
        return str(raw)
    s = str(raw).rjust(decimals + 1, "0")
    whole = s[:-decimals] or "0"
    frac = s[-decimals:].rstrip("0")
    return whole if not frac else f"{whole}.{frac}"


async def extract_price(item: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    order = item.get("bestSellOrder") or item.get("bestSell") or {}
    ownership = item.get("ownership") or {}
    if not order and ownership:
        p = ownership.get("price")
        symbol = None
        last = item.get("lastSellPrice") or {}
        currency = last.get("currency") or {}
        sym = currency.get("symbol")
        if isinstance(sym, str):
            symbol = sym
        return (str(p) if p is not None else None), symbol
    if not order:
        return None, None
    price = order.get("price") or order.get("takePrice") or order.get("makePrice")
    currency_symbol: Optional[str] = None
    take = order.get("take") or {}
    asset_type = take.get("assetType") or {}
    asset_class = (asset_type.get("assetClass") or "").upper()
    if asset_class in ("ETH", "NATIVE"):
        currency_symbol = "MATIC" if (item.get("blockchain") or "").upper() == "POLYGON" else "ETH"
    elif asset_class == "ERC20":
        currency_symbol = "ERC20"
    else:
        currency_symbol = asset_class or None
    if isinstance(price, str):
        return price, currency_symbol
    raw_value = take.get("value")
    dec = 18
    normalized = _safe_decimal_str(raw_value, decimals=dec)
    return normalized, currency_symbol

def _normalize_ipfs(uri: str) -> str:
    s = uri.strip().strip("`").strip()
    if s.startswith("ipfs://"):
        return f"https://ipfs.io/ipfs/{s.removeprefix('ipfs://')}"
    return s

async def fetch_metadata(session: aiohttp.ClientSession, uri: Optional[str]) -> Dict[str, Any]:
    if not uri or not isinstance(uri, str):
        return {}
    url = _normalize_ipfs(uri)
    try:
        async with session.get(
            url,
            headers={"Accept": "application/json"},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            if resp.status == 200:
                return await resp.json()
            return {}
    except asyncio.CancelledError:
        raise
    except Exception:
        return {}

async def extract_from_metadata(session: aiohttp.ClientSession, item: Dict[str, Any]) -> Dict[str, Optional[str]]:
    meta_block = item.get("meta") or {}
    metadata_uri = meta_block.get("metadataUri")
    data = await fetch_metadata(session, metadata_uri)
    name = None
    image = None
    rarity = None
    media_entries = data.get("mediaEntries") or []
    if isinstance(media_entries, list) and media_entries:
        preferred = ("ORIGINAL", "BIG", "PREVIEW")
        for size in preferred:
            for m in media_entries:
                if (m.get("contentType") or "").upper() == "IMAGE" and (m.get("sizeType") or "").upper() == size and m.get("url"):
                    image = _normalize_ipfs(str(m.get("url")))
                    break
            if image:
                break
    for key in ("name", "title"):
        v = data.get(key)
        if isinstance(v, str):
            name = v
            break
    for key in ("image", "image_url", "imageURI"):
        v = data.get(key)
        if isinstance(v, str):
            image = _normalize_ipfs(v)
            break
    attrs = data.get("attributes") or []
    if isinstance(attrs, list):
        for a in attrs:
            if (a.get("key") or a.get("trait_type") or "").lower() == "rarity":
                rv = a.get("value")
                if isinstance(rv, str):
                    rarity = rv
                break
    return {"name": name, "image_url": image, "rarity": rarity}

def extract_preview_url(item: Dict[str, Any], meta_extracted: Dict[str, Optional[str]]) -> Optional[str]:
    props = item.get("properties") or {}
    entries: List[Dict[str, Any]] = props.get("mediaEntries") or []
    if isinstance(entries, list):
        for m in entries:
            if (m.get("contentType") or "").upper() == "IMAGE" and (m.get("sizeType") or "").upper() == "PREVIEW" and m.get("url"):
                return _normalize_ipfs(str(m.get("url")))
    preview = meta_extracted.get("image_url")
    return preview
def extract_rarity(item: Dict[str, Any], meta_rarity: Optional[str]) -> Optional[str]:
    props = item.get("properties") or {}
    attrs = props.get("attributes") or []
    if isinstance(attrs, list):
        for a in attrs:
            key = a.get("key")
            if isinstance(key, str) and key.lower() == "rarity":
                rv = a.get("value")
                if isinstance(rv, str):
                    return rv
                break
    return meta_rarity


async def get_items_by_collection(
    session: aiohttp.ClientSession, base: str, collection: str, page_size: int = 100, max_pages: int = 20
) -> List[Dict[str, Any]]:
    url = f"{base}/v0.1/items/byCollection"
    items: List[Dict[str, Any]] = []
    continuation: Optional[str] = None
    for _ in range(max_pages):
        params: Dict[str, Any] = {"collection": collection, "size": page_size}
        if continuation:
            params["continuation"] = continuation
        data = await fetch_json(session, url, params=params)
        new_items = data.get("items") or []
        items.extend(new_items)
        continuation = data.get("continuation")
        if not continuation or not new_items:
            break
    return items

async def search_items_marketplace(
    session: aiohttp.ClientSession, base: str, collection: str, page_size: int = 100, max_pages: int = 20
) -> List[Dict[str, Any]]:
    url = f"{base}/items/search"
    items: List[Dict[str, Any]] = []
    continuation: Optional[str] = None
    for _ in range(max_pages):
        payload: Dict[str, Any] = {
            "size": page_size,
            "continuation": continuation,
            "filter": {
                "verifiedOnly": False,
                "sort": "LOW_PRICE_FIRST",
                "collections": [collection],
                "blockchains": ["ETHEREUM","MOONBEAM","ETHERLINK","POLYGON","BASE","RARI","ZKSYNC","APTOS","GOAT","SHAPE","TELOS","MATCH","ARBITRUM","ABSTRACT","HEDERAEVM","VICTION","ZKCANDY"],
                "hideItemsSupply": "HIDE_LAZY_SUPPLY",
                "nsfw": True,
                "hasMetaContentOnly": False,
            },
        }
        data = await post_json(session, url, payload)
        if isinstance(data, list):
            new_items = data
            continuation = None
        else:
            new_items = data.get("items") or []
            continuation = data.get("continuation")
        items.extend(new_items)
        if not continuation or not new_items:
            break
    return items

async def search_cheapest_by_rarity(
    session: aiohttp.ClientSession, base: str, collection: str, rarity: str
) -> List[Dict[str, Any]]:
    url = f"{base}/items/search"
    payload: Dict[str, Any] = {
        "size": 1,
        "filter": {
            "verifiedOnly": False,
            "sort": "LOW_PRICE_FIRST",
            "collections": [collection],
            "blockchains": ["ETHEREUM","MOONBEAM","ETHERLINK","POLYGON","BASE","RARI","ZKSYNC","APTOS","GOAT","SHAPE","TELOS","MATCH","ARBITRUM","ABSTRACT","HEDERAEVM","VICTION","ZKCANDY"],
            "hideItemsSupply": "HIDE_LAZY_SUPPLY",
            "nsfw": True,
            "hasMetaContentOnly": False,
            "traits": [{"key": "Rarity", "values": [rarity]}],
        },
    }
    data = await post_json(session, url, payload)
    if isinstance(data, list):
        return data[:1]
    return (data.get("items") or [])[:1]

async def init_db(conn: aiosqlite.Connection) -> None:
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS floors (rarity TEXT PRIMARY KEY, price REAL, updated_at TEXT)"
    )
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS notifications (item_id TEXT PRIMARY KEY, last_price REAL, last_at TEXT)"
    )
    await conn.commit()

async def ensure_threshold_column(conn: aiosqlite.Connection) -> None:
    async with conn.execute("PRAGMA table_info(floors)") as cur:
        cols = await cur.fetchall()
        names = [c[1] for c in cols]
        if "threshold_percent" not in names:
            await conn.execute("ALTER TABLE floors ADD COLUMN threshold_percent REAL")
            await conn.commit()
    await conn.execute("UPDATE floors SET threshold_percent = COALESCE(threshold_percent, 50)")
    await conn.commit()

async def get_floor(conn: aiosqlite.Connection, rarity: str) -> Optional[float]:
    async with conn.execute("SELECT price FROM floors WHERE rarity = ?", (rarity,)) as cur:
        row = await cur.fetchone()
        if not row:
            return None
        v = row[0]
        try:
            return float(v)
        except Exception:
            return None

async def set_floor(conn: aiosqlite.Connection, rarity: str, price: float) -> None:
    ts = datetime.now(ZoneInfo("Europe/Warsaw")).strftime("%d.%m.%Y %H:%M:%S")
    await conn.execute(
        "INSERT INTO floors(rarity, price, updated_at) VALUES(?, ?, ?) ON CONFLICT(rarity) DO UPDATE SET price=excluded.price, updated_at=excluded.updated_at",
        (rarity, round(price, 2), ts),
    )
    await conn.commit()

async def get_notified(conn: aiosqlite.Connection, item_id: str) -> Optional[float]:
    async with conn.execute("SELECT last_price FROM notifications WHERE item_id = ?", (item_id,)) as cur:
        row = await cur.fetchone()
        if not row:
            return None
        v = row[0]
        try:
            return float(v)
        except Exception:
            return None

async def set_notified(conn: aiosqlite.Connection, item_id: str, price: float) -> None:
    ts = datetime.now(ZoneInfo("Europe/Warsaw")).strftime("%d.%m.%Y %H:%M:%S")
    await conn.execute(
        "INSERT INTO notifications(item_id, last_price, last_at) VALUES(?, ?, ?) ON CONFLICT(item_id) DO UPDATE SET last_price=excluded.last_price, last_at=excluded.last_at",
        (item_id, round(price, 2), ts),
    )
    await conn.commit()

async def get_threshold(conn: aiosqlite.Connection, rarity: str) -> float:
    async with conn.execute("SELECT threshold_percent FROM floors WHERE rarity = ?", (rarity,)) as cur:
        row = await cur.fetchone()
        if not row or row[0] is None:
            return 50.0
        try:
            return float(row[0])
        except Exception:
            return 50.0

async def set_threshold(conn: aiosqlite.Connection, rarity: str, percent: float) -> None:
    await conn.execute(
        "INSERT INTO floors(rarity, price, updated_at, threshold_percent) VALUES(?, ?, ?, ?) ON CONFLICT(rarity) DO UPDATE SET threshold_percent=excluded.threshold_percent",
        (rarity, None, "", percent),
    )
    await conn.commit()

def _parse_price(price_str: Optional[str]) -> Optional[float]:
    if price_str is None:
        return None
    try:
        return float(price_str)
    except Exception:
        return None

async def get_eth_usdt_rate(session: aiohttp.ClientSession) -> Optional[float]:
    try:
        async with session.get("https://api.binance.com/api/v3/ticker/price", params={"symbol": "ETHUSDT"}, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status == 200:
                data = await resp.json()
                p = data.get("price")
                if isinstance(p, str):
                    return float(p)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.exception(f"Rate fetch error: {e}")
    return None

def _format_caption(token_id: Optional[str], rarity: Optional[str], price: Optional[str], floor_price: Optional[float], rarible_url: Optional[str], opensea_url: Optional[str]) -> str:
    tz = ZoneInfo("Europe/Warsaw")
    now = datetime.now(tz).strftime("%H:%M:%S")
    num = token_id or ""
    rar = rarity or ""
    pr = price or ""
    fp = "" if floor_price is None else f"{floor_price:.2f} USD"
    rurl = rarible_url or ""
    ourl = opensea_url or ""
    return (
        f"🔢 <b>Number:</b> {num}\n"
        f"🎰 <b>Rarity:</b> {rar}\n"
        f"💰 <b>Price:</b> {pr}\n"
        f"📊 <b>Floor Price:</b> {fp}\n"
        f"🔗 <b>Rarible link:</b> <a href=\"{rurl}\">View NFT</a>\n"
        f"🔗 <b>OpenSea link:</b> <a href=\"{ourl}\">View NFT</a>\n\n"
        f"🕒 <b>Time:</b> {now}"
    )


async def run() -> None:
    collection_hyphen = "POLYGON-0xd8156606d2bf60c12d55f561395d29ba3c5ccc63"

    load_dotenv()
    marketplace_base = "https://og.rarible.com/marketplace/api/v4"
    bot_token = os.getenv("BOT_TOKEN")
    channel_id = os.getenv("CHANNEL_ID")
    channel_id_int: Optional[int] = None
    try:
        if channel_id and channel_id.startswith("-"):
            channel_id_int = int(channel_id)
    except Exception:
        channel_id_int = None

    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=16, enable_cleanup_closed=True)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        bot: Optional[Bot] = None
        if bot_token:
            bot = Bot(token=bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        conn = await aiosqlite.connect("floors.db")
        await init_db(conn)
        await ensure_threshold_column(conn)
        rarities = ["Legendary", "Epic", "Rare", "Uncommon", "Common"]
        dp: Optional[Dispatcher] = None
        router: Optional[Router] = None
        if bot:
            dp = Dispatcher()
            router = Router()
            async def _handle_set(msg: types.Message) -> None:
                if not msg.text:
                    return
                txt = msg.text.strip()
                chat_ok = True
                if channel_id and channel_id.startswith("@") and msg.chat and msg.chat.username and ("@" + msg.chat.username) == channel_id:
                    chat_ok = True
                if channel_id_int is not None and msg.chat and msg.chat.id == channel_id_int:
                    chat_ok = True
                if not chat_ok:
                    if bot:
                        await bot.send_message(chat_id=msg.chat.id, text="Command is only available in the specified channel")
                    return
                try:
                    payload = txt[4:].strip()
                    if not payload:
                        await bot.send_message(chat_id=msg.chat.id, text="Use the format: /set Rarity, Percent")
                        return
                    parts = [p.strip() for p in payload.split(",") if p.strip()]
                    if len(parts) != 2:
                        parts = [p for p in payload.split() if p]
                    if len(parts) != 2:
                        await bot.send_message(chat_id=msg.chat.id, text="Use the format: /set Rarity, Percent")
                        return
                    rarity_in = parts[0].lower()
                    percent_in = parts[1]
                    mapping = {r.lower(): r for r in rarities}
                    rarity_norm = mapping.get(rarity_in)
                    if not rarity_norm:
                        await bot.send_message(chat_id=msg.chat.id, text="Rarity must be one of: Legendary, Epic, Rare, Uncommon, Common")
                        return
                    try:
                        percent_val = float(percent_in)
                    except Exception:
                        await bot.send_message(chat_id=msg.chat.id, text="Percent must be a number between 1 and 100")
                        return
                    if not (0 < percent_val <= 100):
                        await bot.send_message(chat_id=msg.chat.id, text="Percent must be a number between 1 and 100")
                        return
                    await set_threshold(conn, rarity_norm, percent_val)
                    await bot.send_message(chat_id=msg.chat.id, text=f"Threshold updated for {rarity_norm}: {round(percent_val, 2):.2f}%")
                except Exception:
                    logger.info("Set command error")
                    if bot:
                        await bot.send_message(chat_id=msg.chat.id, text="Use the format: /set Rarity, Percent")
            router.message.register(_handle_set, Command("set"))
            router.channel_post.register(_handle_set, Command("set"))
            async def _handle_current(msg: types.Message) -> None:
                if not msg.text:
                    return
                logger.info("Received /current request")
                chat_ok = True
                if channel_id and channel_id.startswith("@") and msg.chat and msg.chat.username and ("@" + msg.chat.username) == channel_id:
                    chat_ok = True
                if channel_id_int is not None and msg.chat and msg.chat.id == channel_id_int:
                    chat_ok = True
                if not chat_ok:
                    if bot:
                        await bot.send_message(chat_id=msg.chat.id, text="Command is only available in the specified channel")
                    return
                try:
                    emojis = {
                        "Legendary": "🟨",
                        "Epic": "🟪",
                        "Rare": "🟦",
                        "Uncommon": "🟩",
                        "Common": "⬜️",
                    }
                    lines = []
                    for rname in rarities:
                        thv = await get_threshold(conn, rname)
                        em = emojis.get(rname, "")
                        lines.append(f"{em} {rname} -> {round(thv, 2):.2f}%")
                    text = "\n".join(lines)
                    await bot.send_message(chat_id=msg.chat.id, text=text)
                    logger.info("Current thresholds sent")
                except Exception:
                    logger.info("Failed to fetch current thresholds")
                    if bot:
                        await bot.send_message(chat_id=msg.chat.id, text="Failed to fetch current thresholds")
            router.message.register(_handle_current, Command("current"))
            router.channel_post.register(_handle_current, Command("current"))
            dp.include_router(router)
            asyncio.create_task(dp.start_polling(bot))

        async def _enrich(it: Dict[str, Any], rate: Optional[float]) -> Dict[str, Any]:
            image_url = await extract_image_url(it)
            meta_extracted = await extract_from_metadata(session, it)
            if not image_url:
                image_url = meta_extracted.get("image_url")
            preview_url = extract_preview_url(it, meta_extracted) or image_url
            price, currency = await extract_price(it)
            token_id = it.get("tokenId")
            item_id = it.get("id")
            rarible_url = f"https://og.rarible.com/token/{item_id}" if isinstance(item_id, str) else None
            opensea_url = f"https://opensea.io/item/pol/{collection_hyphen}/{token_id}" if isinstance(token_id, str) else None
            rarity = extract_rarity(it, meta_extracted.get("rarity"))
            price_val = _parse_price(price)
            price_usd: Optional[float] = None
            if rate is not None and price_val is not None:
                price_usd = price_val * rate
            return {
                "image_url": image_url,
                "preview_url": preview_url,
                "price": price,
                "currency": currency,
                "rarible_url": rarible_url,
                "opensea_url": opensea_url,
                "token_id": token_id,
                "item_id": item_id,
                "rarity": rarity,
                "price_usd": price_usd,
            }

        logger.info("Initializing floors and starting watcher")
        tick = 0
        while True:
            tick += 1
            rate = await get_eth_usdt_rate(session)
            if rate:
                logger.info(f"ETHUSDT rate: {rate:.2f}")
            else:
                logger.info("ETHUSDT rate unavailable")
            tasks = [search_cheapest_by_rarity(session, marketplace_base, collection_hyphen, r) for r in rarities]
            logger.info("Fetching cheapest items per rarity")
            per_rarity: List[List[Dict[str, Any]]] = await asyncio.gather(*tasks)
            items: List[Dict[str, Any]] = []
            for lst in per_rarity:
                items.extend(lst)
            logger.info(f"Items fetched: {len(items)}")
            results: List[Dict[str, Any]] = await asyncio.gather(*[ _enrich(it, rate) for it in items ])
            for r in results:
                rarity = r.get("rarity")
                price_usd = r.get("price_usd")
                price_str = f"{price_usd:.2f} USD" if isinstance(price_usd, float) else r.get("price")
                floor_price = await get_floor(conn, rarity) if isinstance(rarity, str) else None
                th = await get_threshold(conn, rarity) if isinstance(rarity, str) else 50.0
                if isinstance(price_usd, float) and isinstance(floor_price, float):
                    price_cmp = round(price_usd, 2)
                    limit_cmp = round(floor_price * (1 - th / 100.0), 2)
                    logger.info(f"Compare: rarity {rarity} price {price_cmp:.2f} <= limit {limit_cmp:.2f} ({round(th,2):.2f}%)")
                if price_usd is not None and floor_price is not None and round(price_usd, 2) <= round(floor_price * (1 - th / 100.0), 2):
                    logger.info(f"Trigger: rarity {rarity} price_usd {round(price_usd, 2):.2f} floor {round(floor_price, 2):.2f} ({round(th,2):.2f}%)")
                    last_notified_price = None
                    if isinstance(r.get("item_id"), str):
                        last_notified_price = await get_notified(conn, r.get("item_id"))
                    should_notify = last_notified_price is None or (isinstance(last_notified_price, float) and price_usd < last_notified_price)
                    if should_notify and bot and channel_id and r.get("image_url"):
                        logger.info(f"Sending telegram alert for {rarity} to {channel_id}")
                        caption = _format_caption(r.get("token_id"), rarity, price_str, floor_price, r.get("rarible_url"), r.get("opensea_url"))
                        img_url = _normalize_ipfs(r.get("preview_url") or r.get("image_url") or "")
                        try:
                            async with session.get(img_url, timeout=aiohttp.ClientTimeout(total=8)) as ir:
                                if ir.status == 200:
                                    content = await ir.read()
                                    fname = f"{rarity}_{r.get('token_id') or ''}.jpg"
                                    photo = BufferedInputFile(content, fname)
                                    await bot.send_photo(chat_id=channel_id, photo=photo, caption=caption)
                                else:
                                    alt = img_url
                                    if "ipfs.raribleuserdata.com/ipfs/" in img_url or img_url.startswith("ipfs://"):
                                        try:
                                            cid = img_url.split("ipfs/")[1] if "ipfs/" in img_url else img_url.removeprefix("ipfs://")
                                            alt = f"https://ipfs.io/ipfs/{cid}"
                                        except Exception:
                                            alt = img_url
                                    async with session.get(alt, timeout=aiohttp.ClientTimeout(total=8)) as ar:
                                        if ar.status == 200:
                                            content = await ar.read()
                                            fname = f"{rarity}_{r.get('token_id') or ''}.jpg"
                                            photo = BufferedInputFile(content, fname)
                                            await bot.send_photo(chat_id=channel_id, photo=photo, caption=caption)
                                        else:
                                            await bot.send_message(chat_id=channel_id, text=caption)
                            logger.info("Telegram message sent")
                            if isinstance(rarity, str) and isinstance(price_usd, float):
                                await set_floor(conn, rarity, price_usd)
                                logger.info(f"Floor updated immediately for {rarity}: {round(price_usd, 2):.2f} USD")
                            if isinstance(r.get("item_id"), str) and isinstance(price_usd, float):
                                await set_notified(conn, r.get("item_id"), price_usd)
                        except Exception:
                            logger.info("Telegram send failed")
                if tick == 1 and price_usd is not None and isinstance(rarity, str):
                    await set_floor(conn, rarity, price_usd)
                    logger.info(f"Floor initialized for {rarity}: {round(price_usd, 2):.2f} USD")
                if price_usd is not None and tick % 3 == 0 and isinstance(rarity, str):
                    await set_floor(conn, rarity, price_usd)
                    logger.info(f"Floor updated for {rarity}: {round(price_usd, 2):.2f} USD")
            await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(run())
