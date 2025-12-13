# WinkWatcher 🚀

WinkWatcher is an asynchronous Telegram alert bot that monitors the Rarible marketplace for the cheapest NFT per rarity in a specific Polygon collection, tracks floor prices in SQLite, and sends alerts when a listing drops by 50% or more below the current floor. All requests are fully async using `aiohttp`, `aiogram`, and `aiosqlite`.

## Features ✨
- Async polling every 10 seconds for rarities: Legendary, Epic, Rare, Uncommon, Common
- Floor price tracking in SQLite, initialized on startup and updated every 30 seconds
- Alerts to Telegram with image, rarity, price (USD), floor (USD), and links to Rarible and OpenSea
- IPFS image handling with preview and fallback to `ipfs.io`
- Clean logs via `loguru`, PEP8 formatted with `ruff`

## Requirements 🛠️
- Python 3.11
- Dependencies in `requirements.txt`
- Telegram Bot token and channel ID

## Installation ⚙️
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Environment Variables 🔐
Create a `.env` file in the project root:
```
BOT_TOKEN=your_telegram_bot_token
CHANNEL_ID=-100XXXXXXXXXX
```

## Run ▶️
```bash
python3 main.py
```

## Output 🔔
The bot sends a photo with HTML-formatted caption:
- Number, Rarity, Price (USD), Floor Price (USD)
- Rarible link and OpenSea link (clickable “View NFT”)
- Time in Europe/Warsaw (`HH:MM:SS`)

## Commands 🧭
- `/current` — shows current thresholds per rarity:
  - 🟨 Legendary -> 10%
  - 🟪 Epic -> 20%
  - 🟦 Rare -> 50%
  - 🟩 Uncommon -> 50%
  - ⬜️ Common -> 10%
- `/set Rarity, Percent` — updates threshold for a rarity, percent range `1..100`
  - Example: `/set Common, 10`
  - Supported rarities: `Legendary`, `Epic`, `Rare`, `Uncommon`, `Common`
  - Works in any chat where the bot is present

## Linting 🧹
```bash
ruff check . --fix
```

## Notes 📒
- Only public APIs are used; no API keys required for marketplace requests
- `.env` holds secrets; do not commit real tokens
- Floor and notification state are stored in `floors.db` and managed asynchronously

## License 📄
MIT License — see `LICENSE`
