"""
Notifications async (Telegram uniquement) - Fire-and-forget pour ne pas bloquer les trades.
"""
import asyncio
import httpx
from typing import Optional
from loguru import logger
from pydantic import Field
from pydantic_settings import BaseSettings


class NotificationSettings(BaseSettings):
    """Configuration des notifications (Telegram)."""
    telegram_bot_token: str = Field(default="", env="TELEGRAM_BOT_TOKEN")
    telegram_chat_id: str = Field(default="", env="TELEGRAM_CHAT_ID")
    notifications_enabled: bool = Field(default=True, env="NOTIFICATIONS_ENABLED")
    
    class Config:
        env_file = ".env"
        extra = "ignore"


notif_settings = NotificationSettings()


def _chat_ids() -> list[str]:
    """List of chat_ids (multiple allowed, comma-separated in .env)."""
    raw = (notif_settings.telegram_chat_id or "").strip()
    if not raw:
        return []
    return [c.strip() for c in raw.split(",") if c.strip()]


# Reusable HTTP client (avoids connection overhead)
_http_client: Optional[httpx.AsyncClient] = None


async def _get_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None:
        _http_client = httpx.AsyncClient(timeout=10.0)
    return _http_client


async def _send_telegram(message: str) -> bool:
    """Send a Telegram message to every configured chat_id."""
    if not notif_settings.telegram_bot_token:
        return False
    chat_ids = _chat_ids()
    if not chat_ids:
        return False
    try:
        client = await _get_client()
        url = f"https://api.telegram.org/bot{notif_settings.telegram_bot_token}/sendMessage"
        ok = True
        for chat_id in chat_ids:
            try:
                resp = await client.post(url, json={
                    "chat_id": chat_id,
                    "text": message,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                })
                if resp.status_code != 200:
                    ok = False
            except Exception as e:
                logger.debug(f"Telegram send to {chat_id}: {e}")
                ok = False
        return ok
    except Exception as e:
        logger.debug(f"Telegram error: {e}")
        return False


def notify_trade(
    event_type: str,
    match: str,
    team: str,
    score: str,
    amount_usd: float,
    shares: float = 0,
    price: float = 0,
    pnl_pct: float = 0,
    reason: str = "",
) -> None:
    """
    Notifie un trade (BUY/SELL/TP/SL) - Fire-and-forget, ne bloque pas.
    
    Args:
        event_type: "BUY", "SELL", "TP", "SL", "TIMEOUT", "SKIP"
        match: "Home vs Away"
        team: Team we are betting on
        score: Score actuel "1-0"
        amount_usd: Montant en USD
        shares: Nombre de shares
        price: Entry/exit price
        pnl_pct: P&L en pourcentage (pour SELL)
        reason: Raison (pour SKIP)
    """
    if not notif_settings.notifications_enabled:
        return
    
    # Emoji selon le type
    emojis = {
        "BUY": "🟢",
        "SELL": "🔴",
        "TP": "🎯",
        "SL": "🛑",
        "TIMEOUT": "⏰",
        "SKIP": "⏭️",
        "GOAL": "⚽",
    }
    emoji = emojis.get(event_type, "📊")
    
    # Construire le message
    if event_type == "BUY":
        msg = (
            f"{emoji} <b>BUY</b> | {match}\n"
            f"⚽ But → <b>{score}</b>\n"
            f"💰 ${amount_usd:.2f} → {shares:.2f} shares @ ${price:.4f}\n"
            f"📍 Pari: <b>{team}</b>"
        )
    elif event_type in ("SELL", "TP", "SL", "TIMEOUT"):
        pnl_emoji = "📈" if pnl_pct >= 0 else "📉"
        msg = (
            f"{emoji} <b>{event_type}</b> | {match}\n"
            f"{pnl_emoji} P&L: <b>{pnl_pct:+.2f}%</b>\n"
            f"💰 {shares:.2f} shares @ ${price:.4f}"
        )
    elif event_type == "SKIP":
        msg = (
            f"{emoji} <b>SKIP</b> | {match}\n"
            f"⚽ {score}\n"
            f"📝 {reason}"
        )
    elif event_type == "GOAL":
        msg = (
            f"{emoji} <b>GOAL DETECTED</b>\n"
            f"🏟️ {match}\n"
            f"📊 Score: <b>{score}</b>"
        )
    else:
        msg = f"{emoji} {event_type}: {match} | {score}"
    
    asyncio.create_task(_send_telegram(msg))


def notify_error(error: str, context: str = "") -> None:
    """Notifie une erreur critique."""
    if not notif_settings.notifications_enabled:
        return
    msg = f"🚨 <b>ERREUR</b>\n{error}"
    if context:
        msg += f"\n📍 {context}"
    asyncio.create_task(_send_telegram(msg))


def notify_system(message: str) -> None:
    """Notify a system event (startup, shutdown, etc.)."""
    if not notif_settings.notifications_enabled:
        return
    msg = f"🤖 <b>SYSTEM</b>\n{message}"
    asyncio.create_task(_send_telegram(msg))


def notify_sell_failed(match: str, remaining_shares: float, slug: str = "") -> None:
    """Notify that a sell failed (position still open)."""
    if not notif_settings.notifications_enabled:
        return
    msg = (
        "⚠️ <b>SELL FAILED</b>\n"
        f"🏟️ {match}\n"
        f"📉 Position restante: <b>{remaining_shares:.2f} shares</b>\n"
        f"→ Check manually on Polymarket."
    )
    if slug:
        msg += f"\n📍 Slug: {slug}"
    asyncio.create_task(_send_telegram(msg))


def notify_matches_followed(matches: list[str]) -> None:
    """Notifie la liste des matchs en live (flux WS avec period/elapsed)."""
    if not notif_settings.notifications_enabled or not matches:
        return
    lines = [f"• {m}" for m in matches[:30]]  # cap at 30 to keep the message short
    if len(matches) > 30:
        lines.append(f"… et {len(matches) - 30} autre(s)")
    msg = f"📺 <b>Matchs en live</b> ({len(matches)})\n\n" + "\n".join(lines)
    asyncio.create_task(_send_telegram(msg))


def notify_goal(match: str, score: str, minute: str = "", source: str = "") -> None:
    """Notify a goal on a followed match (fire-and-forget)."""
    if not notif_settings.notifications_enabled:
        return
    msg = (
        "⚽ <b>GOAL</b> on a followed match\n"
        f"🏟️ {match}\n"
        f"📊 Score: <b>{score}</b>"
    )
    if minute:
        msg += f"\n⏱ {minute}'"
    if source:
        msg += f"\n<i>{source}</i>"
    asyncio.create_task(_send_telegram(msg))
