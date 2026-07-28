import asyncio
import random
import os
import json
import logging
import aiohttp
from pathlib import Path
from datetime import datetime
from playwright.async_api import async_playwright
from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from dotenv import load_dotenv

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────────────────────
IS_ON_VPS     = False    # Running on GCP VPS (africa-south1)

COOKIES_FILE  = "bybit_session.json"
BYBIT_P2P_URL = "https://www.bybit.com/en/p2p/merchant-admin/backlog"
FALLBACK_URL  = "https://www.bybit.com/en/p2p/buy/BTC/NGN"

MIN_INTERVAL  = 25 * 60   # 25 minutes
MAX_INTERVAL  = 40 * 60   # 40 minutes

BROWSER_RECYCLE_SECONDS = 12 * 60 * 60   # internal recycle: close & relaunch browser stack every 12h
RUN_SAFE_MAX_RETRIES    = 3              # how many times run_safe restarts a crashed task

BOT_TOKEN     = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID       = int(os.getenv("TELEGRAM_CHAT_ID"))

# Marker file written by the systemd scheduled-restart service right before
# it restarts the bot. On boot, the bot checks for this file to tell the
# difference between a genuine startup (crash/reboot/manual) and a routine
# scheduled restart — only the former sends a phone call.
SCHEDULED_RESTART_MARKER = "/tmp/bybit_scheduled_restart"
# ─────────────────────────────────────────────────────────────────────────────

# ── ALERT GATEWAY (phone call trigger) ──────────────────────────────────────
class alert_client:
    log = logging.getLogger(__name__)

    ALERT_GATEWAY_URL     = os.getenv("ALERT_GATEWAY_URL", "https://redux-server-api.onrender.com/caller")
    ALERT_INTERNAL_SECRET = os.getenv("CALLER_INTERNAL_SECRET", "change-me-please")
    ALERT_DEVICE_ID       = os.getenv("ALERT_DEVICE_ID", "redux-phone-1")

    _session: aiohttp.ClientSession | None = None

    @classmethod
    def _get_session(cls) -> aiohttp.ClientSession:
        if cls._session is None or cls._session.closed:
            cls._session = aiohttp.ClientSession()
        return cls._session

    @classmethod
    async def trigger_call(
        cls,
        source: str,
        error_signature: str,
        message: str,
        severity: str = "critical",
        max_retries: int = 1,
    ):
        payload = {
            "source": source,
            "error_signature": error_signature,
            "message": message,
            "severity": severity,
            "max_retries": max_retries,
            "device_id": cls.ALERT_DEVICE_ID,
        }

        try:
            session = cls._get_session()
            async with session.post(
                f"{cls.ALERT_GATEWAY_URL}/trigger-call",
                json=payload,
                headers={"x-internal-secret": cls.ALERT_INTERNAL_SECRET},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    cls.log.error(f"[alert_client] trigger-call failed [{resp.status}]: {body}")
                    return
                print("[alert_client] trigger-call success")
        except Exception as e:
            cls.log.error(f"[alert_client] trigger-call error: {e}")

    @classmethod
    async def close(cls):
        if cls._session and not cls._session.closed:
            await cls._session.close()
# ─────────────────────────────────────────────────────────────────────────────

# ── STATE ─────────────────────────────────────────────────────────────────────
state = {
    "running":         True,   # user intent: paused or not (drives session_task existence)
    "last_refresh":    None,
    "refresh_count":   0,
    "session_alive":   True,   # bybit login state (independent of pause)
    "started_at":      None,
    "expiry_alerted":  False,  # prevents alert spam across systemd restarts
    "session_task":    None,   # the currently running keepalive/login task (or None)
}

# Live references to the current browser stack — read by pause/resume handlers
# so they can attach a new session_task to the same page/context without
# needing a full browser relaunch.
browser_ref = {
    "playwright": None,
    "browser":    None,
    "context":    None,
    "page":       None,
}
# ─────────────────────────────────────────────────────────────────────────────

bot    = Bot(token=BOT_TOKEN)
router = Router()

# ── KEYBOARD ──────────────────────────────────────────────────────────────────
KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="⏸ Pause"),
            KeyboardButton(text="▶️ Resume"),
        ],
        [
            KeyboardButton(text="📊 Status"),
        ],
    ],
    resize_keyboard=True,
    persistent=True,
)
# ─────────────────────────────────────────────────────────────────────────────


def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")



async def notify(msg: str):
    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=msg,
            reply_markup=KEYBOARD
        )
    except Exception as e:
        log(f"[Telegram ERROR] {e}")


async def send_telegram_raw(msg: str):
    """Send via aiohttp — used when bot loop isn't running yet."""
    if not BOT_TOKEN or not CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(url, json={
                "chat_id": CHAT_ID,
                "text": msg
            })
    except Exception as e:
        log(f"[Telegram ERROR] {e}")


# ── RUN SAFE ──────────────────────────────────────────────────────────────────
async def run_safe(name: str, coro_func, *args, max_retries: int = RUN_SAFE_MAX_RETRIES):
    """
    Wraps a top-level task (browser_manager, start_telegram, etc.) so that if
    it raises, we log it, notify + call, and restart it — up to max_retries
    times. CancelledError is never swallowed, so Pause can still cancel a
    running session_task cleanly without triggering a "crash" restart.
    """
    attempt = 0
    while attempt < max_retries:
        try:
            await coro_func(*args)
            # Normal return (not expected for infinite loops, but handle it
            # gracefully rather than looping forever on a no-op).
            log(f"[i] {name} exited normally.")
            return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            attempt += 1
            log(f"[!] {name} crashed (attempt {attempt}/{max_retries}): {e}")
            await notify(f"⚠️ {name} crashed (attempt {attempt}/{max_retries}):\n{e}")
            await alert_client.trigger_call(
                source="bybit-keepalive",
                error_signature=f"{name}_crash",
                message=f"{name} crashed (attempt {attempt}/{max_retries}): {e}",
                severity="critical",
            )
            if attempt >= max_retries:
                log(f"[!] {name} failed {max_retries} times — giving up. Manual intervention needed.")
                await notify(f"🔴 {name} has failed {max_retries} times and will NOT restart automatically.")
                await alert_client.trigger_call(
                    source="bybit-keepalive",
                    error_signature=f"{name}_exhausted",
                    message=f"{name} failed {max_retries} times in a row and will not restart automatically.",
                    severity="critical",
                )
                return
            await asyncio.sleep(10)


# ── SESSION ───────────────────────────────────────────────────────────────────
async def save_cookies(context):
    cookies = await context.cookies()
    with open(COOKIES_FILE, "w") as f:
        json.dump(cookies, f, indent=4)
    log("[+] Session cookies saved.")
    state["expiry_alerted"] = False


async def load_cookies(context):
    if Path(COOKIES_FILE).exists():
        with open(COOKIES_FILE, "r") as f:
            cookies = json.load(f)
        await context.add_cookies(cookies)
        log("[+] Session cookies loaded.")
        return True
    log("[!] No session file found.")
    return False


# ── LOGIN CHECK ───────────────────────────────────────────────────────────────
async def is_logged_in(page) -> bool:
    if "login" in page.url.lower():
        return False
    try:
        await page.wait_for_selector(".p2p__nickName--wrap", timeout=10000)
        return True
    except Exception:
        return False


# ── MANUAL LOGIN ──────────────────────────────────────────────────────────────
async def manual_login(page, context):
    """
    On VPS: login happens LIVE, in this same browser, via noVNC/VNC.
    We just wait here until is_logged_in() becomes true, then save cookies.

    IMPORTANT: never close this tab manually inside the VNC session.
    """
    if IS_ON_VPS:
        if not state["expiry_alerted"]:
            msg = (
                "🔴 Bybit session EXPIRED / not logged in.\n\n"
                "To fix:\n"
                "1. SSH tunnel in: ssh -L 6080:localhost:6080 <user>@<vps_ip>\n"
                "2. Open http://localhost:6080/vnc.html in your browser\n"
                "3. Log in live in the Chrome window (this same session)\n"
                "4. It will detect login automatically and resume — no restart needed"
            )
            await notify(msg)
            state["expiry_alerted"] = True
            log("[!] Session expired on VPS. Telegram alert sent (won't repeat until resolved).")

            await alert_client.trigger_call(
                source="bybit-keepalive",
                error_signature="session_expired",
                message="Bybit P2P session expired. Live login required via noVNC.",
                severity="critical",
            )

        log("[!] Waiting for live login via noVNC... polling every 60s.")
        await page.goto(BYBIT_P2P_URL, wait_until="domcontentloaded")

        POLL_INTERVAL   = 60
        RECALL_INTERVAL = 30 * 60
        waited = 0

        while not await is_logged_in(page):
            await asyncio.sleep(POLL_INTERVAL)
            waited += POLL_INTERVAL

            if waited >= RECALL_INTERVAL:
                waited = 0
                log("[!] Still not logged in after wait window — escalating with another call.")
                await notify("🔴 Still OFFLINE — Bybit session not yet restored. Reminder call incoming.")
                await alert_client.trigger_call(
                    source="bybit-keepalive",
                    error_signature="session_expired_unresolved",
                    message="Bybit P2P session still not restored after 10+ minutes. Live login still required.",
                    severity="critical",
                )

            try:
                await page.goto(BYBIT_P2P_URL, wait_until="domcontentloaded")
            except Exception:
                pass

        log("[✓] Live login detected on VPS.")
        await save_cookies(context)
        await notify("✅ Live login detected. Session restored — resuming keep-alive.")
        return

    # Local (laptop) path — unchanged
    log("[!] Not logged in. Please log in manually in the browser window.")
    log("[!] Press ENTER here once you are fully logged in.")
    await page.goto(BYBIT_P2P_URL, wait_until="domcontentloaded")
    input()
    await save_cookies(context)


# ── KEEPALIVE LOOP ────────────────────────────────────────────────────────────
async def keepalive_loop(page, context):
    """
    Runs the refresh cycle + login-wait recovery. This is the coroutine that
    gets wrapped as state["session_task"] — Pause cancels it, Resume recreates
    it. No internal "running" flag check needed anymore since Pause now
    controls this at the task level.
    """
    log("[+] Keep-alive loop started. Account is now ONLINE.")
    state["started_at"] = datetime.now()
    await notify("✅ Bybit Keep-Alive started.\nYour P2P account is ONLINE.")

    while True:
        interval = random.randint(MIN_INTERVAL, MAX_INTERVAL)
        log(f"[~] Next refresh in {interval // 60}m {interval % 60}s")
        await asyncio.sleep(interval)

        state["refresh_count"] += 1
        log(f"[*] Refreshing page (#{state['refresh_count']})...")

        await page.goto(FALLBACK_URL, wait_until="domcontentloaded")
        await asyncio.sleep(2)
        await page.goto(BYBIT_P2P_URL, wait_until="domcontentloaded")
        await asyncio.sleep(3)

        if await is_logged_in(page):
            state["session_alive"] = True
            state["last_refresh"]  = datetime.now()
            log(f"[✓] Still ONLINE. Refresh #{state['refresh_count']} successful.")
        else:
            log("[!] Session looks dead. Trying cookie reload...")
            await load_cookies(context)
            await page.goto(BYBIT_P2P_URL, wait_until="domcontentloaded")
            await asyncio.sleep(5)

            if await is_logged_in(page):
                state["session_alive"] = True
                state["last_refresh"]  = datetime.now()
                log("[✓] Session recovered via cookie reload.")
            else:
                state["session_alive"] = False
                log("[!] Session DEAD. Entering live re-login wait...")
                await manual_login(page, context)
                state["session_alive"] = True


# ── BROWSER MANAGER (owns Playwright lifecycle + 12h internal recycle) ───────
async def launch_browser_stack():
    """Launches a fresh Playwright + browser + context + page, stores refs."""
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(
        headless=False,
        channel="chrome",
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
        ]
    )

    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        locale="en-NG",
        timezone_id="Africa/Lagos",
        permissions=["notifications"],
    )

    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
        window.chrome = { runtime: {} };
    """)

    page = await context.new_page()

    browser_ref["playwright"] = playwright
    browser_ref["browser"]    = browser
    browser_ref["context"]    = context
    browser_ref["page"]       = page

    return playwright, browser, context, page


async def close_browser_stack():
    """Cancels the running session task and tears down the browser stack."""
    task = state.get("session_task")
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    state["session_task"] = None

    browser = browser_ref.get("browser")
    playwright = browser_ref.get("playwright")
    if browser:
        try:
            await browser.close()
        except Exception as e:
            log(f"[!] Error closing browser: {e}")
    if playwright:
        try:
            await playwright.stop()
        except Exception as e:
            log(f"[!] Error stopping playwright: {e}")

    browser_ref["playwright"] = None
    browser_ref["browser"]    = None
    browser_ref["context"]    = None
    browser_ref["page"]       = None


async def browser_manager():
    """
    Owns the full Playwright/browser lifecycle. Launches the stack, logs in,
    starts the keepalive session task, then sleeps until the 12h internal
    recycle mark — at which point it closes everything and relaunches fresh.
    This never touches the Telegram bot, which runs as a fully separate task.
    """
    while True:
        playwright, browser, context, page = await launch_browser_stack()

        has_cookies = await load_cookies(context)
        if has_cookies:
            await page.goto(BYBIT_P2P_URL, wait_until="domcontentloaded")
            await asyncio.sleep(3)

        if not await is_logged_in(page):
            await manual_login(page, context)

        if not await is_logged_in(page):
            log("[!] Could not log in. Exiting browser_manager.")
            await send_telegram_raw("❌ Keep-Alive failed to start. Could not log in.")
            await browser.close()
            await playwright.stop()
            return

        log("[+] Logged in. Starting keep-alive...")
        state["running"] = True
        state["session_task"] = asyncio.create_task(
            run_safe("KeepAliveLoop", keepalive_loop, page, context)
        )

        log(f"[i] Browser stack will recycle in {BROWSER_RECYCLE_SECONDS // 3600}h.")
        await asyncio.sleep(BROWSER_RECYCLE_SECONDS)

        log("[~] 12h internal recycle — closing browser stack and relaunching...")
        await notify("🔄 Internal 12h browser recycle — closing and relaunching Chrome (Telegram bot unaffected).")
        await close_browser_stack()
        # loop restarts: fresh launch_browser_stack() at top


# ── TELEGRAM HANDLERS ─────────────────────────────────────────────────────────
@router.message(lambda m: m.text == "⏸ Pause")
async def cmd_pause(message: Message):
    if message.chat.id != CHAT_ID:
        return

    task = state.get("session_task")
    if task and not task.done():
        task.cancel()
        state["session_task"] = None
        state["running"] = False
        log("[Telegram] Paused by user — session task cancelled.")
        await message.answer(
            "⏸ Keep-Alive PAUSED.\nYou will appear OFFLINE on P2P.",
            reply_markup=KEYBOARD
        )
    else:
        state["running"] = False
        log("[Telegram] Pause requested — no active session task to cancel.")
        await message.answer(
            "⏸ Already paused (no active session).",
            reply_markup=KEYBOARD
        )


@router.message(lambda m: m.text == "▶️ Resume")
async def cmd_resume(message: Message):
    if message.chat.id != CHAT_ID:
        return

    task = state.get("session_task")
    if task is None or task.done():
        page    = browser_ref.get("page")
        context = browser_ref.get("context")
        if page is None or context is None:
            await message.answer(
                "⚠️ Browser isn't ready yet — try again in a moment.",
                reply_markup=KEYBOARD
            )
            return
        state["running"] = True
        state["session_task"] = asyncio.create_task(
            run_safe("KeepAliveLoop", keepalive_loop, page, context)
        )
        log("[Telegram] Resumed by user — session task created.")
        await message.answer(
            "▶️ Keep-Alive RESUMED.\nYou will appear ONLINE on next refresh cycle.",
            reply_markup=KEYBOARD
        )
    else:
        await message.answer(
            "▶️ Already running.",
            reply_markup=KEYBOARD
        )


@router.message(lambda m: m.text == "📊 Status")
async def cmd_status(message: Message):
    if message.chat.id != CHAT_ID:
        return

    task = state.get("session_task")
    task_alive = bool(task and not task.done())
    running_str = "▶️ Running" if task_alive else "⏸ Paused"
    session_str = "✅ Alive"   if state["session_alive"] else "🔴 DEAD"
    last_str    = (
        state["last_refresh"].strftime("%Y-%m-%d %H:%M:%S")
        if state["last_refresh"] else "Not yet"
    )
    uptime_str  = (
        str(datetime.now() - state["started_at"]).split(".")[0]
        if state["started_at"] else "N/A"
    )

    await message.answer(
        f"📊 Keep-Alive Status\n\n"
        f"State:           {running_str}\n"
        f"Session:         {session_str}\n"
        f"Last refresh:    {last_str}\n"
        f"Total refreshes: {state['refresh_count']}\n"
        f"Uptime:          {uptime_str}",
        reply_markup=KEYBOARD
    )


# ── MAIN ──────────────────────────────────────────────────────────────────────
async def start_telegram():
    dp = Dispatcher()
    dp.include_router(router)

    marker = Path(SCHEDULED_RESTART_MARKER)

    if marker.exists():
        marker.unlink()
        log("[i] Detected scheduled restart marker — quiet restart, no call.")
        await notify("🔄 Bybit Keep-Alive scheduled restart completed.")
    else:
        log("[i] Genuine startup (no scheduled-restart marker) — sending call.")
        await notify("🚀 Bybit Keep-Alive process starting up on VPS...")
        await alert_client.trigger_call(
            source="bybit-keepalive",
            error_signature="process_startup",
            message=f"Bybit Keep-Alive process just started on the VPS. Updated now {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        )

    log("[*] Telegram bot started.")
    await dp.start_polling(bot)


async def main():
    log("=" * 55)
    log("  Bybit P2P Keep-Alive")
    log(f"  Mode: {'VPS' if IS_ON_VPS else 'LOCAL'}")
    log("=" * 55)

    try:
        await asyncio.gather(
            run_safe("BrowserManager", browser_manager),
            run_safe("TelegramBot", start_telegram),
        )
    finally:
        log("[+] Shutting down. Closing browser...")
        await close_browser_stack()
        await alert_client.close()


if __name__ == "__main__":
    asyncio.run(main())