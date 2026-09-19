import asyncio
import random
import os
import io
import json
import logging
import aiohttp
import qrcode
from pathlib import Path
from datetime import datetime
from playwright.async_api import async_playwright 
from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile
from dotenv import load_dotenv


load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────────────────────
IS_ON_VPS     = True    # Running on GCP VPS (africa-south1)

COOKIES_FILE  = "bybit_session.json"
BOT_STATE_FILE = "bybit_bot_state.json"   # persists user intent (running/browser_enabled) across restarts
BYBIT_P2P_URL = "https://www.bybit.com/en/p2p/merchant-admin/backlog"
FALLBACK_URL  = "https://www.bybit.com/en/p2p/buy/BTC/NGN"


MIN_INTERVAL  = 25 * 60   # 25 minutes
MAX_INTERVAL  = 40 * 60   # 40 minutes

BROWSER_RECYCLE_SECONDS = 12 * 60 * 60   # internal recycle: close & relaunch browser stack every 12h
RUN_SAFE_MAX_RETRIES    = 3              # how many times run_safe restarts a crashed task
BROWSER_ENABLED_CHECK_INTERVAL = 30      # how often the recycle-sleep wakes to check for a close request

BOT_TOKEN     = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID       = int(os.getenv("TELEGRAM_CHAT_ID"))

# Marker file written by the systemd scheduled-restart service right before
# it restarts the bot. On boot, the bot checks for this file to tell the
# difference between a genuine startup (crash/reboot/manual) and a routine
# scheduled restart — only the former sends a phone call.
SCHEDULED_RESTART_MARKER = "/tmp/bybit_scheduled_restart"

# ── QR LOGIN ──────────────────────────────────────────────────────────────────
QR_GENERATE_URL  = "https://www.bybit.com/x-api/v3/public/qrcode/generate"
QR_STATUS_URL    = "https://www.bybit.com/x-api/v3/public/qrcode/status"
QR_REFRESH_SECS  = 5 * 60          # regenerate + resend QR every 5 minutes
QR_POLL_MIN      = 5               # random poll interval min (seconds)
QR_POLL_MAX      = 10              # random poll interval max (seconds)
QR_HEADERS = {
    "accept": "application/json",
    "accept-language": "en-US,en;q=0.9,en-NG;q=0.8",
    "content-type": "application/json;charset=UTF-8",
    "dnt": "1",
    "guid": "adf175f4-dd04-cd06-c0bf-b96824d7e3a7",
    "lang": "en",
    "platform": "pc",
    "referer": "https://www.bybit.com/en/login?redirect_url=https%3A%2F%2Fwww.bybit.com%2Fen%2F&isHomepage=1",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/153.0.0.0 Safari/537.36 Edg/153.0.0.0"
    ),
}
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
class State:
    # User intent — paused or running (drives session_task existence)
    running: bool = True
    # Keepalive counters
    last_refresh: datetime | None = None
    refresh_count: int = 0
    # Bybit login state (independent of pause/browser control)
    session_alive: bool = True
    started_at: datetime | None = None
    # Prevents alert spam across systemd restarts
    expiry_alerted: bool = False
    # The currently running keepalive task (or None when paused/closed)
    session_task: asyncio.Task | None = None
    # Whether the browser stack is physically open — set/cleared by
    # launch_browser_stack / close_browser_stack. Drives the keyboard label
    # and guards double-open / double-close taps.
    browser_opened: bool = False

# Live references to the current browser stack — read by pause/resume handlers
# so they can attach a new session_task to the same page/context without
# needing a full browser relaunch.
browser_ref = {
    "playwright": None,
    "browser":    None,
    "context":    None,
    "page":       None,
}

class Events:
    # Set   -> browser_manager should launch/keep the browser running.
    # Clear -> browser should be fully closed and stay down.
    browser_enabled: asyncio.Event = asyncio.Event()
    # Set by launch_browser_stack() once Chrome is genuinely up.
    # Cleared by close_browser_stack() so the next Open starts fresh.
    # Awaited by the Open handler to confirm before replying.
    browser_launched: asyncio.Event = asyncio.Event()
# ─────────────────────────────────────────────────────────────────────────────

bot    = Bot(token=BOT_TOKEN)
router = Router()
dp = Dispatcher()
dp.include_router(router)


# ── KEYBOARD (built dynamically from state/memory on every render) ──────────
def get_keyboard() -> ReplyKeyboardMarkup:
    pause_label   = "⏸ Pause" if State.running else "▶️ Resume"
    browser_label = "🔴 Close Browser" if State.browser_opened else "🟢 Open Browser"
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=pause_label), KeyboardButton(text=browser_label)],
            [KeyboardButton(text="📊 Status")],
        ],
        resize_keyboard=True,
        persistent=True,
    )
# ─────────────────────────────────────────────────────────────────────────────


def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


# ── PERSISTED INTENT (survives restarts) ─────────────────────────────────────
# Only stores the user's last deliberate choice — not live counters like
# refresh_count or last_refresh, which naturally reset on a fresh process.
def load_persisted_intent() -> dict:
    defaults = {"browser_enabled": True, "running": True}
    if Path(BOT_STATE_FILE).exists():
        try:
            with open(BOT_STATE_FILE, "r") as f:
                saved = json.load(f)
            defaults.update({k: saved[k] for k in defaults if k in saved})
        except Exception as e:
            log(f"[!] Failed to read {BOT_STATE_FILE}, using defaults: {e}")
    return defaults


def save_persisted_intent():
    try:
        with open(BOT_STATE_FILE, "w") as f:
            json.dump({
                "browser_enabled": Events.browser_enabled.is_set(),
                "running": State.running,
            }, f, indent=4)
    except Exception as e:
        log(f"[!] Failed to write {BOT_STATE_FILE}: {e}")


def apply_persisted_intent():
    """Loads bybit_bot_state.json and sets browser_enabled accordingly.
    Must run before browser_manager starts. The 'running' (pause) intent is
    applied separately, inside browser_manager, right after a successful
    login — since there's no session_task to pause/resume until then."""
    intent = load_persisted_intent()
    if intent["browser_enabled"]:
        Events.browser_enabled.set()
    else:
        Events.browser_enabled.clear()
    State.running = intent["running"]
    log(f"[i] Restored last known state -> browser_enabled={intent['browser_enabled']}, running={intent['running']}")
    return intent



async def notify(msg: str):
    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=msg,
            reply_markup=get_keyboard()
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
    State.expiry_alerted = False


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


# ── QR LOGIN ─────────────────────────────────────────────────────────────────
async def qr_login(context) -> bool:
    """
    Generates a Bybit QR code, sends it to Telegram, polls for scan.
    Refreshes the QR every 5 minutes automatically if not yet scanned.
    On confirmed scan (code=200), injects auth cookies into Playwright context.
    Returns True on success, False if browser_enabled was cleared mid-wait.
    """
    log("[QR] Starting QR login flow...")
    await notify(
        "🔐 Bybit session expired.\n"
        "Generating QR code — scan with your Bybit app to log back in."
    )
    await alert_client.trigger_call(
        source="bybit-keepalive",
        error_signature="session_expired",
        message="Bybit P2P session expired. QR code sent to Telegram for re-login.",
        severity="critical",
    )

    async def _generate_and_send(session: aiohttp.ClientSession, prev_message_id: int | None = None) -> tuple[str, int | None]:
        """Generate a fresh QR, send to Telegram, return (uuid, message_id)."""
        async with session.get(QR_GENERATE_URL, headers=QR_HEADERS) as resp:
            data = await resp.json()
            if data.get("ret_code") != 0:
                raise RuntimeError(f"QR generate failed: {data}")
        result       = data["result"]
        uuid         = result["uuid"]
        code_content = result["codeContent"]

        # Build QR image in memory
        qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_H, box_size=10, border=4)
        qr.add_data(code_content)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white").convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)

        # Mark previous QR as expired before sending new one
        if prev_message_id:
            try:
                await bot.edit_message_caption(
                    chat_id=CHAT_ID,
                    message_id=prev_message_id,
                    caption="⌛ QR Expired — do not scan this. A fresh one is on its way...",
                )
            except Exception as e:
                log(f"[QR] Could not mark previous QR as expired: {e}")

        # Send to Telegram as photo
        message_id = None
        try:
            sent = await bot.send_photo(
                chat_id=CHAT_ID,
                photo=BufferedInputFile(buf.read(), filename="bybit_qr.png"),
                caption="📱 Scan this QR with your Bybit app to restore the session.\nExpires in 5 minutes — a new one will be sent automatically.",
                reply_markup=get_keyboard(),
            )
            message_id = sent.message_id
            log("[QR] QR code sent to Telegram.")
        except Exception as e:
            log(f"[QR] Failed to send QR to Telegram: {e}")

        return uuid, message_id

    async with aiohttp.ClientSession() as session:
        uuid, message_id = await _generate_and_send(session)
        elapsed          = 0
        last_code        = None

        while True:
            # Check if browser was closed mid-wait
            if not Events.browser_enabled.is_set():
                log("[QR] Browser close requested during QR login — aborting.")
                return False

            # Refresh QR every 5 minutes
            if elapsed >= QR_REFRESH_SECS:
                log("[QR] QR expired — regenerating and resending...")
                uuid, message_id = await _generate_and_send(session, prev_message_id=message_id)
                elapsed = 0

            # Poll status
            poll_interval = random.randint(QR_POLL_MIN, QR_POLL_MAX)
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

            async with session.get(f"{QR_STATUS_URL}?uuid={uuid}", headers=QR_HEADERS) as resp:
                data   = await resp.json()
                result = data.get("result", {})
                code   = result.get("code")

                if code != last_code:
                    log(f"[QR] Status: {last_code} → {code}")
                    last_code = code

                if code == "200":
                    log("[QR] ✅ Scan confirmed! Injecting cookies...")

                    # Mark QR as used
                    if message_id:
                        try:
                            await bot.edit_message_caption(
                                chat_id=CHAT_ID,
                                message_id=message_id,
                                caption="✅ QR scanned successfully — session restored.",
                            )
                        except Exception:
                            pass

                    # Extract auth data
                    token        = result.get("userToken", "")
                    secure_token = None
                    for cookie in session.cookie_jar:
                        if cookie.key == "secure-token":
                            secure_token = cookie.value
                    # Fallback to Token response header
                    if not secure_token:
                        secure_token = dict(resp.headers).get("Token", token)

                    # Inject into Playwright context
                    await context.add_cookies([
                        {"name": "isLogin",      "value": "1",          "domain": ".bybit.com", "path": "/"},
                        {"name": "secure-token", "value": secure_token, "domain": ".bybit.com", "path": "/"},
                        {"name": "token",        "value": token,        "domain": ".bybit.com", "path": "/"},
                    ])
                    log("[QR] Cookies injected into Playwright context.")

                    # Navigate to target page and confirm login before saving
                    page = browser_ref.get("page")
                    if page:
                        await page.goto(BYBIT_P2P_URL, wait_until="domcontentloaded")
                        await asyncio.sleep(4)
                        if await is_logged_in(page):
                            await save_cookies(context)
                            await notify("✅ QR scan confirmed. Session restored — resuming keep-alive.")
                            log("[QR] Session saved and confirmed on P2P page.")
                        else:
                            log("[QR] ⚠️ Cookies injected but login not confirmed on P2P page.")
                    return True


# ── MANUAL LOGIN (dead code — kept for reference) ─────────────────────────────
async def manual_login(page, context):
    """
    On VPS: login happens LIVE, in this same browser, via noVNC/VNC.
    We just wait here until is_logged_in() becomes true, then save cookies.

    IMPORTANT: never close this tab manually inside the VNC session.

    Also bails out early if the user requests a browser close mid-wait, so
    a "Close Browser" tap doesn't get stuck behind a login poll loop.
    """
    if IS_ON_VPS:
        if not State.expiry_alerted:
            msg = (
                "🔴 Bybit session EXPIRED / not logged in.\n\n"
                "To fix:\n"
                "1. SSH tunnel in: ssh -L 6080:localhost:6080 <user>@<vps_ip>\n"
                "2. Open http://localhost:6080/vnc.html in your browser\n"
                "3. Log in live in the Chrome window (this same session)\n"
                "4. It will detect login automatically and resume — no restart needed"
            )
            await notify(msg)
            State.expiry_alerted = True
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
            if not Events.browser_enabled.is_set():
                log("[i] Browser close requested during login wait — aborting manual_login.")
                return
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
    gets wrapped as State.session_task — Pause cancels it, Resume recreates
    it. No internal "running" flag check needed anymore since Pause now
    controls this at the task level.
    """
    log("[+] Keep-alive loop started. Account is now ONLINE.")
    State.started_at = datetime.now()
    await notify("✅ Bybit Keep-Alive started.\nYour P2P account is ONLINE.")

    while True:
        interval = random.randint(MIN_INTERVAL, MAX_INTERVAL)
        log(f"[~] Next refresh in {interval // 60}m {interval % 60}s")
        await asyncio.sleep(interval)

        if not State.browser_opened:
            log("[i] Browser closed remotely — continuing.")
            continue

        State.refresh_count += 1
        log(f"[*] Refreshing page (#{State.refresh_count})...")

        await page.goto(FALLBACK_URL, wait_until="domcontentloaded")
        await asyncio.sleep(2)
        await page.goto(BYBIT_P2P_URL, wait_until="domcontentloaded")
        await asyncio.sleep(3)

        if await is_logged_in(page):
            State.session_alive = True
            State.last_refresh  = datetime.now()
            log(f"[✓] Still ONLINE. Refresh #{State.refresh_count} successful.")
        else:
            log("[!] Session looks dead. Trying cookie reload...")
            await load_cookies(context)
            await page.goto(BYBIT_P2P_URL, wait_until="domcontentloaded")
            await asyncio.sleep(5)

            if await is_logged_in(page):
                State.session_alive = True
                State.last_refresh  = datetime.now()
                log("[✓] Session recovered via cookie reload.")
            else:
                State.session_alive = False
                log("[!] Session DEAD. Starting QR re-login...")
                await qr_login(context)
                State.session_alive = True


# ── BROWSER MANAGER (owns Playwright lifecycle + 12h internal recycle + remote close/open) ───
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

    State.browser_opened = True
    Events.browser_launched.set()
    log("[+] Browser stack launched.")
    return playwright, browser, context, page


async def close_browser_stack(pause_intent: bool = True):
    """Cancels the running session task and tears down the browser stack.

    pause_intent controls whether this teardown should also flip the user's
    'running' (pause/resume) intent to False. This should be True for a real
    user-driven close (or a login failure, where nothing was running anyway),
    but False for the transparent 12h internal recycle — that relaunch should
    come back up in whatever running-state the user last chose, not silently
    paused.
    """
    task = State.session_task
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    State.session_task = None

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

    State.browser_opened = False
    Events.browser_launched.clear()
    if pause_intent:
        State.running = False

    log("[+] Browser stack closed.")
    save_persisted_intent()


async def browser_manager():
    """
    Owns the full Playwright/browser lifecycle.

    Waits on `browser_enabled` before doing anything. When enabled, launches
    the stack, logs in, starts the keepalive session task, then sleeps until
    either the 12h internal recycle mark is hit OR the user clears
    `browser_enabled` (remote "Close Browser") — whichever comes first. On
    recycle it relaunches immediately (preserving whatever running-state was
    already in effect); on a user-requested close it tears everything down,
    marks running=False, and goes back to waiting until "Open Browser" is
    tapped.
    """
    while True:
        await Events.browser_enabled.wait()

        playwright, browser, context, page = await launch_browser_stack()

        # State.browser_opened is already True — set inside launch_browser_stack().
        # Authentication is a separate concern that happens below.
        await notify("🌐 Checking session...")

        has_cookies = await load_cookies(context)
        if has_cookies:
            await page.goto(BYBIT_P2P_URL, wait_until="domcontentloaded")
            await asyncio.sleep(4)

        if not await is_logged_in(page):
            success = await qr_login(context)
            if not success:
                log("[i] Browser close requested during QR login — tearing down.")
                await close_browser_stack(pause_intent=True)
                continue

        if not Events.browser_enabled.is_set():
            # user closed the browser while login was waiting
            log("[i] Browser close requested during login — tearing down.")
            await close_browser_stack(pause_intent=True)
            continue

        if not await is_logged_in(page):
            log("[!] Could not log in. Exiting browser_manager.")
            await send_telegram_raw("❌ Keep-Alive failed to start. Could not log in.")
            await close_browser_stack(pause_intent=True)
            return

        if State.running:
            log("[+] Logged in. Starting keep-alive (restored 'running' state)...")
            State.session_task = asyncio.create_task(
                run_safe("KeepAliveLoop", keepalive_loop, page, context)
            )
        else:
            log("[+] Logged in, but restored state was PAUSED — leaving session task off.")
            State.started_at = datetime.now()
            await notify("✅ Bybit browser is up (restored PAUSED state — tap ▶️ Resume to go ONLINE).")

        log(f"[i] Browser stack will recycle in {BROWSER_RECYCLE_SECONDS // 3600}h (or sooner if closed remotely).")

        # Sleep for the recycle window, but wake early + often to check
        # whether a remote "Close Browser" request came in.
        elapsed = 0
        while elapsed < BROWSER_RECYCLE_SECONDS and Events.browser_enabled.is_set():
            await asyncio.sleep(min(BROWSER_ENABLED_CHECK_INTERVAL, BROWSER_RECYCLE_SECONDS - elapsed))
            elapsed += BROWSER_ENABLED_CHECK_INTERVAL

        if Events.browser_enabled.is_set():
            # Transparent internal recycle — do NOT touch the user's pause
            # intent. Whatever 'running' was before (True or False) should
            # still be true right after relaunch.
            log("[~] 12h internal recycle — closing browser stack and relaunching...")
            await notify("🔄 Internal 12h browser recycle — closing and relaunching Chrome (Telegram bot unaffected).")
            await close_browser_stack(pause_intent=False)
        else:
            # browser_enabled was cleared — either by the handler (direct close)
            # or some other path. Only act if the browser is still open, meaning
            # the handler didn't already close and notify. Prevents the duplicate
            # "Browser CLOSED" message when the handler does the close directly.
            if State.browser_opened:
                log("[~] Remote close detected — closing browser stack and standing by.")
                await notify("🔴 Browser CLOSED.\nYou will appear OFFLINE. Tap 🟢 Open Browser to bring it back up.")
                await close_browser_stack(pause_intent=True)
            else:
                log("[~] Remote close detected — browser already closed by handler, skipping.")
        # loop restarts: either relaunches immediately (recycle) or blocks
        # on browser_enabled.wait() until the user taps "Open Browser".



# ── TELEGRAM HANDLERS ─────────────────────────────────────────────────────────
@router.message(lambda m: m.text in ("⏸ Pause", "▶️ Resume"))
async def cmd_toggle_pause(message: Message):
    if message.chat.id != CHAT_ID:
        return

    if not State.browser_opened:
        await message.answer(
            "⚠️ Browser is closed right now — nothing to pause/resume.\nTap 🟢 Open Browser first.",
            reply_markup=get_keyboard()
        )
        return

    task = State.session_task

    if task and not task.done():
        # currently running -> pause
        task.cancel()
        State.session_task = None
        State.running = False
        save_persisted_intent()
        log("[Telegram] Paused by user — session task cancelled.")
        await message.answer(
            "⏸ Keep-Alive PAUSED.\nYou will appear OFFLINE on P2P.",
            reply_markup=get_keyboard()
        )
    else:
        # currently paused -> resume
        page    = browser_ref.get("page")
        context = browser_ref.get("context")
        if page is None or context is None:
            await message.answer(
                "⚠️ Browser isn't ready yet — try again in a moment.",
                reply_markup=get_keyboard()
            )
            return
        State.running = True
        State.session_task = asyncio.create_task(
            run_safe("KeepAliveLoop", keepalive_loop, page, context)
        )
        save_persisted_intent()
        log("[Telegram] Resumed by user — session task created.")
        await message.answer(
            "▶️ Keep-Alive RESUMED.\nYou will appear ONLINE on next refresh cycle.",
            reply_markup=get_keyboard()
        )


@router.message(lambda m: m.text in ("🔴 Close Browser", "🟢 Open Browser"))
async def cmd_toggle_browser(message: Message):
    if message.chat.id != CHAT_ID:
        return

    if message.text == "🔴 Close Browser":
        if not State.browser_opened:
            await message.answer(
                "⚠️ Browser is already closed — nothing to do.\nTap 🟢 Open Browser to bring it back up.",
                reply_markup=get_keyboard()
            )
            return
        log("[Telegram] Close Browser requested by user.")
        Events.browser_enabled.clear()
        # Close directly — State.browser_opened and Events.browser_launched
        # are both cleared inside close_browser_stack(), so get_keyboard()
        # returns the flipped button confirmed in the reply below.
        await close_browser_stack(pause_intent=True)
        await message.answer(
            "🔴 Browser CLOSED.\nYou will appear OFFLINE on P2P. Tap 🟢 Open Browser to bring it back up.",
            reply_markup=get_keyboard()
        )

    elif message.text == "🟢 Open Browser":
        if State.browser_opened:
            await message.answer(
                "⚠️ Browser is already open — nothing to do.\nTap 🔴 Close Browser to shut it down.",
                reply_markup=get_keyboard()
            )
            return
        log("[Telegram] Open Browser requested by user.")
        # Clear first so we get a fresh signal from this launch.
        Events.browser_launched.clear()
        State.running = True
        Events.browser_enabled.set()
        save_persisted_intent()
        # Wait for launch_browser_stack() to confirm the browser is genuinely
        # up before replying — keyboard flips only when it's real.
        try:
            await asyncio.wait_for(Events.browser_launched.wait(), timeout=30)
            await message.answer(
                "🟢 Browser OPENED.\nKeep-Alive will resume automatically once the session is restored.",
                reply_markup=get_keyboard()
            )
        except asyncio.TimeoutError:
            log("[!] Browser launch timed out — no confirmation from launch_browser_stack.")
            await message.answer(
                "⚠️ Browser launch timed out. Check VPS and try again.",
                reply_markup=get_keyboard()
            )


@router.message(lambda m: m.text == "📊 Status")
async def cmd_status(message: Message):
    if message.chat.id != CHAT_ID:
        return

    task = State.session_task
    task_alive = bool(task and not task.done())
    browser_str = "🟢 Open" if State.browser_opened else "🔴 Closed"
    running_str = "▶️ Running" if task_alive else "⏸ Paused"
    session_str = "✅ Alive"   if State.session_alive else "🔴 DEAD"
    last_str    = (
        State.last_refresh.strftime("%Y-%m-%d %H:%M:%S")
        if State.last_refresh else "Not yet"
    )
    uptime_str  = (
        str(datetime.now() - State.started_at).split(".")[0]
        if State.started_at else "N/A"
    )

    await message.answer(
        f"📊 Keep-Alive Status\n\n"
        f"Browser:         {browser_str}\n"
        f"State:           {running_str}\n"
        f"Session:         {session_str}\n"
        f"Last refresh:    {last_str}\n"
        f"Total refreshes: {State.refresh_count}\n"
        f"Uptime:          {uptime_str}",
        reply_markup=get_keyboard()
    )


# ── MAIN ──────────────────────────────────────────────────────────────────────
async def start_telegram():

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

    apply_persisted_intent()

    try:
        await asyncio.gather(
            run_safe("TelegramBot", start_telegram),
            run_safe("BrowserManager", browser_manager),
        )
    finally:
        log("[+] Shutting down. Closing browser...")
        await close_browser_stack(pause_intent=True)
        await alert_client.close()


if __name__ == "__main__":
    asyncio.run(main())