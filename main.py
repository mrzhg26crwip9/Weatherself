"""
Tehran Weather Reporter — Telegram Userbot (Pyrogram)
=============================================================
به‌صورت پیش‌فرض هر ۱ ساعت یک‌بار (قابل تنظیم با REPORT_INTERVAL_HOURS)،
وضعیت آب‌وهوای تهران را از چند منبع مستقل دریافت می‌کند، آن‌ها را با هم
مقایسه/ترکیب کرده و یک گزارش زیبا (HTML) به کانال تلگرامی مشخص‌شده ارسال
می‌کند. همچنین می‌توان با REPORT_MODE=daily آن را به حالت «یک‌بار در روز
در ساعت مشخص» تغییر داد.

منابع داده:
  1) Open-Meteo (Forecast API)      -> دما، وضعیت هوا، رطوبت، باد   [بدون نیاز به کلید]
  2) Open-Meteo (Air Quality API)   -> شاخص کیفیت هوا AQI            [بدون نیاز به کلید]
  3) wttr.in                        -> منبع دوم برای صحت‌سنجی دما/وضعیت [بدون نیاز به کلید]
  4) WeatherAPI.com (اختیاری)       -> فقط اگر WEATHER_API_KEY تنظیم شده باشد

اجرا روی Railway به عنوان Worker (نه Web Service) — رجوع کنید به Procfile.
"""

import os
import asyncio
import logging
from datetime import datetime
from statistics import mean

import pytz
import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from pyrogram import Client
from pyrogram.enums import ParseMode

# ---------------------------------------------------------------------------
# پیکربندی از طریق Environment Variables
# ---------------------------------------------------------------------------
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_STRING = os.environ["SESSION_STRING"]
CHANNEL_ID = os.environ["CHANNEL_ID"]  # مثلاً "-1001234567890" یا "@my_channel"
WEATHER_API_KEY = os.environ.get("WEATHER_API_KEY")  # اختیاری

# حالت زمان‌بندی گزارش:
#   "interval" -> هر N ساعت یک‌بار ارسال می‌شود (پیش‌فرض، هر ۱ ساعت)
#   "daily"    -> فقط یک‌بار در روز، در ساعت مشخص، ارسال می‌شود
REPORT_MODE = os.environ.get("REPORT_MODE", "interval").lower()

# برای حالت interval: هر چند ساعت یک‌بار گزارش ارسال شود (پیش‌فرض: هر ۱ ساعت)
REPORT_INTERVAL_HOURS = int(os.environ.get("REPORT_INTERVAL_HOURS", 1))

# برای حالت daily: ساعت و دقیقه ارسال گزارش (به وقت تهران)
REPORT_HOUR = int(os.environ.get("REPORT_HOUR", 8))
REPORT_MINUTE = int(os.environ.get("REPORT_MINUTE", 0))

# اگر True باشد، بلافاصله پس از بالا آمدن ربات یک گزارش تست ارسال می‌شود
RUN_ON_START = os.environ.get("RUN_ON_START", "false").lower() == "true"

TEHRAN_LAT, TEHRAN_LON = 35.6892, 51.3890
TIMEZONE = pytz.timezone("Asia/Tehran")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("weather-bot")

# ---------------------------------------------------------------------------
# جدول تبدیل کد وضعیت هوا (WMO) به متن و ایموجی
# ---------------------------------------------------------------------------
WMO_CODES = {
    0: ("آسمان صاف", "☀️"),
    1: ("عمدتاً صاف", "🌤️"),
    2: ("نیمه‌ابری", "⛅"),
    3: ("ابری", "☁️"),
    45: ("مه‌آلود", "🌫️"),
    48: ("مه یخ‌زده", "🌫️"),
    51: ("نم‌نم باران سبک", "🌦️"),
    53: ("نم‌نم باران", "🌦️"),
    55: ("نم‌نم باران شدید", "🌧️"),
    56: ("باران یخ‌زدهٔ سبک", "🌧️"),
    57: ("باران یخ‌زدهٔ شدید", "🌧️"),
    61: ("باران سبک", "🌧️"),
    63: ("باران متوسط", "🌧️"),
    65: ("باران شدید", "🌧️"),
    66: ("باران یخ‌زدهٔ سبک", "🌧️"),
    67: ("باران یخ‌زدهٔ شدید", "🌧️"),
    71: ("برف سبک", "❄️"),
    73: ("برف متوسط", "❄️"),
    75: ("برف شدید", "❄️"),
    77: ("دانه‌های برف", "❄️"),
    80: ("رگبار سبک", "🌦️"),
    81: ("رگبار متوسط", "🌧️"),
    82: ("رگبار شدید", "⛈️"),
    85: ("رگبار برف سبک", "🌨️"),
    86: ("رگبار برف شدید", "🌨️"),
    95: ("رعدوبرق", "⛈️"),
    96: ("رعدوبرق همراه با تگرگ سبک", "⛈️"),
    99: ("رعدوبرق همراه با تگرگ شدید", "⛈️"),
}


def describe_weather_code(code):
    return WMO_CODES.get(code, ("نامشخص", "❔"))


def describe_aqi(aqi):
    """European AQI scale used by Open-Meteo."""
    if aqi is None:
        return "نامشخص", "❔"
    if aqi <= 20:
        return "خوب", "🟢"
    if aqi <= 40:
        return "قابل قبول", "🟡"
    if aqi <= 60:
        return "متوسط", "🟠"
    if aqi <= 80:
        return "ناسالم برای گروه‌های حساس", "🔴"
    if aqi <= 100:
        return "ناسالم", "🟣"
    return "بسیار ناسالم/خطرناک", "🟤"


# ---------------------------------------------------------------------------
# دریافت داده از منابع مختلف
# ---------------------------------------------------------------------------
async def fetch_open_meteo(session: aiohttp.ClientSession):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": TEHRAN_LAT,
        "longitude": TEHRAN_LON,
        "daily": "temperature_2m_max,temperature_2m_min,weathercode,"
        "relative_humidity_2m_mean,wind_speed_10m_max",
        "timezone": "Asia/Tehran",
        "forecast_days": 1,
    }
    try:
        async with session.get(url, params=params, timeout=15) as resp:
            resp.raise_for_status()
            data = await resp.json()
            daily = data["daily"]
            return {
                "source": "Open-Meteo",
                "temp_max": daily["temperature_2m_max"][0],
                "temp_min": daily["temperature_2m_min"][0],
                "weather_code": daily["weathercode"][0],
                "humidity": daily.get("relative_humidity_2m_mean", [None])[0],
                "wind": daily.get("wind_speed_10m_max", [None])[0],
            }
    except Exception as e:
        logger.warning(f"Open-Meteo forecast fetch failed: {e}")
        return None


async def fetch_air_quality(session: aiohttp.ClientSession):
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    params = {
        "latitude": TEHRAN_LAT,
        "longitude": TEHRAN_LON,
        "current": "european_aqi",
        "timezone": "Asia/Tehran",
    }
    try:
        async with session.get(url, params=params, timeout=15) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data.get("current", {}).get("european_aqi")
    except Exception as e:
        logger.warning(f"Air quality fetch failed: {e}")
        return None


async def fetch_wttr(session: aiohttp.ClientSession):
    """منبع دوم و مستقل برای صحت‌سنجی."""
    url = "https://wttr.in/Tehran"
    params = {"format": "j1"}
    try:
        async with session.get(url, params=params, timeout=15) as resp:
            resp.raise_for_status()
            data = await resp.json()
            today = data["weather"][0]
            current = data["current_condition"][0]
            return {
                "source": "wttr.in",
                "temp_max": float(today["maxtempC"]),
                "temp_min": float(today["mintempC"]),
                "condition": current["weatherDesc"][0]["value"],
                "humidity": float(current["humidity"]),
                "wind_kmph": float(current["windspeedKmph"]),
            }
    except Exception as e:
        logger.warning(f"wttr.in fetch failed: {e}")
        return None


async def fetch_weatherapi(session: aiohttp.ClientSession):
    """منبع سوم اختیاری — فقط اگر WEATHER_API_KEY تنظیم شده باشد."""
    if not WEATHER_API_KEY:
        return None
    url = "https://api.weatherapi.com/v1/forecast.json"
    params = {"key": WEATHER_API_KEY, "q": "Tehran", "days": 1, "aqi": "no"}
    try:
        async with session.get(url, params=params, timeout=15) as resp:
            resp.raise_for_status()
            data = await resp.json()
            day = data["forecast"]["forecastday"][0]["day"]
            return {
                "source": "WeatherAPI",
                "temp_max": day["maxtemp_c"],
                "temp_min": day["mintemp_c"],
                "condition": day["condition"]["text"],
                "humidity": day.get("avghumidity"),
                "wind_kmph": day.get("maxwind_kph"),
            }
    except Exception as e:
        logger.warning(f"WeatherAPI fetch failed: {e}")
        return None


# ---------------------------------------------------------------------------
# ترکیب و ساخت گزارش نهایی
# ---------------------------------------------------------------------------
def build_report(open_meteo, aqi, wttr, weatherapi) -> str:
    now = datetime.now(TIMEZONE)
    weekday_fa = {
        0: "دوشنبه", 1: "سه‌شنبه", 2: "چهارشنبه", 3: "پنجشنبه",
        4: "جمعه", 5: "شنبه", 6: "یکشنبه",
    }[now.weekday()]
    date_str = f"{weekday_fa} {now.strftime('%Y/%m/%d')} — ساعت {now.strftime('%H:%M')}"

    # --- جمع‌آوری دماهای معتبر از همهٔ منابع برای میانگین‌گیری ---
    max_temps, min_temps, humidities = [], [], []
    sources_used = []

    if open_meteo:
        max_temps.append(open_meteo["temp_max"])
        min_temps.append(open_meteo["temp_min"])
        if open_meteo["humidity"] is not None:
            humidities.append(open_meteo["humidity"])
        sources_used.append("Open-Meteo")

    if wttr:
        max_temps.append(wttr["temp_max"])
        min_temps.append(wttr["temp_min"])
        humidities.append(wttr["humidity"])
        sources_used.append("wttr.in")

    if weatherapi:
        max_temps.append(weatherapi["temp_max"])
        min_temps.append(weatherapi["temp_min"])
        if weatherapi["humidity"]:
            humidities.append(weatherapi["humidity"])
        sources_used.append("WeatherAPI")

    if not max_temps:
        return (
            "⚠️ <b>خطا در دریافت اطلاعات آب‌وهوا</b>\n"
            "متأسفانه هیچ‌کدام از منابع در دسترس پاسخ ندادند. لطفاً بعداً بررسی کنید."
        )

    temp_max = round(mean(max_temps), 1)
    temp_min = round(mean(min_temps), 1)
    humidity = round(mean(humidities)) if humidities else None

    # وضعیت کلی هوا از منبع اصلی (Open-Meteo) در صورت وجود
    if open_meteo:
        condition_text, condition_emoji = describe_weather_code(open_meteo["weather_code"])
        wind = open_meteo["wind"]
    elif wttr:
        condition_text, condition_emoji = wttr["condition"], "🌡️"
        wind = wttr.get("wind_kmph")
    else:
        condition_text, condition_emoji = weatherapi["condition"], "🌡️"
        wind = weatherapi.get("wind_kmph")

    # هشدار اختلاف قابل توجه بین منابع (صحت‌سنجی متقابل)
    disagreement_note = ""
    if len(max_temps) > 1 and (max(max_temps) - min(max_temps) > 4):
        disagreement_note = (
            "\n⚠️ <i>توجه: اختلاف محسوسی بین پیش‌بینی منابع مختلف مشاهده شد؛ "
            "عدد نمایش‌داده‌شده میانگین منابع است.</i>\n"
        )

    aqi_text, aqi_emoji = describe_aqi(aqi)

    # --- تحلیل کوتاه خودکار ---
    if temp_max >= 35:
        analysis = "هوای امروز بسیار گرم است؛ از فعالیت طولانی زیر آفتاب در ساعات میانی روز خودداری کنید."
    elif temp_max >= 28:
        analysis = "روز گرمی در پیش است؛ نوشیدن آب کافی و استفاده از ضدآفتاب توصیه می‌شود."
    elif temp_min <= 5:
        analysis = "هوا نسبتاً سرد است؛ پوشیدن لباس گرم به‌ویژه در ساعات ابتدایی و انتهایی روز فراموش نشود."
    elif "باران" in condition_text or "rain" in condition_text.lower():
        analysis = "احتمال بارش وجود دارد؛ همراه داشتن چتر خالی از لطف نیست."
    else:
        analysis = "شرایط جوی امروز نسبتاً معتدل و مناسب برای فعالیت‌های روزمره است."

    report = (
        f"🌆 <b>گزارش آب‌وهوای تهران</b>\n"
        f"🗓️ {date_str}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"{condition_emoji} <b>وضعیت کلی:</b> {condition_text}\n"
        f"🌡️ <b>دما:</b> حداقل {temp_min}°C   |   حداکثر {temp_max}°C\n"
    )
    if humidity is not None:
        report += f"💧 <b>رطوبت:</b> {humidity}%\n"
    if wind:
        report += f"🍃 <b>سرعت باد:</b> {round(wind)} km/h\n"
    report += f"{aqi_emoji} <b>کیفیت هوا (AQI):</b> {aqi if aqi is not None else 'نامشخص'} ({aqi_text})\n"
    report += disagreement_note
    report += (
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📝 <b>تحلیل کوتاه:</b>\n{analysis}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔎 <i>منابع:</i> {', '.join(sources_used)}"
        + (" ,Open-Meteo Air Quality" if aqi is not None else "")
    )
    return report


# ---------------------------------------------------------------------------
# منطق ارسال گزارش
# ---------------------------------------------------------------------------
async def send_daily_report(app: Client):
    logger.info("در حال دریافت اطلاعات آب‌وهوا از منابع مختلف...")
    async with aiohttp.ClientSession() as session:
        open_meteo, aqi, wttr, weatherapi = await asyncio.gather(
            fetch_open_meteo(session),
            fetch_air_quality(session),
            fetch_wttr(session),
            fetch_weatherapi(session),
        )

    report_text = build_report(open_meteo, aqi, wttr, weatherapi)

    try:
        await app.send_message(CHANNEL_ID, report_text, parse_mode=ParseMode.HTML)
        logger.info("گزارش با موفقیت ارسال شد.")
    except Exception as e:
        logger.error(f"ارسال گزارش به کانال ناموفق بود: {e}")


# ---------------------------------------------------------------------------
# راه‌اندازی کلاینت و زمان‌بند
# ---------------------------------------------------------------------------
app = Client(
    "weather_selfbot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    in_memory=True,  # روی Railway نیازی به ذخیره فایل session نیست
)


async def main():
    async with app:
        me = await app.get_me()
        logger.info(f"با اکانت «{me.first_name}» با موفقیت وارد شدیم.")

        scheduler = AsyncIOScheduler(timezone=TIMEZONE)

        if REPORT_MODE == "daily":
            scheduler.add_job(
                send_daily_report,
                trigger=CronTrigger(hour=REPORT_HOUR, minute=REPORT_MINUTE),
                args=[app],
                id="weather_report",
            )
            logger.info(
                f"زمان‌بند فعال شد (حالت daily). گزارش هر روز ساعت "
                f"{REPORT_HOUR:02d}:{REPORT_MINUTE:02d} (به وقت تهران) ارسال خواهد شد."
            )
        else:
            scheduler.add_job(
                send_daily_report,
                trigger=IntervalTrigger(hours=REPORT_INTERVAL_HOURS),
                args=[app],
                id="weather_report",
                next_run_time=datetime.now(TIMEZONE),  # اولین اجرا بلافاصله
            )
            logger.info(
                f"زمان‌بند فعال شد (حالت interval). گزارش هر "
                f"{REPORT_INTERVAL_HOURS} ساعت یک‌بار ارسال خواهد شد."
            )

        scheduler.start()

        if RUN_ON_START and REPORT_MODE == "daily":
            # در حالت interval، اولین اجرا خودش بلافاصله انجام می‌شود (نیازی به این پرچم نیست)
            logger.info("RUN_ON_START فعال است؛ ارسال گزارش تستی...")
            await send_daily_report(app)

        # ربات را تا ابد در حال اجرا نگه می‌دارد
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
