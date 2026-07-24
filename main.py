"""
Weather Telegram Bot - Daily Weather Report
Author: Custom Bot
Deploy: Railway
"""

import os
import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import aiohttp
from pyrogram import Client
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ============ CONFIG ============
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
CHANNEL_ID = os.environ.get("CHANNEL_ID", "")  # @channel or -100xxxxxxxxxx
WEATHER_API_KEY = os.environ.get("WEATHER_API_KEY", "")  # Optional: OpenWeatherMap

# Tehran coordinates
TEHRAN_LAT = 35.6892
TEHRAN_LON = 51.3890
TEHRAN_TZ = ZoneInfo("Asia/Tehran")

# Schedule: 08:00 Tehran time daily
SCHEDULE_HOUR = int(os.environ.get("SCHEDULE_HOUR", "8"))
SCHEDULE_MINUTE = int(os.environ.get("SCHEDULE_MINUTE", "0"))

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("weather-bot")


# ============ CLIENT ============
app = Client(
    name="weather-bot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
)


# ============ WEATHER FETCHERS ============
async def fetch_open_meteo(session: aiohttp.ClientSession) -> dict:
    """Fetch weather from Open-Meteo (free, no API key required)."""
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={TEHRAN_LAT}&longitude={TEHRAN_LON}"
        "&current=temperature_2m,relative_humidity_2m,apparent_temperature,"
        "weather_code,wind_speed_10m,wind_direction_10m"
        "&daily=temperature_2m_max,temperature_2m_min,weather_code,"
        "precipitation_sum,wind_speed_10m_max,sunrise,sunset,uv_index_max"
        "&timezone=Asia%2FTehran&forecast_days=1"
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                data = await resp.json()
                return {"source": "Open-Meteo", "data": data, "ok": True}
            return {"source": "Open-Meteo", "ok": False, "error": f"HTTP {resp.status}"}
    except Exception as e:
        return {"source": "Open-Meteo", "ok": False, "error": str(e)}


async def fetch_open_meteo_air_quality(session: aiohttp.ClientSession) -> dict:
    """Fetch air quality from Open-Meteo Air Quality API."""
    url = (
        "https://air-quality-api.open-meteo.com/v1/air-quality"
        f"?latitude={TEHRAN_LAT}&longitude={TEHRAN_LON}"
        "&current=european_aqi,us_aqi,pm10,pm2_5,carbon_monoxide,nitrogen_dioxide"
        "&timezone=Asia%2FTehran"
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                data = await resp.json()
                return {"source": "Open-Meteo AQI", "data": data, "ok": True}
            return {"source": "Open-Meteo AQI", "ok": False, "error": f"HTTP {resp.status}"}
    except Exception as e:
        return {"source": "Open-Meteo AQI", "ok": False, "error": str(e)}


async def fetch_wttr(session: aiohttp.ClientSession) -> dict:
    """Fetch weather from wttr.in (free, no API key)."""
    url = "https://wttr.in/Tehran?format=j1"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                data = await resp.json()
                return {"source": "wttr.in", "data": data, "ok": True}
            return {"source": "wttr.in", "ok": False, "error": f"HTTP {resp.status}"}
    except Exception as e:
        return {"source": "wttr.in", "ok": False, "error": str(e)}


async def fetch_openweathermap(session: aiohttp.ClientSession) -> dict:
    """Fetch weather from OpenWeatherMap (requires API key)."""
    if not WEATHER_API_KEY:
        return {"source": "OpenWeatherMap", "ok": False, "error": "No API key"}
    url = (
        "https://api.openweathermap.org/data/2.5/weather"
        f"?lat={TEHRAN_LAT}&lon={TEHRAN_LON}&appid={WEATHER_API_KEY}"
        "&units=metric&lang=fa"
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                data = await resp.json()
                return {"source": "OpenWeatherMap", "data": data, "ok": True}
            return {"source": "OpenWeatherMap", "ok": False, "error": f"HTTP {resp.status}"}
    except Exception as e:
        return {"source": "OpenWeatherMap", "ok": False, "error": str(e)}


# ============ WEATHER CODE MAPPING ============
WMO_CODES = {
    0: ("☀️ آفتابی صاف", "☀️"),
    1: ("🌤️ عمدتاً آفتابی", "🌤️"),
    2: ("⛅ نیمه‌ابری", "⛅"),
    3: ("☁️ ابری", "☁️"),
    45: ("🌫️ مه‌آلود", "🌫️"),
    48: ("🌫️ مه یخ‌زده", "🌫️"),
    51: ("🌦️ نم‌نم باران خفیف", "🌦️"),
    53: ("🌦️ نم‌نم باران", "🌦️"),
    55: ("🌧️ نم‌نم باران شدید", "🌧️"),
    61: ("🌧️ باران خفیف", "🌧️"),
    63: ("🌧️ باران متوسط", "🌧️"),
    65: ("🌧️ باران شدید", "🌧️"),
    71: ("🌨️ برف خفیف", "🌨️"),
    73: ("🌨️ برف متوسط", "🌨️"),
    75: ("❄️ برف شدید", "❄️"),
    77: ("🌨️ دانه‌های برف", "🌨️"),
    80: ("🌦️ رگبار خفیف", "🌦️"),
    81: ("🌧️ رگبار متوسط", "🌧️"),
    82: ("⛈️ رگبار شدید", "⛈️"),
    85: ("🌨️ رگبار برف خفیف", "🌨️"),
    86: ("🌨️ رگبار برف شدید", "🌨️"),
    95: ("⛈️ رعدوبرق", "⛈️"),
    96: ("⛈️ رعدوبرق با تگرگ خفیف", "⛈️"),
    99: ("⛈️ رعدوبرق با تگرگ شدید", "⛈️"),
}


def get_weather_desc(code: int) -> tuple:
    return WMO_CODES.get(code, ("🌡️ نامشخص", "🌡️"))


def aqi_level(us_aqi: float) -> tuple:
    """Return (label, emoji, color-like description)."""
    if us_aqi <= 50:
        return ("پاک", "🟢")
    if us_aqi <= 100:
        return ("متوسط", "🟡")
    if us_aqi <= 150:
        return ("ناسالم برای گروه‌های حساس", "🟠")
    if us_aqi <= 200:
        return ("ناسالم", "🔴")
    if us_aqi <= 300:
        return ("بسیار ناسالم", "🟣")
    return ("خطرناک", "🟤")


# ============ ANALYSIS & REPORT ============
async def build_weather_report() -> str:
    """Fetch from multiple sources and build a unified report."""
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_open_meteo(session),
            fetch_open_meteo_air_quality(session),
            fetch_wttr(session),
            fetch_openweathermap(session),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Filter successful results
    ok_results = [r for r in results if isinstance(r, dict) and r.get("ok")]
    failed = [r for r in results if isinstance(r, dict) and not r.get("ok")]

    for f in failed:
        log.warning(f"Source failed: {f.get('source')} -> {f.get('error')}")

    if not ok_results:
        return "❌ <b>خطا در دریافت اطلاعات آب‌وهوا</b>\n\nهیچ‌یک از منابع در دسترس نبودند."

    # Extract data from sources
    open_meteo_data = None
    aqi_data = None
    wttr_data = None
    owm_data = None

    for r in ok_results:
        if r["source"] == "Open-Meteo":
            open_meteo_data = r["data"]
        elif r["source"] == "Open-Meteo AQI":
            aqi_data = r["data"]
        elif r["source"] == "wttr.in":
            wttr_data = r["data"]
        elif r["source"] == "OpenWeatherMap":
            owm_data = r["data"]

    # ===== Temperature =====
    temps = []
    if open_meteo_data:
        try:
            t_max = open_meteo_data["daily"]["temperature_2m_max"][0]
            t_min = open_meteo_data["daily"]["temperature_2m_min"][0]
            t_cur = open_meteo_data["current"]["temperature_2m"]
            temps.append({"src": "Open-Meteo", "cur": t_cur, "min": t_min, "max": t_max})
        except Exception:
            pass
    if wttr_data:
        try:
            cur = wttr_data["current_condition"][0]
            today = wttr_data["weather"][0]
            temps.append({
                "src": "wttr.in",
                "cur": float(cur["temp_C"]),
                "min": float(today["mintempC"]),
                "max": float(today["maxtempC"]),
            })
        except Exception:
            pass
    if owm_data:
        try:
            temps.append({
                "src": "OpenWeatherMap",
                "cur": owm_data["main"]["temp"],
                "min": owm_data["main"]["temp_min"],
                "max": owm_data["main"]["temp_max"],
            })
        except Exception:
            pass

    # Average temperatures
    if temps:
        avg_cur = sum(t["cur"] for t in temps) / len(temps)
        avg_min = sum(t["min"] for t in temps) / len(temps)
        avg_max = sum(t["max"] for t in temps) / len(temps)
    else:
        avg_cur = avg_min = avg_max = None

    # ===== Humidity & Wind =====
    humidities = []
    winds = []
    if open_meteo_data:
        try:
            humidities.append(open_meteo_data["current"]["relative_humidity_2m"])
            winds.append(open_meteo_data["current"]["wind_speed_10m"])
        except Exception:
            pass
    if wttr_data:
        try:
            cur = wttr_data["current_condition"][0]
            humidities.append(int(cur["humidity"]))
            winds.append(float(cur["windspeedKmph"]))
        except Exception:
            pass
    if owm_data:
        try:
            humidities.append(owm_data["main"]["humidity"])
            winds.append(owm_data["wind"]["speed"] * 3.6)  # m/s -> km/h
        except Exception:
            pass

    avg_humidity = sum(humidities) / len(humidities) if humidities else None
    avg_wind = sum(winds) / len(winds) if winds else None

    # ===== Weather condition =====
    condition = "🌡️ نامشخص"
    condition_emoji = "🌡️"
    if open_meteo_data:
        try:
            code = open_meteo_data["current"]["weather_code"]
            condition, condition_emoji = get_weather_desc(code)
        except Exception:
            pass
    elif owm_data:
        try:
            desc = owm_data["weather"][0]["description"]
            condition = desc.capitalize()
            condition_emoji = "🌤️"
        except Exception:
            pass
    elif wttr_data:
        try:
            desc = wttr_data["current_condition"][0]["weatherDesc"][0]["value"]
            condition = desc
            condition_emoji = "🌤️"
        except Exception:
            pass

    # ===== Air Quality =====
    aqi_value = None
    aqi_label = None
    aqi_emoji = None
    pm25 = None
    pm10 = None
    if aqi_data:
        try:
            cur = aqi_data["current"]
            aqi_value = cur.get("us_aqi")
            if aqi_value is not None:
                aqi_label, aqi_emoji = aqi_level(aqi_value)
            pm25 = cur.get("pm2_5")
            pm10 = cur.get("pm10")
        except Exception:
            pass

    # ===== Sunrise / Sunset / UV =====
    sunrise = sunset = uv_index = None
    if open_meteo_data:
        try:
            sunrise = open_meteo_data["daily"]["sunrise"][0].split("T")[1]
            sunset = open_meteo_data["daily"]["sunset"][0].split("T")[1]
            uv_index = open_meteo_data["daily"]["uv_index_max"][0]
        except Exception:
            pass

    # ===== Build report (HTML) =====
    now = datetime.now(TEHRAN_TZ)
    date_str = now.strftime("%Y/%m/%d")
    time_str = now.strftime("%H:%M")
    sources_used = len(temps)

    # Analysis summary
    if avg_cur is not None:
        if avg_cur >= 35:
            analysis = "🔥 هوای گرم و نیاز به مراقبت در برابر گرما؛ مصرف آب را فراموش نکنید."
        elif avg_cur >= 25:
            analysis = "🌡️ هوای معتدل و دلپذیر؛ شرایط مناسبی برای فعالیت‌های بیرون از منزل."
        elif avg_cur >= 15:
            analysis = "🍃 هوای خنک؛ پیشنهاد می‌شود لباس گرم همراه داشته باشید."
        else:
            analysis = "❄️ هوای سرد؛ از پوشش مناسب استفاده کنید و مراقب یخ‌زدگی معابر باشید."
    else:
        analysis = "📊 اطلاعات کافی برای تحلیل در دسترس نبود."

    # Build message
    lines = []
    lines.append(f"🌆 <b>گزارش آب‌وهوای تهران</b>")
    lines.append(f"📅 <i>{date_str}</i>  ⏰ <i>{time_str}</i>")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append(f"{condition_emoji} <b>وضعیت کلی:</b> {condition}")
    lines.append("")
    lines.append("🌡️ <b>دما:</b>")
    if avg_cur is not None:
        lines.append(f"   • لحظه‌ای: <b>{avg_cur:.1f}°C</b>")
        lines.append(f"   • حداقل: <b>{avg_min:.1f}°C</b>  |  حداکثر: <b>{avg_max:.1f}°C</b>")
    lines.append("")
    if avg_humidity is not None:
        lines.append(f"💧 <b>رطوبت:</b> {avg_humidity:.0f}%")
    if avg_wind is not None:
        lines.append(f"💨 <b>سرعت باد:</b> {avg_wind:.1f} km/h")
    lines.append("")
    if aqi_value is not None:
        lines.append(f"🫁 <b>کیفیت هوا (US AQI):</b> {aqi_emoji} <b>{int(aqi_value)}</b> — {aqi_label}")
        if pm25 is not None:
            lines.append(f"   • PM2.5: <code>{pm25:.1f}</code> µg/m³")
        if pm10 is not None:
            lines.append(f"   • PM10: <code>{pm10:.1f}</code> µg/m³")
        lines.append("")
    if sunrise:
        lines.append(f"🌅 <b>طلوع:</b> {sunrise}   |   🌇 <b>غروب:</b> {sunset}")
    if uv_index is not None:
        lines.append(f"🔆 <b>شاخص UV:</b> {uv_index:.1f}")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"📝 <b>تحلیل:</b> {analysis}")
    lines.append("")
    lines.append(f"🔗 <i>منابع: {sources_used} سرویس (Open-Meteo, wttr.in, OpenWeatherMap)</i>")
    lines.append("🤖 <i>ارسال خودکار توسط ربات</i>")

    return "\n".join(lines)


# ============ JOB ============
async def send_daily_report():
    """Scheduled job: build and send the weather report."""
    log.info("Running daily weather report job...")
    try:
        if not app.is_connected:
            await app.start()
        report = await build_weather_report()
        await app.send_message(CHANNEL_ID, report, disable_web_page_preview=True)
        log.info("Weather report sent successfully.")
    except Exception as e:
        log.exception(f"Failed to send weather report: {e}")


# ============ MAIN ============
async def main():
    log.info("Starting Weather Bot...")
    await app.start()
    log.info(f"Logged in as {(await app.get_me()).first_name}")

    # Schedule daily job
    scheduler = AsyncIOScheduler(timezone=TEHRAN_TZ)
    scheduler.add_job(
        send_daily_report,
        trigger=CronTrigger(hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE),
        id="daily_weather",
        name="Daily Weather Report",
        replace_existing=True,
    )
    scheduler.start()
    log.info(f"Scheduler started. Next run at {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} Tehran time.")

    # Optional: send a test report on startup (disable in production)
    if os.environ.get("SEND_ON_START", "0") == "1":
        await send_daily_report()

    # Keep alive
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        log.info("Shutting down...")
    finally:
        scheduler.shutdown(wait=False)
        await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
