import os
import logging
import secrets
from datetime import datetime
from dotenv import load_dotenv
from flask import Flask, request, render_template, send_from_directory

# Импорт Celery-задач (где происходит вся логика)
from tasks import log_to_server_logs, save_client_data

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
load_dotenv()

app = Flask(__name__)

# Ключ для сессии (если вообще нужна Flask‑сессия)
# Если хотим полностью отказаться от сессии — можно убрать
app.secret_key = os.getenv("FLASK_SECRET_KEY", "fallback-dev-key")

# (Опционально) Ограничиваем загрузку файлов
app.config["MAX_CONTENT_LENGTH"] = 15 * 1024 * 1024


#############################################
# 1. Логика server_logs
#############################################
@app.before_request
def handle_server_logging():
    """
    Просто собираем сырые поля и передаём их Celery‑задаче log_to_server_logs.
    """
    # Если НЕ хотим логировать каждый запрос — проверяем path:
    monitored_paths = [
        "/",
        "/about",
        "/contact",
        "/services",
        "/foto_na_document",
        "/portfolio",
        "/prices",
        "/faq",
        "/blog",
        "/terms",
        "/privacy",
        "/document_plus",
    ]
    if request.path not in monitored_paths:
        return

    # Сырые данные:
    ip_address = request.headers.get("X-Real-IP", request.remote_addr)
    user_agent = request.headers.get("User-Agent", "")
    referer = request.headers.get("Referer", "")
    accept_language = request.headers.get("Accept-Language", "")
    cookies_str = "; ".join(f"{k}={v}" for k, v in request.cookies.items())
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    path = request.path

    # Берём cookie uid (если есть)
    cookie_uid = request.cookies.get("uid", "")

    # Берём fingerprint из ... (если хотим, но обычно он есть только в клиентских логах)
    # Допустим его тут нет — пусть будет пусто
    fingerprint = ""

    url_query = request.query_string.decode(
        "utf-8"
    )  # получаем "utm_source=google&utm_medium=cpc..."

    # Собираем словарь
    log_data = {
        "ip_address": ip_address,
        "user_agent": user_agent,
        "referer": referer,
        "accept_language": accept_language,
        "request_time": now_str,
        "cookies": cookies_str,
        "path": path,
        "cookie_uid": cookie_uid,
        "fingerprint": fingerprint,
        "url_query": url_query,  # <-- новенькое
        # user_id / config_id мы НЕ генерируем здесь;
        # пусть Celery это делает.
    }

    # Отправляем в Celery (асинхронно)
    log_to_server_logs.delay(log_data)


#############################################
# 2. Устанавливаем cookie uid, если нет
#############################################
@app.after_request
def ensure_uid_cookie(response):
    """
    Добавляем cookie 'uid' на год, если её нет.
    """
    if "uid" not in request.cookies:
        new_uid = secrets.token_hex(16)
        response.set_cookie(
            "uid",
            new_uid,
            max_age=31536000,  # 1 год
            secure=True,  # если https
            httponly=False,  # разрешаем JS читать (по желанию)
            samesite="Lax",
        )
    return response


#############################################
# 3. Страницы сайта
#############################################
@app.route("/", methods=["GET"])
def main_page():
    return render_template("main_page.html")


@app.route("/foto_na_document", methods=["GET"])
def foto_na_document():
    return render_template("foto_na_document.html")

@app.route("/document_plus", methods=["GET"])
def document_plus():
    return render_template("document_plus.html")

@app.route("/compare_woled_qdoled", methods=["GET"])
def compare_woled_qdoled():
    return render_template("compare_woled_qdoled.html")

@app.route("/sitemap.xml", methods=["GET"])
def sitemap():
    """
    Обслуживает файл sitemap.xml из папки static.
    """
    return send_from_directory(app.static_folder, "sitemap.xml", mimetype="application/xml")


@app.route("/robots.txt", methods=["GET"])
def robots():
    """
    Обслуживает файл robots.txt из папки static.
    """
    return send_from_directory(app.static_folder, "robots.txt", mimetype="text/plain")


#############################################
# 4. Фронтенд-логи: /collect_frontend_data
#############################################
@app.route("/collect_frontend_data", methods=["POST"])
def collect_frontend_data():
    """
    JS присылает fingerprint и т.д.
    Собираем сырые поля → save_client_data Celery.
    """
    try:
        data_from_js = request.get_json(force=True)
    except:
        data_from_js = {}

    ip_address = request.headers.get("X-Real-IP", request.remote_addr)
    user_agent = request.headers.get("User-Agent", "")
    accept_language = request.headers.get("Accept-Language", "")
    cookies_str = "; ".join(f"{k}={v}" for k, v in request.cookies.items())
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cookie_uid = request.cookies.get("uid", "")

    # Собираем:
    front_data = {
        "ip_address": ip_address,
        "user_agent": user_agent,
        "accept_language": accept_language,
        "cookies_str": cookies_str,
        "timestamp": now_str,
        "cookie_uid": cookie_uid,
        # всё, что прилетело от JS (fingerprint, geolocation ...)
        "front_json": data_from_js,
    }

    # Отправляем Celery‑задачу
    save_client_data.delay(front_data)

    return {"status": "ok"}, 200


#############################################
# Запуск
#############################################
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
