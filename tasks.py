import os
import json
import logging
import pymysql
import uuid

from dotenv import load_dotenv
from celery import Celery
from celery.schedules import crontab
from device_detector import DeviceDetector
from urllib.parse import parse_qs
from datetime import datetime, timedelta
from collections import defaultdict

logging.basicConfig(level=logging.DEBUG)

load_dotenv()

# --------------------------------------------------------------------------------
# Redis / MySQL Config
# --------------------------------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "mydb")

# --------------------------------------------------------------------------------
# Celery
# --------------------------------------------------------------------------------
celery_app = Celery(
    "mytasks",
    broker=f"redis://{REDIS_HOST}:{REDIS_PORT}/0",
    backend=f"redis://{REDIS_HOST}:{REDIS_PORT}/1",
)
celery_app.conf.update(
    result_expires=3600,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    broker_connection_retry_on_startup=True,
)
celery_app.conf.beat_schedule = {
    "aggregate_data_every_hour": {
        "task": "tasks.aggregate_data",
        "schedule": crontab(minute="*/5"),
    }
}


def get_db_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def is_bot_user_agent(user_agent: str) -> bool:
    if not user_agent:
        return False
    bot_signatures = [
        "Googlebot",
        "YandexBot",
        "bingbot",
        "DuckDuckBot",
        "crawler",
        "spider",
        "AhrefsBot",
        "Bot/",
        "Bot ",
    ]
    ua_upper = user_agent.upper()
    return any(sig.upper() in ua_upper for sig in bot_signatures)


def get_geo_info(ip_address: str) -> dict:
    # Заглушка
    return {}


def generate_visitor_id(cookie_uid: str, fingerprint: str, ip_address: str) -> str:
    if cookie_uid:
        return cookie_uid
    elif fingerprint:
        from hashlib import md5

        raw_str = f"{ip_address}|{fingerprint}"
        return md5(raw_str.encode("utf-8")).hexdigest()
    else:
        return "anonymous"


def parse_all_query_params(url_query: str) -> str:
    try:
        parsed = parse_qs(url_query, keep_blank_values=True)
        result_dict = {}
        for key, value_list in parsed.items():
            if len(value_list) == 1:
                result_dict[key] = value_list[0]
            else:
                result_dict[key] = value_list
        return json.dumps(result_dict, ensure_ascii=False)
    except Exception as e:
        logging.warning(f"Ошибка при разборе query: {e}")
        return "{}"


def enrich_user_agent(user_agent: str) -> dict:
    result = {"parsed_ua": {}, "is_bot": False}
    try:
        detector = DeviceDetector(user_agent).parse()
        result["parsed_ua"] = {
            "client_name": detector.client_name() or "",
            "client_version": detector.client_version() or "",
            "os_name": detector.os_name() or "",
            "os_version": detector.os_version() or "",
            "device_brand": detector.device_brand() or "",
            "device_model": detector.device_model() or "",
            "device_type": detector.device_type() or "",
        }
    except Exception as err:
        logging.warning(f"Ошибка при парсинге User-Agent: {err}")
    result["is_bot"] = is_bot_user_agent(user_agent)
    return result


@celery_app.task
def log_to_server_logs(log_data):
    try:
        ip_address = log_data.get("ip_address", "")
        user_agent = log_data.get("user_agent", "")
        referer = log_data.get("referer", "")
        accept_language = log_data.get("accept_language", "")
        request_time = log_data.get("request_time", "")
        cookies_str = log_data.get("cookies", "")
        path = log_data.get("path", "")
        cookie_uid = log_data.get("cookie_uid", "")
        fingerprint = log_data.get("fingerprint", "")
        url_query = log_data.get("url_query", "")

        visitor_id = generate_visitor_id(cookie_uid, fingerprint, ip_address)
        ua_info = enrich_user_agent(user_agent)
        ua_json = json.dumps(ua_info["parsed_ua"], ensure_ascii=False)
        is_bot = 1 if ua_info["is_bot"] else 0
        parsed_query = parse_all_query_params(url_query) if url_query else "{}"
        geo_info = get_geo_info(ip_address)
        geo_json = json.dumps(geo_info, ensure_ascii=False)

        parsed_utm = log_data.get("parsed_utm", "{}")  # Если нужно

        conn = get_db_connection()
        with conn.cursor() as cur:
            sql = """
            INSERT INTO server_logs (
                visitor_id,
                ip_address,
                user_agent,
                referer,
                accept_language,
                request_time,
                cookies,
                path,
                cookie_uid,
                fingerprint,
                parsed_ua,
                is_bot,
                parsed_geo,
                parsed_query,
                parsed_utm,
                processed
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0)
            """
            cur.execute(
                sql,
                (
                    visitor_id,
                    ip_address,
                    user_agent,
                    referer,
                    accept_language,
                    request_time,
                    cookies_str,
                    path,
                    cookie_uid,
                    fingerprint,
                    ua_json,
                    is_bot,
                    geo_json,
                    parsed_query,
                    parsed_utm,
                ),
            )
    except Exception as e:
        logging.warning(f"Ошибка в log_to_server_logs: {e}")
    finally:
        if "conn" in locals():
            conn.close()


@celery_app.task
def save_client_data(front_data):
    try:
        ip_address = front_data.get("ip_address", "")
        user_agent = front_data.get("user_agent", "")
        accept_language = front_data.get("accept_language", "")
        cookies_str = front_data.get("cookies_str", "")
        now_str = front_data.get("timestamp", "")
        cookie_uid = front_data.get("cookie_uid", "")
        url_query = front_data.get("url_query", "")

        data_json = front_data.get("front_json", {})
        fingerprint = data_json.get("fingerprint", "")

        visitor_id = generate_visitor_id(cookie_uid, fingerprint, ip_address)
        ua_info = enrich_user_agent(user_agent)
        ua_json = json.dumps(ua_info["parsed_ua"], ensure_ascii=False)
        is_bot = 1 if ua_info["is_bot"] else 0
        parsed_query = parse_all_query_params(url_query) if url_query else "{}"
        geo_json = json.dumps(get_geo_info(ip_address), ensure_ascii=False)

        geolocation = data_json.get("geolocation", {})
        battery = data_json.get("battery", {})
        screen = data_json.get("screen", {})
        events = data_json.get("events", [])
        perf_timing = data_json.get("performanceTiming", {})
        parsed_utm = data_json.get("parsed_utm", "{}")  # Если нужно

        geoloc_lat = geolocation.get("latitude")
        geoloc_lng = geolocation.get("longitude")
        screen_w = screen.get("width")
        screen_h = screen.get("height")
        orientation = screen.get("orientation") or data_json.get("orientation", "")
        battery_charging = battery.get("charging")
        battery_level = battery.get("level")
        perf_json = json.dumps(perf_timing, ensure_ascii=False)
        events_json = json.dumps(events, ensure_ascii=False)

        conn = get_db_connection()
        with conn.cursor() as cur:
            sql = """
            INSERT INTO client_logs (
                visitor_id,
                ip_address,
                user_agent,
                accept_language,
                cookies_str,
                timestamp,
                cookie_uid,
                fingerprint,
                parsed_ua,
                is_bot,
                parsed_geo,
                geolocation_lat,
                geolocation_lng,
                screen_width,
                screen_height,
                orientation,
                battery_charging,
                battery_level,
                performance_timing,
                events,
                parsed_query,
                parsed_utm,
                processed
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0)
            """
            cur.execute(
                sql,
                (
                    visitor_id,
                    ip_address,
                    user_agent,
                    accept_language,
                    cookies_str,
                    now_str,
                    cookie_uid,
                    fingerprint,
                    ua_json,
                    is_bot,
                    geo_json,
                    geoloc_lat,
                    geoloc_lng,
                    screen_w,
                    screen_h,
                    orientation,
                    battery_charging,
                    battery_level,
                    perf_json,
                    events_json,
                    parsed_query,
                    parsed_utm,
                ),
            )
    except Exception as e:
        logging.warning(f"Ошибка в save_client_data: {e}")
    finally:
        if "conn" in locals():
            conn.close()
            
def extract_utm_params(parsed_utm_json: str) -> dict:
    """
    Парсит JSON-строку из parsed_utm и возвращает UTM-параметры.
    
    :param parsed_utm_json: JSON-строка с UTM-параметрами.
    :return: Словарь с UTM-параметрами.
    """
    try:
        utm_data = json.loads(parsed_utm_json)
        return {
            "utm_source": utm_data.get("utm_source", ""),
            "utm_medium": utm_data.get("utm_medium", ""),
            "utm_campaign": utm_data.get("utm_campaign", ""),
            "utm_term": utm_data.get("utm_term", ""),
            "utm_content": utm_data.get("utm_content", "")
        }
    except json.JSONDecodeError:
        logging.warning("Неверный JSON в parsed_utm")
        return {
            "utm_source": "",
            "utm_medium": "",
            "utm_campaign": "",
            "utm_term": "",
            "utm_content": ""
        }
        
def extract_ua_params(parsed_ua_json: str) -> dict:
    """
    Парсит JSON-строку из parsed_ua и возвращает отдельные параметры User-Agent.
    
    :param parsed_ua_json: JSON-строка с параметрами User-Agent.
    :return: Словарь с параметрами User-Agent.
    """
    try:
        ua_data = json.loads(parsed_ua_json)
        return {
            "client_name": ua_data.get("client_name", ""),
            "client_version": ua_data.get("client_version", ""),
            "device_brand": ua_data.get("device_brand", ""),
            "device_model": ua_data.get("device_model", ""),
            "device_type": ua_data.get("device_type", ""),
            "os_name": ua_data.get("os_name", ""),
            "os_version": ua_data.get("os_version", "")
        }
    except json.JSONDecodeError:
        logging.warning("Неверный JSON в parsed_ua")
        return {
            "client_name": "",
            "client_version": "",
            "device_brand": "",
            "device_model": "",
            "device_type": "",
            "os_name": "",
            "os_version": ""
        }
        
def extract_query_params(parsed_query_json: str) -> dict:
    """
    Парсит JSON-строку из parsed_query и возвращает стандартные UTM-параметры,
    а также дополнительные параметры, если они присутствуют.
    
    :param parsed_query_json: JSON-строка с query-параметрами.
    :return: Словарь с UTM-параметрами и дополнительными параметрами.
    """
    try:
        query_data = json.loads(parsed_query_json)
    except json.JSONDecodeError:
        logging.warning("Неверный JSON в parsed_query")
        return {
            "utm_source": "",
            "utm_medium": "",
            "utm_campaign": "",
            "utm_term": "",
            "utm_content": "",
            "additional_params": {}
        }
    
    # Извлечение стандартных UTM-параметров
    utm_params = {
        "utm_source": query_data.get("utm_source", ""),
        "utm_medium": query_data.get("utm_medium", ""),
        "utm_campaign": query_data.get("utm_campaign", ""),
        "utm_term": query_data.get("utm_term", ""),
        "utm_content": query_data.get("utm_content", "")
    }
    
    # Извлечение дополнительных параметров
    additional_params = {k: v for k, v in query_data.items() if k not in utm_params}
    
    if additional_params:
        utm_params["additional_params"] = additional_params
    else:
        utm_params["additional_params"] = {}
    
    return utm_params


@celery_app.task
def aggregate_data():
    """
    ETL: Мержим server_logs (processed=0) + client_logs (processed=0)
    и пишем в unified_analytics. Сохраняем все поля,
    включая time_on_page, utm, events, и т.д.
    """

    logging.info("Запуск задачи aggregate_data (merging server+client)")

    try:
        conn = get_db_connection()
        with conn.cursor() as cur:

            # 1) Извлекаем необработанные server_logs
            sql_srv = """
            SELECT
                id,
                visitor_id,
                ip_address,
                user_agent,
                referer,
                accept_language,
                request_time AS event_time,
                cookies,
                path,
                cookie_uid,
                fingerprint,
                parsed_ua,
                is_bot,
                parsed_geo,
                parsed_query,
                parsed_utm,
                processed
            FROM server_logs
            WHERE processed=0
            """
            cur.execute(sql_srv)
            server_rows = cur.fetchall()

            # 2) Извлекаем необработанные client_logs
            sql_clt = """
            SELECT
                id,
                visitor_id,
                ip_address,
                user_agent,
                accept_language,
                cookies_str,
                timestamp AS event_time,
                cookie_uid,
                fingerprint,
                parsed_ua,
                is_bot,
                parsed_geo,
                geolocation_lat,
                geolocation_lng,
                screen_width,
                screen_height,
                orientation,
                battery_charging,
                battery_level,
                performance_timing,
                events,
                parsed_query,
                parsed_utm,
                processed,
                -- Дополнительно: В client_logs у нас нет отдельного time_on_page поля,
                -- но 'timeOnPage' может лежать в JSON. Предположим, оно лежит
                -- в performance_timing или во front_json (тогда надо
                -- сюда JOIN или хранить в data). Если вы уже добавили
                -- столбец time_on_page - селектируйте здесь.
                -- Иначе берём из performance_timing/timeOnPage ниже
                -- referer, path, data -- если добавлены в схему
                referer,
                path,
                data
            FROM client_logs
            WHERE processed=0
            """
            cur.execute(sql_clt)
            client_rows = cur.fetchall()

        # Группируем по visitor_id
        from collections import defaultdict

        srv_by_vid = defaultdict(list)
        clt_by_vid = defaultdict(list)

        for s in server_rows:
            srv_by_vid[s["visitor_id"]].append(s)
        for c in client_rows:
            clt_by_vid[c["visitor_id"]].append(c)

        # Функция парсинга даты/времени (datetime или str)
        def parse_dt(dt):
            if isinstance(dt, datetime):
                return dt
            elif isinstance(dt, str):
                return datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
            else:
                raise ValueError(f"Неверный тип event_time: {type(dt)} => {dt}")

        # Сортируем записи по event_time внутри каждой группы
        for vid in srv_by_vid:
            srv_by_vid[vid].sort(key=lambda x: parse_dt(x["event_time"]))
        for vid in clt_by_vid:
            clt_by_vid[vid].sort(key=lambda x: parse_dt(x["event_time"]))

        used_srv_ids = set()
        used_clt_ids = set()
        merged_rows = []

        # Интервал, в пределах которого считаем событие "одним"
        time_threshold = timedelta(seconds=20)

        # Все visitor_id, встретившиеся в server или client
        all_vids = set(srv_by_vid.keys()) | set(clt_by_vid.keys())

        # ------------------------------------------------------------------
        # Вспомогательные функции
        # ------------------------------------------------------------------
        def get_time_on_page(row: dict) -> int:
            """
            Пытаемся достать timeOnPage (мс) из JSON performance_timing,
            либо row['data'], либо row['front_json'] (зависит от структуры).
            По умолчанию 0.
            """
            # 1) Если в client_logs есть столбец data (JSON), можно искать там
            # 2) Или в performance_timing (JSON) => timeOnPage
            # 3) Либо row['time_on_page'] прямо, если есть

            # Пример: смотрим performance_timing => { "timeOnPage": 12345 }
            perf = row.get("performance_timing")
            if perf and isinstance(perf, dict):
                # timeOnPage может лежать внутри
                if "timeOnPage" in perf:
                    return int(perf["timeOnPage"])
            # Если row["data"] есть, можем json.loads(...) и искать
            d = row.get("data")
            if d and isinstance(d, dict) and "timeOnPage" in d:
                return int(d["timeOnPage"])

            # Если нет — 0
            return 0

        def create_server_only(s, vid):
            t_s = parse_dt(s["event_time"])
            # Все поля, которые хотим хранить
            return {
                "visitor_id": vid,
                "event_time": t_s,
                "source_table": "server_only",
                "is_bot": s["is_bot"],
                "ip_address": s["ip_address"],
                "user_agent": s["user_agent"],
                "referer": s["referer"],
                "path": s["path"],
                "screen_width": None,
                "screen_height": None,
                "battery_level": None,
                "load_time": None,
                "events": None,
                "geolocation_lat": None,
                "geolocation_lng": None,
                "server_event_time": t_s,
                "client_event_time": None,
                "accept_language": s["accept_language"],
                "orientation": None,
                "battery_charging": None,
                "cookies": s["cookies"],
                "cookies_str": None,
                "cookie_uid": s["cookie_uid"],
                "fingerprint": s["fingerprint"],
                "parsed_ua": s["parsed_ua"],
                "parsed_geo": s["parsed_geo"],
                "parsed_query": s["parsed_query"],
                "parsed_utm": s["parsed_utm"],
                "time_on_page": 0,  # На сервере нет info
                "data_json": None,  # Если нужно
            }

        def create_client_only(c, vid):
            t_c = parse_dt(c["event_time"])
            top = get_time_on_page(c)
            return {
                "visitor_id": vid,
                "event_time": t_c,
                "source_table": "client_only",
                "is_bot": c["is_bot"],
                "ip_address": c["ip_address"],
                "user_agent": c["user_agent"],
                "referer": c.get("referer"),
                "path": c.get("path"),
                "screen_width": c["screen_width"],
                "screen_height": c["screen_height"],
                "battery_level": c["battery_level"],
                "load_time": None,  # Если хотите load_time (добывайте из perf)
                "events": c["events"],
                "geolocation_lat": c["geolocation_lat"],
                "geolocation_lng": c["geolocation_lng"],
                "server_event_time": None,
                "client_event_time": t_c,
                "accept_language": c["accept_language"],
                "orientation": c["orientation"],
                "battery_charging": c["battery_charging"],
                "cookies": None,  # c не хранит raw cookies?
                "cookies_str": c["cookies_str"],
                "cookie_uid": c["cookie_uid"],
                "fingerprint": c["fingerprint"],
                "parsed_ua": c["parsed_ua"],
                "parsed_geo": c["parsed_geo"],
                "parsed_query": c["parsed_query"],
                "parsed_utm": c["parsed_utm"],
                "time_on_page": top,
                "data_json": c.get("data"),  # Если есть
            }

        def create_merged(s, c, vid):
            t_s = parse_dt(s["event_time"])
            t_c = parse_dt(c["event_time"])
            top = get_time_on_page(c)
            return {
                "visitor_id": vid,
                "event_time": min(t_s, t_c),
                "source_table": "merged",
                "is_bot": 1 if (s["is_bot"] or c["is_bot"]) else 0,
                "ip_address": s["ip_address"] or c["ip_address"],
                "user_agent": s["user_agent"] or c["user_agent"],
                "referer": s["referer"] or c.get("referer"),
                "path": s["path"] or c.get("path"),
                "screen_width": c["screen_width"],
                "screen_height": c["screen_height"],
                "battery_level": c["battery_level"],
                "load_time": None,  # или вытащить load_time из c["performance_timing"]
                "events": c["events"],
                "geolocation_lat": c["geolocation_lat"],
                "geolocation_lng": c["geolocation_lng"],
                "server_event_time": t_s,
                "client_event_time": t_c,
                "accept_language": s["accept_language"] or c["accept_language"],
                "orientation": c["orientation"],
                "battery_charging": c["battery_charging"],
                "cookies": s["cookies"],
                "cookies_str": c["cookies_str"],
                "cookie_uid": s["cookie_uid"] or c["cookie_uid"],
                "fingerprint": s["fingerprint"] or c["fingerprint"],
                "parsed_ua": s["parsed_ua"] or c["parsed_ua"],
                "parsed_geo": s["parsed_geo"] or c["parsed_geo"],
                "parsed_query": s["parsed_query"] or c["parsed_query"],
                "parsed_utm": s["parsed_utm"] or c["parsed_utm"],
                "time_on_page": top,
                "data_json": c.get("data"),
            }

        # 2. Основной цикл (два указателя)
        for vid in all_vids:
            s_list = srv_by_vid.get(vid, [])
            c_list = clt_by_vid.get(vid, [])
            i, j = 0, 0
            while i < len(s_list) and j < len(c_list):
                s = s_list[i]
                c = c_list[j]
                t_s = parse_dt(s["event_time"])
                t_c = parse_dt(c["event_time"])
                delta = abs(t_s - t_c)
                if delta <= time_threshold:
                    row_merged = create_merged(s, c, vid)
                    merged_rows.append(row_merged)
                    used_srv_ids.add(s["id"])
                    used_clt_ids.add(c["id"])
                    i += 1
                    j += 1
                else:
                    if t_s < t_c:
                        row_srv = create_server_only(s, vid)
                        merged_rows.append(row_srv)
                        used_srv_ids.add(s["id"])
                        i += 1
                    else:
                        row_clt = create_client_only(c, vid)
                        merged_rows.append(row_clt)
                        used_clt_ids.add(c["id"])
                        j += 1

            while i < len(s_list):
                s = s_list[i]
                row_srv = create_server_only(s, vid)
                merged_rows.append(row_srv)
                used_srv_ids.add(s["id"])
                i += 1

            while j < len(c_list):
                c = c_list[j]
                row_clt = create_client_only(c, vid)
                merged_rows.append(row_clt)
                used_clt_ids.add(c["id"])
                j += 1

        # --- 3) Пишем в unified_analytics
        try:
            with conn.cursor() as cur:
                # Вставляем все поля
                ins_sql = """
                INSERT INTO unified_analytics (
                    visitor_id,
                    event_time,
                    source_table,
                    is_bot,
                    ip_address,
                    user_agent,
                    referer,
                    path,
                    screen_width,
                    screen_height,
                    battery_level,
                    load_time,
                    events,
                    geolocation_lat,
                    geolocation_lng,
                    server_event_time,
                    client_event_time,
                    accept_language,
                    orientation,
                    battery_charging,
                    cookies,
                    cookies_str,
                    cookie_uid,
                    fingerprint,
                    parsed_ua,
                    parsed_geo,
                    parsed_query,
                    parsed_utm,
                    time_on_page,
                    data_json
                )
                VALUES (
                    %s,%s,%s,%s,
                    %s,%s,%s,%s,
                    %s,%s,%s,%s,
                    %s,%s,%s,%s,
                    %s,%s,%s,%s,
                    %s,%s,%s,%s,
                    %s,%s,%s,%s,
                    %s,%s
                )
                """
                for row in merged_rows:
                    # events -> JSON
                    events_json = None
                    if row["events"]:
                        if isinstance(row["events"], list):
                            events_json = json.dumps(row["events"], ensure_ascii=False)
                        else:
                            events_json = row["events"]

                    cur.execute(
                        ins_sql,
                        (
                            row["visitor_id"],
                            row["event_time"],
                            row["source_table"],
                            row["is_bot"],
                            row["ip_address"],
                            row["user_agent"],
                            row["referer"],
                            row["path"],
                            row["screen_width"],
                            row["screen_height"],
                            row["battery_level"],
                            row["load_time"],
                            events_json,
                            row["geolocation_lat"],
                            row["geolocation_lng"],
                            row["server_event_time"],
                            row["client_event_time"],
                            row["accept_language"],
                            row["orientation"],
                            row["battery_charging"],
                            row["cookies"],
                            row["cookies_str"],
                            row["cookie_uid"],
                            row["fingerprint"],
                            # parsed_ua/geo/query/utm => нужно сериализовать, если dict
                            (
                                json.dumps(row["parsed_ua"], ensure_ascii=False)
                                if isinstance(row["parsed_ua"], dict)
                                else row["parsed_ua"]
                            ),
                            (
                                json.dumps(row["parsed_geo"], ensure_ascii=False)
                                if isinstance(row["parsed_geo"], dict)
                                else row["parsed_geo"]
                            ),
                            (
                                json.dumps(row["parsed_query"], ensure_ascii=False)
                                if isinstance(row["parsed_query"], dict)
                                else row["parsed_query"]
                            ),
                            (
                                json.dumps(row["parsed_utm"], ensure_ascii=False)
                                if isinstance(row["parsed_utm"], dict)
                                else row["parsed_utm"]
                            ),
                            row["time_on_page"],  # <- время на сайте (мс)
                            (
                                json.dumps(row["data_json"], ensure_ascii=False)
                                if row["data_json"]
                                else None
                            ),
                        ),
                    )

                # 4) Ставим processed=1
                if used_srv_ids:
                    srv_str = ",".join(map(str, used_srv_ids))
                    cur.execute(
                        f"UPDATE server_logs SET processed=1 WHERE id IN ({srv_str})"
                    )
                if used_clt_ids:
                    clt_str = ",".join(map(str, used_clt_ids))
                    cur.execute(
                        f"UPDATE client_logs SET processed=1 WHERE id IN ({clt_str})"
                    )

        except Exception as e:
            logging.warning(f"Ошибка при записи в unified_analytics: {e}")

    except Exception as e:
        logging.warning(f"Ошибка в aggregate_data: {e}")
    finally:
        if "conn" in locals():
            conn.close()


# --------------------------------------------------------------------------------
# 4) aggregate_sessions
# --------------------------------------------------------------------------------
@celery_app.task
def aggregate_sessions():
    """
    1) Выбираем НЕобработанные записи (server_logs, client_logs, realtime_events),
       у которых processed=0.
    2) Собираем сессии (таймаут 30 мин).
       Используем UUID для session_id.
    3) Обновляем processed=1 в источниках.
    """
    try:
        conn = get_db_connection()

        with conn.cursor() as cur:
            # С учётом processed=0
            union_sql = """
            SELECT
                'server' AS source,
                id,
                visitor_id,
                request_time AS event_time,
                is_bot
            FROM server_logs
            WHERE processed=0

            UNION ALL

            SELECT
                'client' AS source,
                id,
                visitor_id,
                timestamp AS event_time,
                is_bot
            FROM client_logs
            WHERE processed=0

            UNION ALL

            SELECT
                'realtime' AS source,
                id,
                visitor_id,
                event_time AS event_time,
                0 AS is_bot
            FROM realtime_events
            WHERE processed=0
            """
            cur.execute(union_sql)
            raw_events = cur.fetchall()

        # Разбиваем события по visitor_id
        events_by_visitor = defaultdict(list)
        for ev in raw_events:
            events_by_visitor[ev["visitor_id"]].append(ev)

        session_timeout = timedelta(minutes=30)

        def parse_dt(dt_str):
            if isinstance(dt_str, datetime):
                # Если это уже объект datetime, возвращаем его
                return dt_str
            elif isinstance(dt_str, str):
                # Если это строка, преобразуем её в datetime
                return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
            else:
                # Если тип данных некорректен, поднимаем ошибку
                raise ValueError(f"Неверный тип данных для parse_dt: {type(dt_str)}")

        sessions_inserts = []
        # Для последующего UPDATE processed=1
        processed_ids_server = []
        processed_ids_client = []
        processed_ids_rt = []

        for visitor_id, ev_list in events_by_visitor.items():
            # Сортируем по времени
            ev_list.sort(key=lambda x: x["event_time"])
            current_session_start = None
            current_session_end = None
            current_session_uuid = None
            event_count = 0
            pageviews_count = 0
            last_ts = None

            for e in ev_list:
                t = parse_dt(e["event_time"])

                if current_session_start is None:
                    # Создаём новую сессию (UUID)
                    current_session_start = t
                    current_session_end = t
                    current_session_uuid = str(uuid.uuid4())
                    event_count = 1
                    pageviews_count = 1 if e["source"] == "server" else 0
                else:
                    # Проверяем разрыв
                    if (t - last_ts) > session_timeout:
                        # Закрываем предыдущую сессию
                        sessions_inserts.append(
                            {
                                "session_id": current_session_uuid,
                                "visitor_id": visitor_id,
                                "start": current_session_start,
                                "end": current_session_end,
                                "events_count": event_count,
                                "pageviews": pageviews_count,
                            }
                        )
                        # Начинаем новую
                        current_session_start = t
                        current_session_end = t
                        current_session_uuid = str(uuid.uuid4())
                        event_count = 1
                        pageviews_count = 1 if e["source"] == "server" else 0
                    else:
                        # Продолжаем
                        current_session_end = t
                        event_count += 1
                        if e["source"] == "server":
                            pageviews_count += 1

                last_ts = t

                # Собираем id для UPDATE processed=1
                if e["source"] == "server":
                    processed_ids_server.append(e["id"])
                elif e["source"] == "client":
                    processed_ids_client.append(e["id"])
                else:
                    # realtime
                    processed_ids_rt.append(e["id"])

            # Закрываем хвост
            if current_session_uuid is not None:
                sessions_inserts.append(
                    {
                        "session_id": current_session_uuid,
                        "visitor_id": visitor_id,
                        "start": current_session_start,
                        "end": current_session_end,
                        "events_count": event_count,
                        "pageviews": pageviews_count,
                    }
                )

        # Записываем в sessions
        try:
            with conn.cursor() as cur:
                insert_sql = """
                INSERT INTO sessions (
                    session_uuid,
                    visitor_id,
                    session_start,
                    session_end,
                    events_count,
                    pageviews
                )
                VALUES (%s,%s,%s,%s,%s,%s)
                """
                for s in sessions_inserts:
                    cur.execute(
                        insert_sql,
                        (
                            s["session_id"],
                            s["visitor_id"],
                            s["start"].strftime("%Y-%m-%d %H:%M:%S"),
                            s["end"].strftime("%Y-%m-%d %H:%M:%S"),
                            s["events_count"],
                            s["pageviews"],
                        ),
                    )

                # Обновляем processed=1
                if processed_ids_server:
                    upd_s = (
                        "UPDATE server_logs SET processed=1 WHERE id IN ("
                        + ",".join(map(str, processed_ids_server))
                        + ")"
                    )
                    cur.execute(upd_s)
                if processed_ids_client:
                    upd_c = (
                        "UPDATE client_logs SET processed=1 WHERE id IN ("
                        + ",".join(map(str, processed_ids_client))
                        + ")"
                    )
                    cur.execute(upd_c)
                if processed_ids_rt:
                    upd_r = (
                        "UPDATE realtime_events SET processed=1 WHERE id IN ("
                        + ",".join(map(str, processed_ids_rt))
                        + ")"
                    )
                    cur.execute(upd_r)

        except Exception as e:
            logging.warning(f"Ошибка при вставке sessions: {e}")

    except Exception as e:
        logging.warning(f"Ошибка в aggregate_sessions: {e}")
    finally:
        if "conn" in locals():
            conn.close()


# --------------------------------------------------------------------------------
# 5) Пример задачи для прогноза
# --------------------------------------------------------------------------------
@celery_app.task
def forecast_traffic():
    """
    Пример "современной" задачи прогнозирования с FB Prophet.
    """
    try:
        from prophet import Prophet
        import pandas as pd

        conn = get_db_connection()
        with conn.cursor() as cur:
            # (1) Выгружаем суточную статистику (например, за 90 дней)
            sql = """
            SELECT 
               DATE(request_time) as dt,
               COUNT(*) as visits
            FROM server_logs
            WHERE request_time >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
            GROUP BY dt
            ORDER BY dt
            """
            cur.execute(sql)
            rows = cur.fetchall()

        # (2) Преобразуем в DataFrame
        data_for_model = pd.DataFrame([{"ds": r["dt"], "y": r["visits"]} for r in rows])
        if data_for_model.empty:
            logging.info("Нет данных для прогноза.")
            return

        # (3) Прогоняем через Prophet
        model = Prophet(seasonality_mode="multiplicative")
        model.fit(data_for_model)
        future = model.make_future_dataframe(periods=14)  # прогноз на 14 дней
        forecast = model.predict(future)

        # (4) Сохраняем результат
        with conn.cursor() as cur:
            for i, row_f in forecast.iterrows():
                forecast_date = row_f["ds"].strftime("%Y-%m-%d")
                forecast_visits = float(row_f["yhat"])  # прогноз float
                sql_insert = """
                INSERT INTO forecast_visits (forecast_date, visits_predicted)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE visits_predicted=VALUES(visits_predicted)
                """
                cur.execute(sql_insert, (forecast_date, forecast_visits))

        logging.info("Прогноз трафика успешно обновлён.")

    except Exception as e:
        logging.warning(f"Ошибка в forecast_traffic: {e}")
    finally:
        if "conn" in locals():
            conn.close()
