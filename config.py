

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "mygo",
    "password": "123456",
    "database": "radio_spectrum_monitor",
    "charset": "utf8mb4"
}

REDIS_CONFIG = {
    "enabled": True,
    "host": "127.0.0.1",
    "port": 16379,
    "db": 0,
    "decode_responses": True,
    "username": None,
    "password": "123456",
    "latest_realtime_ttl_seconds": 120,
    "station_online_ttl_seconds": 300,
    "unread_alarm_ttl_seconds": 120
}

SIMULATOR_CONFIG = {
    "interval_seconds": 3,
    "points_count": 64,
    "alarm_cooldown_seconds": 30,
    "verbose": True
}

AI_SERVICE_CONFIG = {
    "enabled": True,
    "base_url": "http://127.0.0.1:9300",
    "predict_path": "/predict",
    "timeout_seconds": 3
}