DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "mygo",
    "password": "123456",
    "database": "radio_spectrum_monitor",
    "charset": "utf8mb4"
}

SIMULATOR_CONFIG = {
    "interval_seconds": 3,
    "points_count": 64,
    "verbose": True
}

CORE_API_CONFIG = {
    "enabled": True,
    "base_url": "http://127.0.0.1:9200",
    "report_path": "/api/core/open/collect/report",
    "timeout_seconds": 5
}