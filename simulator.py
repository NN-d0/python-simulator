


import json
import math
import random
import time
from datetime import datetime
from typing import Dict, List

import pymysql
import redis
import requests
from pymysql.cursors import DictCursor

from config import AI_SERVICE_CONFIG, DB_CONFIG, REDIS_CONFIG, SIMULATOR_CONFIG


class SpectrumSimulator:
    def __init__(self):
        self.conn = None
        self.redis_client = None
        self.task_states: Dict[int, Dict] = {}
        self.last_alarm_time_by_task: Dict[int, float] = {}
        self.http = requests.Session()

    # 数据库连接

    def connect(self):
        self.conn = pymysql.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"],
            charset=DB_CONFIG["charset"],
            cursorclass=DictCursor,
            autocommit=False
        )

    def connect_redis(self):
        if not REDIS_CONFIG["enabled"]:
            return

        redis_kwargs = {
            "host": REDIS_CONFIG["host"],
            "port": REDIS_CONFIG["port"],
            "db": REDIS_CONFIG["db"],
            "decode_responses": REDIS_CONFIG["decode_responses"]
        }

        username = REDIS_CONFIG.get("username")
        password = REDIS_CONFIG.get("password")

        if username:
            redis_kwargs["username"] = username

        if password:
            redis_kwargs["password"] = password

        self.redis_client = redis.Redis(**redis_kwargs)
        self.redis_client.ping()

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

        if self.redis_client:
            try:
                self.redis_client.close()
            except Exception:
                pass
            self.redis_client = None

    # 读取系统参数阈值
    def load_thresholds(self, cursor) -> Dict[str, float]:
        threshold_map = {
            "alarm.power.threshold.dbm": -30.0,
            "alarm.snr.threshold.db": 10.0
        }
        sql = """
        SELECT config_key, config_value
        FROM sys_config
        WHERE config_key IN ('alarm.power.threshold.dbm', 'alarm.snr.threshold.db')
        """
        cursor.execute(sql)
        rows = cursor.fetchall()

        for row in rows:
            key = row["config_key"]
            value = row["config_value"]
            try:
                threshold_map[key] = float(value)
            except Exception:
                pass

        return threshold_map

    def fetch_running_tasks(self, cursor) -> List[Dict]:
        sql = """
        SELECT
            t.id AS task_id,
            t.task_name,
            t.station_id,
            t.device_id,
            t.freq_start_mhz,
            t.freq_end_mhz,
            t.sample_rate_khz,
            t.algorithm_mode,
            t.task_status,
            s.station_name,
            d.device_name
        FROM monitor_task t
        JOIN station s ON t.station_id = s.id
        JOIN device d ON t.device_id = d.id
        WHERE t.task_status = 1
        ORDER BY t.id ASC
        """
        cursor.execute(sql)
        return cursor.fetchall()

    def get_task_state(self, task: Dict) -> Dict:
        task_id = task["task_id"]
        if task_id not in self.task_states:
            freq_start = float(task["freq_start_mhz"])
            freq_end = float(task["freq_end_mhz"])
            band = max(freq_end - freq_start, 0.2)

            self.task_states[task_id] = {
                "center_freq_mhz": round(random.uniform(freq_start + band * 0.2, freq_end - band * 0.2), 3),
                "signal_type": self.pick_signal_type(freq_start, freq_end),
                "channel_model": self.pick_channel_model(),
                "peak_index": random.randint(18, 46),
                "noise_floor": random.uniform(-86.0, -78.0),
                "peak_delta": random.uniform(20.0, 30.0),
                "abnormal_mode": "normal"
            }

        return self.task_states[task_id]

    def pick_signal_type(self, freq_start: float, freq_end: float) -> str:
        center = (freq_start + freq_end) / 2

        if center < 150:
            pool = ["AM", "FM", "FM", "AM", "FM"]
        elif center < 500:
            pool = ["BPSK", "QPSK", "16QAM", "QPSK", "BPSK"]
        else:
            pool = ["AM", "FM", "BPSK", "QPSK", "16QAM"]

        return random.choice(pool)

    def pick_channel_model(self) -> str:
        pool = ["AWGN", "Rayleigh", "CarrierOffset", "SampleRateError", "PathLoss"]
        return random.choice(pool)

    def evolve_state(self, task: Dict, state: Dict):
        freq_start = float(task["freq_start_mhz"])
        freq_end = float(task["freq_end_mhz"])
        band = max(freq_end - freq_start, 0.2)

        state["center_freq_mhz"] += random.uniform(-band * 0.01, band * 0.01)
        state["center_freq_mhz"] = max(freq_start, min(freq_end, state["center_freq_mhz"]))
        state["center_freq_mhz"] = round(state["center_freq_mhz"], 3)

        state["peak_index"] += random.randint(-2, 2)
        state["peak_index"] = max(10, min(54, state["peak_index"]))

        if random.random() < 0.08:
            state["signal_type"] = self.pick_signal_type(freq_start, freq_end)

        if random.random() < 0.12:
            state["channel_model"] = self.pick_channel_model()

        p = random.random()
        if p < 0.12:
            state["abnormal_mode"] = "high_power"
        elif p < 0.24:
            state["abnormal_mode"] = "low_snr"
        else:
            state["abnormal_mode"] = "normal"

        if state["abnormal_mode"] == "normal":
            state["noise_floor"] = random.uniform(-86.0, -78.0)
            state["peak_delta"] = random.uniform(18.0, 30.0)
        elif state["abnormal_mode"] == "high_power":
            state["noise_floor"] = random.uniform(-84.0, -76.0)
            state["peak_delta"] = random.uniform(45.0, 58.0)
        else:
            state["noise_floor"] = random.uniform(-60.0, -52.0)
            state["peak_delta"] = random.uniform(7.0, 11.0)

    def generate_power_points(self, state: Dict, points_count: int) -> List[float]:
        peak_index = state["peak_index"]
        noise_floor = state["noise_floor"]
        peak_delta = state["peak_delta"]
        channel_model = state["channel_model"]

        signal_type = state["signal_type"]
        width_map = {
            "AM": 4.0,
            "FM": 7.0,
            "BPSK": 5.0,
            "QPSK": 6.0,
            "16QAM": 8.0
        }
        width = width_map.get(signal_type, 6.0)

        if channel_model == "Rayleigh":
            width += 1.0
        elif channel_model == "CarrierOffset":
            peak_index += random.randint(-3, 3)
        elif channel_model == "SampleRateError":
            width += 2.0
        elif channel_model == "PathLoss":
            peak_delta -= random.uniform(3.0, 7.0)

        points = []
        for i in range(points_count):
            base = noise_floor + random.uniform(-2.0, 2.0)

            distance = (i - peak_index) / max(width, 1.0)
            main_peak = peak_delta * math.exp(-(distance ** 2))

            shoulder_peak = 0.0
            if signal_type in ("FM", "16QAM", "QPSK"):
                shoulder_distance = (i - (peak_index + 5)) / max(width + 1.5, 1.0)
                shoulder_peak = (peak_delta * 0.35) * math.exp(-(shoulder_distance ** 2))

            value = base + main_peak + shoulder_peak
            points.append(round(value, 2))

        return points


    def calculate_metrics(self, points: List[float], bandwidth_khz: float) -> Dict:
        if not points:
            return {
                "peak_power_dbm": -90.0,
                "snr_db": 0.0,
                "occupied_bandwidth_khz": 0.0
            }

        peak_power = round(max(points), 2)

        sorted_points = sorted(points)
        take_n = max(1, int(len(points) * 0.2))
        noise_floor = sum(sorted_points[:take_n]) / take_n
        noise_floor = round(noise_floor, 2)

        snr_db = round(peak_power - noise_floor, 2)

        threshold = noise_floor + 3.0
        active_bins = sum(1 for x in points if x >= threshold)
        freq_step = bandwidth_khz / len(points)
        occupied_bw = round(max(freq_step, active_bins * freq_step), 2)

        return {
            "peak_power_dbm": peak_power,
            "snr_db": snr_db,
            "occupied_bandwidth_khz": occupied_bw
        }

    def local_rule_predict(self, payload: Dict) -> Dict:
        center_freq = float(payload.get("center_freq_mhz", 0))
        occupied_bw = float(payload.get("occupied_bandwidth_khz", 0))
        peak_power_dbm = float(payload.get("peak_power_dbm", -90))
        snr_db = float(payload.get("snr_db", 0))
        channel_model = str(payload.get("channel_model", "UNKNOWN"))

        if center_freq < 150:
            if occupied_bw >= 170:
                predicted_label = "FM"
                confidence = 0.84
                reason = "本地规则判定：低频模拟段 + 宽带宽，更接近 FM。"
            else:
                predicted_label = "AM"
                confidence = 0.80
                reason = "本地规则判定：低频模拟段 + 窄带宽，更接近 AM。"
        else:
            if occupied_bw < 180:
                predicted_label = "BPSK"
                confidence = 0.76
                reason = "本地规则判定：数字频段 + 窄带宽，更接近 BPSK。"
            elif occupied_bw < 230:
                predicted_label = "QPSK"
                confidence = 0.80
                reason = "本地规则判定：数字频段 + 中带宽，更接近 QPSK。"
            else:
                predicted_label = "16QAM"
                confidence = 0.82
                reason = "本地规则判定：数字频段 + 宽带宽，更接近 16QAM。"

        if channel_model in ("Rayleigh", "CarrierOffset", "SampleRateError"):
            confidence = max(0.60, round(confidence - 0.05, 2))

        should_alarm = peak_power_dbm >= -30.0 or snr_db <= 10.0

        if peak_power_dbm >= -25.0 or snr_db <= 7.0:
            risk_level = "HIGH"
        elif should_alarm:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"

        return {
            "predicted_label": predicted_label,
            "confidence": confidence,
            "risk_level": risk_level,
            "should_alarm": should_alarm,
            "reason": reason,
            "model_name": "local-fallback-rule",
            "source": "local-fallback"
        }

    def predict_with_ai(self, payload: Dict) -> Dict:
        if not AI_SERVICE_CONFIG["enabled"]:
            return self.local_rule_predict(payload)

        url = AI_SERVICE_CONFIG["base_url"].rstrip("/") + AI_SERVICE_CONFIG["predict_path"]
        timeout = AI_SERVICE_CONFIG["timeout_seconds"]

        try:
            response = self.http.post(url, json=payload, timeout=timeout)
            response.raise_for_status()
            result = response.json()

            if result.get("code") != 200 or not result.get("data"):
                return self.local_rule_predict(payload)

            data = result["data"]
            return {
                "predicted_label": data.get("predicted_label", "UNKNOWN"),
                "confidence": data.get("confidence", 0.5),
                "risk_level": data.get("risk_level", "MEDIUM"),
                "should_alarm": bool(data.get("should_alarm", False)),
                "reason": data.get("reason", "AI服务未返回说明"),
                "model_name": data.get("model_name", "rule-model-v1"),
                "source": "flask-ai"
            }
        except Exception:
            return self.local_rule_predict(payload)

    def insert_snapshot(self, cursor, task: Dict, state: Dict, thresholds: Dict) -> Dict:
        bandwidth_khz = float(task["sample_rate_khz"])
        points_count = SIMULATOR_CONFIG["points_count"]

        power_points = self.generate_power_points(state, points_count)
        metrics = self.calculate_metrics(power_points, bandwidth_khz)

        signal_type = state["signal_type"]
        channel_model = state["channel_model"]
        center_freq_mhz = state["center_freq_mhz"]
        peak_power_dbm = metrics["peak_power_dbm"]
        snr_db = metrics["snr_db"]
        occupied_bw = metrics["occupied_bandwidth_khz"]

        ai_payload = {
            "center_freq_mhz": center_freq_mhz,
            "bandwidth_khz": bandwidth_khz,
            "peak_power_dbm": peak_power_dbm,
            "snr_db": snr_db,
            "occupied_bandwidth_khz": occupied_bw,
            "channel_model": channel_model,
            "power_points": power_points
        }

        ai_result = self.predict_with_ai(ai_payload)
        ai_label = ai_result["predicted_label"]

        power_threshold = thresholds["alarm.power.threshold.dbm"]
        snr_threshold = thresholds["alarm.snr.threshold.db"]

        threshold_alarm = (peak_power_dbm >= power_threshold) or (snr_db <= snr_threshold)
        ai_alarm = bool(ai_result.get("should_alarm", False))
        alarm_flag = 1 if (threshold_alarm or ai_alarm) else 0

        sql = """
        INSERT INTO spectrum_snapshot (
            station_id, device_id, task_id,
            center_freq_mhz, bandwidth_khz, signal_type, channel_model,
            peak_power_dbm, snr_db, occupied_bandwidth_khz, ai_label, alarm_flag,
            power_points_json, waterfall_row_json, capture_time, create_time
        ) VALUES (
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, NOW(), NOW()
        )
        """

        power_points_json = json.dumps(power_points, ensure_ascii=False)
        waterfall_row_json = json.dumps(power_points, ensure_ascii=False)

        cursor.execute(sql, (
            task["station_id"],
            task["device_id"],
            task["task_id"],
            center_freq_mhz,
            bandwidth_khz,
            signal_type,
            channel_model,
            peak_power_dbm,
            snr_db,
            occupied_bw,
            ai_label,
            alarm_flag,
            power_points_json,
            waterfall_row_json
        ))

        snapshot_id = cursor.lastrowid
        now_text = datetime.now().isoformat(timespec="seconds")

        return {
            "snapshot_id": snapshot_id,
            "station_id": task["station_id"],
            "station_name": task["station_name"],
            "device_id": task["device_id"],
            "device_name": task["device_name"],
            "task_id": task["task_id"],
            "task_name": task["task_name"],
            "center_freq_mhz": center_freq_mhz,
            "bandwidth_khz": bandwidth_khz,
            "signal_type": signal_type,
            "channel_model": channel_model,
            "peak_power_dbm": peak_power_dbm,
            "snr_db": snr_db,
            "occupied_bandwidth_khz": occupied_bw,
            "ai_label": ai_label,
            "ai_confidence": ai_result.get("confidence"),
            "ai_risk_level": ai_result.get("risk_level"),
            "ai_reason": ai_result.get("reason"),
            "ai_source": ai_result.get("source"),
            "ai_should_alarm": ai_alarm,
            "alarm_flag": alarm_flag,
            "power_points_json": power_points_json,
            "waterfall_row_json": waterfall_row_json,
            "capture_time": now_text,
            "create_time": now_text
        }


    def maybe_insert_alarm(self, cursor, task: Dict, snapshot_info: Dict, thresholds: Dict):
        task_id = task["task_id"]
        now_ts = time.time()
        cooldown = SIMULATOR_CONFIG["alarm_cooldown_seconds"]
        last_alarm_ts = self.last_alarm_time_by_task.get(task_id, 0)

        if snapshot_info["alarm_flag"] != 1:
            return

        if now_ts - last_alarm_ts < cooldown:
            return

        peak_power_dbm = snapshot_info["peak_power_dbm"]
        snr_db = snapshot_info["snr_db"]
        center_freq_mhz = snapshot_info["center_freq_mhz"]

        power_threshold = thresholds["alarm.power.threshold.dbm"]
        snr_threshold = thresholds["alarm.snr.threshold.db"]

        if peak_power_dbm >= power_threshold:
            alarm_type = "ILLEGAL_SIGNAL"
            alarm_level = "HIGH"
            title = "疑似异常高功率信号"
            content = (
                f"在 {center_freq_mhz}MHz 附近检测到异常高功率信号，"
                f"峰值功率 {peak_power_dbm}dBm，AI识别结果为 {snapshot_info['ai_label']}。"
            )
        elif snr_db <= snr_threshold:
            alarm_type = "SNR_LOW"
            alarm_level = "MEDIUM"
            title = "信号质量偏低"
            content = (
                f"在 {center_freq_mhz}MHz 附近监测到低信噪比信号，"
                f"SNR={snr_db}dB，AI识别结果为 {snapshot_info['ai_label']}。"
            )
        else:
            alarm_type = "AI_SUSPECT"
            alarm_level = "MEDIUM"
            title = "AI识别疑似异常信号"
            content = (
                f"AI推理判定该信号存在异常风险，"
                f"识别结果={snapshot_info['ai_label']}，"
                f"风险等级={snapshot_info['ai_risk_level']}，"
                f"原因：{snapshot_info['ai_reason']}"
            )

        alarm_no = f"ALARM{datetime.now().strftime('%Y%m%d%H%M%S')}{random.randint(100, 999)}"

        sql = """
        INSERT INTO alarm_record (
            alarm_no, station_id, device_id, task_id, snapshot_id,
            alarm_type, alarm_level, title, content,
            alarm_status, alarm_time, create_time, update_time
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            0, NOW(), NOW(), NOW()
        )
        """

        cursor.execute(sql, (
            alarm_no,
            task["station_id"],
            task["device_id"],
            task["task_id"],
            snapshot_info["snapshot_id"],
            alarm_type,
            alarm_level,
            title,
            content
        ))

        self.last_alarm_time_by_task[task_id] = now_ts

    def update_online_status(self, cursor, task: Dict):
        sql_device = """
        UPDATE device
        SET run_status = 1,
            last_online_time = NOW(),
            update_time = NOW()
        WHERE id = %s
        """
        cursor.execute(sql_device, (task["device_id"],))

        sql_station = """
        UPDATE station
        SET online_status = 1,
            update_time = NOW()
        WHERE id = %s
        """
        cursor.execute(sql_station, (task["station_id"],))

    def update_realtime_cache(self, snapshot_info: Dict):
        if not self.redis_client:
            return

        payload = {
            "id": snapshot_info["snapshot_id"],
            "stationId": snapshot_info["station_id"],
            "stationName": snapshot_info["station_name"],
            "deviceId": snapshot_info["device_id"],
            "deviceName": snapshot_info["device_name"],
            "taskId": snapshot_info["task_id"],
            "taskName": snapshot_info["task_name"],
            "centerFreqMhz": snapshot_info["center_freq_mhz"],
            "bandwidthKhz": snapshot_info["bandwidth_khz"],
            "signalType": snapshot_info["signal_type"],
            "channelModel": snapshot_info["channel_model"],
            "peakPowerDbm": snapshot_info["peak_power_dbm"],
            "snrDb": snapshot_info["snr_db"],
            "occupiedBandwidthKhz": snapshot_info["occupied_bandwidth_khz"],
            "aiLabel": snapshot_info["ai_label"],
            "alarmFlag": snapshot_info["alarm_flag"],
            "powerPointsJson": snapshot_info["power_points_json"],
            "waterfallRowJson": snapshot_info["waterfall_row_json"],
            "captureTime": snapshot_info["capture_time"],
            "createTime": snapshot_info["create_time"]
        }

        text = json.dumps(payload, ensure_ascii=False)

        self.redis_client.setex(
            f"radio:realtime:latest:{snapshot_info['station_id']}",
            REDIS_CONFIG["latest_realtime_ttl_seconds"],
            text
        )

        self.redis_client.setex(
            "radio:realtime:latest:global",
            REDIS_CONFIG["latest_realtime_ttl_seconds"],
            text
        )
    def update_station_online_cache(self, station_id: int):
        if not self.redis_client:
            return

        self.redis_client.setex(
            f"radio:station:online:{station_id}",
            REDIS_CONFIG["station_online_ttl_seconds"],
            "1"
        )


    def refresh_unread_alarm_cache(self, cursor):
        if not self.redis_client:
            return

        sql = """
        SELECT COUNT(*) AS total
        FROM alarm_record
        WHERE alarm_status = 0
        """
        cursor.execute(sql)
        row = cursor.fetchone()
        total = 0 if not row else int(row["total"])

        self.redis_client.setex(
            "radio:alarm:unread:count",
            REDIS_CONFIG["unread_alarm_ttl_seconds"],
            str(total)
        )

    def run_once(self):
        with self.conn.cursor() as cursor:
            thresholds = self.load_thresholds(cursor)
            tasks = self.fetch_running_tasks(cursor)

            if not tasks:
                if SIMULATOR_CONFIG["verbose"]:
                    print("[INFO] 当前没有 task_status=1 的运行中任务，3 秒后继续检查。")
                self.conn.rollback()
                return

            for task in tasks:
                state = self.get_task_state(task)
                self.evolve_state(task, state)

                snapshot_info = self.insert_snapshot(cursor, task, state, thresholds)
                self.maybe_insert_alarm(cursor, task, snapshot_info, thresholds)
                self.update_online_status(cursor, task)

                if SIMULATOR_CONFIG["verbose"]:
                    print(
                        f"[OK] task={task['task_id']} station={task['station_name']} "
                        f"signal={snapshot_info['signal_type']} "
                        f"ai={snapshot_info['ai_label']} "
                        f"risk={snapshot_info['ai_risk_level']} "
                        f"source={snapshot_info['ai_source']} "
                        f"freq={snapshot_info['center_freq_mhz']}MHz "
                        f"peak={snapshot_info['peak_power_dbm']}dBm "
                        f"snr={snapshot_info['snr_db']}dB "
                        f"alarm={snapshot_info['alarm_flag']}"
                    )


            self.conn.commit()

            if self.redis_client:
                try:
                    for task in tasks:
                        self.update_station_online_cache(task["station_id"])
                    self.refresh_unread_alarm_cache(cursor)
                except Exception as e:
                    if SIMULATOR_CONFIG["verbose"]:
                        print(f"[WARN] Redis缓存更新失败：{e}")

            # 最新频谱缓存单独再做一次，确保每个任务最新值入 Redis
            if self.redis_client:
                try:
                    for task in tasks:
                        latest_sql = """
                        SELECT
                            ss.id,
                            ss.station_id,
                            ss.device_id,
                            ss.task_id,
                            ss.center_freq_mhz,
                            ss.bandwidth_khz,
                            ss.signal_type,
                            ss.channel_model,
                            ss.peak_power_dbm,
                            ss.snr_db,
                            ss.occupied_bandwidth_khz,
                            ss.ai_label,
                            ss.alarm_flag,
                            ss.power_points_json,
                            ss.waterfall_row_json,
                            ss.capture_time,
                            ss.create_time
                        FROM spectrum_snapshot ss
                        WHERE ss.task_id = %s
                        ORDER BY ss.id DESC
                        LIMIT 1
                        """
                        cursor.execute(latest_sql, (task["task_id"],))
                        row = cursor.fetchone()
                        if row:
                            cache_payload = {
                                "snapshot_id": row["id"],
                                "station_id": row["station_id"],
                                "station_name": task["station_name"],
                                "device_id": row["device_id"],
                                "device_name": task["device_name"],
                                "task_id": row["task_id"],
                                "task_name": task["task_name"],
                                "center_freq_mhz": float(row["center_freq_mhz"]),
                                "bandwidth_khz": float(row["bandwidth_khz"]),
                                "signal_type": row["signal_type"],
                                "channel_model": row["channel_model"],
                                "peak_power_dbm": float(row["peak_power_dbm"]),
                                "snr_db": float(row["snr_db"]),
                                "occupied_bandwidth_khz": float(row["occupied_bandwidth_khz"]),
                                "ai_label": row["ai_label"],
                                "alarm_flag": int(row["alarm_flag"]),
                                "power_points_json": row["power_points_json"],
                                "waterfall_row_json": row["waterfall_row_json"],
                                "capture_time": row["capture_time"].isoformat(timespec="seconds") if row["capture_time"] else None,
                                "create_time": row["create_time"].isoformat(timespec="seconds") if row["create_time"] else None
                            }
                            self.update_realtime_cache(cache_payload)
                except Exception as e:
                    if SIMULATOR_CONFIG["verbose"]:
                        print(f"[WARN] 最新频谱Redis缓存更新失败：{e}")

    def run_forever(self):
        interval = SIMULATOR_CONFIG["interval_seconds"]
        print("=====================================================")
        print("无线电频谱仿真数据源已启动")
        print(f"MySQL: {DB_CONFIG['host']}:{DB_CONFIG['port']} / {DB_CONFIG['database']}")
        print(f"Redis: {REDIS_CONFIG['enabled']} -> {REDIS_CONFIG['host']}:{REDIS_CONFIG['port']}")
        print(f"写入周期: {interval} 秒")
        print(f"AI服务开关: {AI_SERVICE_CONFIG['enabled']}")
        if AI_SERVICE_CONFIG["enabled"]:
            print(f"AI服务地址: {AI_SERVICE_CONFIG['base_url']}")
        print("按 Ctrl + C 停止")
        print("=====================================================")

        try:
            while True:
                self.run_once()
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n[INFO] 仿真数据源已停止。")
        except Exception as e:
            print(f"[ERROR] 仿真程序异常：{e}")
            if self.conn:
                self.conn.rollback()
            raise


def main():
    simulator = SpectrumSimulator()
    try:
        simulator.connect()
        simulator.connect_redis()
        simulator.run_forever()
    finally:
        simulator.close()


if __name__ == "__main__":
    main()