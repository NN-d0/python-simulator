import math
import random
import time
from datetime import datetime
from typing import Dict, List, Tuple

import pymysql
import requests
from pymysql.cursors import DictCursor

from config import CORE_API_CONFIG, DB_CONFIG, SIMULATOR_CONFIG


class SpectrumSimulator:
    """
    无线电频谱仿真数据源（HTTP上报 + IQ透传版）

    当前版本职责：
    1. 从 MySQL 读取运行中的 monitor_task（只读）
    2. 只对 task_status = 1 的任务生成频谱
    3. 生成 power_points + i_points + q_points
    4. 通过 HTTP 上报给 Core
    5. Core 再统一调用 Flask AI，并可根据 task.algorithm_mode / model_type 走 RULE / CNN / AUTO
    """

    def __init__(self):
        self.conn = None
        self.task_states: Dict[int, Dict] = {}
        self.http = requests.Session()

    # =========================
    # 数据库连接
    # =========================
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

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

        try:
            self.http.close()
        except Exception:
            pass

    # =========================
    # 读取运行中任务
    # =========================
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

    def sync_local_task_states(self, running_tasks: List[Dict]):
        running_ids = {int(task["task_id"]) for task in running_tasks if task.get("task_id") is not None}
        stale_ids = [task_id for task_id in self.task_states.keys() if task_id not in running_ids]

        for task_id in stale_ids:
            self.task_states.pop(task_id, None)
            if SIMULATOR_CONFIG["verbose"]:
                print(f"[INFO] task={task_id} 已不在运行中，已清理本地仿真状态。")

    # =========================
    # 仿真状态初始化
    # =========================
    def get_task_state(self, task: Dict) -> Dict:
        task_id = int(task["task_id"])

        if task_id not in self.task_states:
            freq_start = float(task["freq_start_mhz"])
            freq_end = float(task["freq_end_mhz"])
            band = max(freq_end - freq_start, 0.2)

            self.task_states[task_id] = {
                "center_freq_mhz": round(
                    random.uniform(freq_start + band * 0.2, freq_end - band * 0.2), 3
                ),
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

    # =========================
    # 频谱折线生成
    # =========================
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

    # =========================
    # I/Q 序列生成
    # =========================
    def generate_random_message(self, seq_len: int) -> List[float]:
        msg = []
        phase = random.uniform(0, 2 * math.pi)
        for i in range(seq_len):
            t = i / max(seq_len, 1)
            val = (
                0.6 * math.sin(2 * math.pi * 2 * t + phase) +
                0.3 * math.sin(2 * math.pi * 5 * t + phase / 2) +
                0.1 * random.uniform(-1, 1)
            )
            msg.append(val)
        max_abs = max(abs(x) for x in msg) + 1e-6
        return [x / max_abs for x in msg]

    def qam16_constellation(self) -> List[complex]:
        pts = [
            -3 - 3j, -3 - 1j, -3 + 1j, -3 + 3j,
            -1 - 3j, -1 - 1j, -1 + 1j, -1 + 3j,
             1 - 3j,  1 - 1j,  1 + 1j,  1 + 3j,
             3 - 3j,  3 - 1j,  3 + 1j,  3 + 3j
        ]
        avg_power = sum(abs(x) ** 2 for x in pts) / len(pts)
        scale = math.sqrt(avg_power)
        return [p / scale for p in pts]

    def smooth_complex(self, seq: List[complex]) -> List[complex]:
        if len(seq) < 3:
            return seq[:]
        out = []
        kernel = [0.2, 0.6, 0.2]
        for i in range(len(seq)):
            acc = 0j
            for k, w in zip([-1, 0, 1], kernel):
                idx = min(max(i + k, 0), len(seq) - 1)
                acc += seq[idx] * w
            out.append(acc)
        return out

    def normalize_complex(self, seq: List[complex]) -> List[complex]:
        power = sum((abs(x) ** 2) for x in seq) / max(len(seq), 1)
        scale = math.sqrt(power + 1e-12)
        return [x / scale for x in seq]

    def generate_clean_iq(self, signal_type: str, seq_len: int) -> List[complex]:
        if signal_type == "AM":
            msg = self.generate_random_message(seq_len)
            carrier_freq = random.uniform(0.04, 0.12)
            mod_index = random.uniform(0.3, 0.9)
            phase0 = random.uniform(0, 2 * math.pi)
            seq = []
            for n in range(seq_len):
                env = 1.0 + mod_index * msg[n]
                phase = 2 * math.pi * carrier_freq * n + phase0
                seq.append(env * complex(math.cos(phase), math.sin(phase)))
            return self.normalize_complex(seq)

        if signal_type == "FM":
            msg = self.generate_random_message(seq_len)
            carrier_freq = random.uniform(0.03, 0.10)
            freq_dev = random.uniform(0.01, 0.05)
            phase = random.uniform(0, 2 * math.pi)
            seq = []
            for n in range(seq_len):
                phase += 2 * math.pi * (carrier_freq + freq_dev * msg[n])
                seq.append(complex(math.cos(phase), math.sin(phase)))
            return self.normalize_complex(seq)

        sps = random.choice([4, 8, 16])
        symbol_count = math.ceil(seq_len / sps) + 2

        if signal_type == "BPSK":
            symbols = [complex(random.choice([-1, 1]), 0.0) for _ in range(symbol_count)]
        elif signal_type == "QPSK":
            symbols = []
            for _ in range(symbol_count):
                i_val = random.choice([-1, 1])
                q_val = random.choice([-1, 1])
                symbols.append(complex(i_val, q_val) / math.sqrt(2))
        else:  # 16QAM
            const = self.qam16_constellation()
            symbols = [random.choice(const) for _ in range(symbol_count)]

        seq = []
        for symbol in symbols:
            seq.extend([symbol] * sps)

        seq = seq[:seq_len]
        if len(seq) < seq_len:
            seq.extend([seq[-1]] * (seq_len - len(seq)))

        seq = self.smooth_complex(seq)
        return self.normalize_complex(seq)

    def apply_channel_effect(self, seq: List[complex], channel_model: str) -> List[complex]:
        n = len(seq)
        out = seq[:]

        if channel_model == "Rayleigh":
            fade = []
            phase0 = random.uniform(0, 2 * math.pi)
            for i in range(n):
                env = 0.6 + 0.4 * abs(math.sin(2 * math.pi * i / max(n, 1) + phase0))
                phase = random.uniform(-0.2, 0.2)
                fade.append(env * complex(math.cos(phase), math.sin(phase)))
            out = [x * h for x, h in zip(out, fade)]

        elif channel_model == "CarrierOffset":
            freq_off = random.uniform(-0.03, 0.03)
            phase0 = random.uniform(0, 2 * math.pi)
            out = [
                x * complex(
                    math.cos(2 * math.pi * freq_off * i + phase0),
                    math.sin(2 * math.pi * freq_off * i + phase0)
                )
                for i, x in enumerate(out)
            ]

        elif channel_model == "SampleRateError":
            error_ratio = random.uniform(-0.02, 0.02)
            src_idx = list(range(n))
            warped = [min(max(int(i * (1.0 + error_ratio)), 0), n - 1) for i in range(n)]
            out = [out[idx] for idx in warped]

        elif channel_model == "PathLoss":
            loss = random.uniform(0.25, 0.85)
            out = [x * loss for x in out]

        out = self.normalize_complex(out)
        return out

    def add_awgn_complex(self, seq: List[complex], snr_db: float) -> List[complex]:
        signal_power = sum(abs(x) ** 2 for x in seq) / max(len(seq), 1)
        snr_linear = 10 ** (snr_db / 10.0)
        noise_power = signal_power / (snr_linear + 1e-12)
        sigma = math.sqrt(noise_power / 2.0)

        out = []
        for x in seq:
            noise = complex(random.gauss(0, sigma), random.gauss(0, sigma))
            out.append(x + noise)

        return self.normalize_complex(out)

    def build_iq_points(self, state: Dict, seq_len: int, snr_db: float) -> Tuple[List[float], List[float]]:
        clean = self.generate_clean_iq(state["signal_type"], seq_len)
        with_channel = self.apply_channel_effect(clean, state["channel_model"])
        noisy = self.add_awgn_complex(with_channel, snr_db)

        i_points = [round(x.real, 6) for x in noisy]
        q_points = [round(x.imag, 6) for x in noisy]
        return i_points, q_points

    # =========================
    # 构造上报数据
    # =========================
    def normalize_channel_model(self, channel_model: str) -> str:
        mapping = {
            "AWGN": "AWGN",
            "Rayleigh": "Rayleigh",
            "CarrierOffset": "CarrierOffset",
            "SampleRateError": "SampleRateError",
            "PathLoss": "PathLoss"
        }
        return mapping.get(channel_model, "AWGN")

    def normalize_task_algorithm_mode(self, algorithm_mode: str) -> str:
        mode = str(algorithm_mode or "RULE").strip().upper()
        if mode == "AI":
            return "CNN"
        if mode == "CNN":
            return "CNN"
        if mode == "AUTO":
            return "AUTO"
        return "RULE"

    def resolve_model_type(self, task: Dict) -> str:
        algorithm_mode = self.normalize_task_algorithm_mode(task.get("algorithm_mode"))
        if algorithm_mode == "CNN":
            return "cnn"
        if algorithm_mode == "AUTO":
            return "auto"
        return "rule"

    def build_report_payload(self, task: Dict, state: Dict) -> Dict:
        bandwidth_khz = float(task["sample_rate_khz"])
        points_count = SIMULATOR_CONFIG["points_count"]

        power_points = self.generate_power_points(state, points_count)
        metrics = self.calculate_metrics(power_points, bandwidth_khz)

        # IQ 长度尽量向训练长度靠拢
        iq_len = max(256, points_count * 4)
        i_points, q_points = self.build_iq_points(
            state=state,
            seq_len=iq_len,
            snr_db=float(metrics["snr_db"])
        )

        capture_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        model_type = self.resolve_model_type(task)

        payload = {
            "stationId": int(task["station_id"]),
            "deviceId": int(task["device_id"]),
            "taskId": int(task["task_id"]),
            "centerFreqMhz": round(float(state["center_freq_mhz"]), 3),
            "bandwidthKhz": round(float(bandwidth_khz), 2),
            "signalType": str(state["signal_type"]),
            "channelModel": self.normalize_channel_model(state["channel_model"]),
            "peakPowerDbm": float(metrics["peak_power_dbm"]),
            "snrDb": float(metrics["snr_db"]),
            "occupiedBandwidthKhz": float(metrics["occupied_bandwidth_khz"]),
            "powerPoints": power_points,
            "waterfallRow": power_points,
            "captureTime": capture_time,
            "modelType": model_type,
            "iPoints": i_points,
            "qPoints": q_points
        }

        return payload

    # =========================
    # HTTP 上报 Core
    # =========================
    def report_to_core(self, payload: Dict) -> Dict:
        if not CORE_API_CONFIG["enabled"]:
            raise RuntimeError("CORE_API_CONFIG.enabled=false，当前未开启 Core 上报")

        url = CORE_API_CONFIG["base_url"].rstrip("/") + CORE_API_CONFIG["report_path"]
        timeout = CORE_API_CONFIG["timeout_seconds"]

        response = self.http.post(url, json=payload, timeout=timeout)
        response.raise_for_status()

        result = response.json()
        code = result.get("code")
        msg = result.get("msg")

        if code == 200:
            data = result.get("data") or {}
            if "accepted" not in data:
                data["accepted"] = True
            return data

        if code == 409:
            return {
                "accepted": False,
                "reason": msg or "任务未处于运行中，Core 已拒绝接收"
            }

        raise RuntimeError(f"Core 上报失败：{result}")

    # =========================
    # 单次运行
    # =========================
    def run_once(self):
        with self.conn.cursor() as cursor:
            tasks = self.fetch_running_tasks(cursor)
            self.sync_local_task_states(tasks)

            if not tasks:
                if SIMULATOR_CONFIG["verbose"]:
                    print("[INFO] 当前没有 task_status=1 的运行中任务，稍后继续检查。")
                self.conn.rollback()
                return

            for task in tasks:
                state = self.get_task_state(task)
                self.evolve_state(task, state)

                payload = self.build_report_payload(task, state)
                response_data = self.report_to_core(payload)

                if not response_data.get("accepted", False):
                    if SIMULATOR_CONFIG["verbose"]:
                        print(
                            f"[SKIP] task={task['task_id']} station={task['station_name']} "
                            f"device={task['device_name']} 已被 Core 拒绝接收，reason={response_data.get('reason')}"
                        )
                    continue

                if SIMULATOR_CONFIG["verbose"]:
                    print(
                        f"[OK] task={task['task_id']} station={task['station_name']} "
                        f"device={task['device_name']} "
                        f"freq={payload['centerFreqMhz']}MHz "
                        f"signal={payload['signalType']} "
                        f"channel={payload['channelModel']} "
                        f"peak={payload['peakPowerDbm']}dBm "
                        f"snr={payload['snrDb']}dB "
                        f"snapshotId={response_data.get('snapshotId')} "
                        f"alarm={response_data.get('alarmFlag')} "
                        f"ai={response_data.get('aiLabel')} "
                        f"algorithmMode={self.normalize_task_algorithm_mode(task.get('algorithm_mode'))} "
                        f"modelType={payload['modelType']} "
                        f"taskStatus={response_data.get('taskStatus')}"
                    )

            self.conn.rollback()

    # =========================
    # 持续运行
    # =========================
    def run_forever(self):
        interval = SIMULATOR_CONFIG["interval_seconds"]

        print("=====================================================")
        print("无线电频谱仿真数据源已启动（HTTP上报 + IQ透传版）")
        print(f"MySQL(只读任务): {DB_CONFIG['host']}:{DB_CONFIG['port']} / {DB_CONFIG['database']}")
        print(f"Core上报地址: {CORE_API_CONFIG['base_url']}{CORE_API_CONFIG['report_path']}")
        print(f"上报周期: {interval} 秒")
        print("当前模式：仿真器上报 power_points + i_points + q_points，并严格跟随任务 algorithm_mode 选择 RULE/CNN/AUTO")
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
        simulator.run_forever()
    finally:
        simulator.close()


if __name__ == "__main__":
    main()