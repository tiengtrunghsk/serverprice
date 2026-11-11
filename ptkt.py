import json
import os
import numpy as np
import pandas as pd


class ProfessionalTechnicalAnalyzer:

    def __init__(self, history_data_or_path):
        """Khởi tạo với dữ liệu lịch sử OHLCV

        history_data_or_path: str (đường dẫn file VD: 'history/VCB.json') hoặc list
        dict / pandas DataFrame
        """
        raw_data = []

        if isinstance(history_data_or_path, str):
            if os.path.exists(history_data_or_path):
                try:
                    with open(history_data_or_path, "r", encoding="utf-8") as f:
                        file_json = json.load(f)
                        raw_data = file_json.get(
                            "history_d1",
                            file_json.get("data", file_json)
                            if isinstance(file_json, dict)
                            else file_json,
                        )
                except Exception as e:
                    print(f"⚠️ Lỗi đọc file lịch sử {history_data_or_path}: {e}")
        elif isinstance(history_data_or_path, list):
            raw_data = history_data_or_path
        elif isinstance(history_data_or_path, pd.DataFrame):
            self.df = history_data_or_path.copy()

        if not hasattr(self, "df") or self.df is None or self.df.empty:
            if isinstance(raw_data, list) and len(raw_data) > 0:
                self.df = pd.DataFrame(raw_data)
            else:
                self.df = pd.DataFrame()

        if not self.df.empty:
            for col in ["open", "high", "low", "close", "volume"]:
                if col in self.df.columns:
                    self.df[col] = pd.to_numeric(self.df[col], errors="coerce")

            # Tính toán các chỉ báo kỹ thuật & khối lượng cơ sở
            self.df["sma20"] = self.df["close"].rolling(window=20).mean()
            self.df["sma50"] = self.df["close"].rolling(window=50).mean()
            self.df["vol_ma20"] = self.df["volume"].rolling(window=20).mean()

    def analyze(self):
        """Chạy quét toàn diện cấu trúc giá kết hợp đánh giá khối lượng dòng tiền"""
        if self.df.empty or len(self.df) < 50:
            return {
                "pattern": "Tích lũy",
                "technical_status": "Chưa đủ dữ liệu lịch sử",
                "technical_score": 50,
                "volume_analysis": "Chưa đủ dữ liệu để đánh giá volume",
                "darvas_breakout": False,
                "double_bottom": False,
                "spring_washout": False,
                "accumulation": False,
            }

        darvas = self._detect_darvas_box()
        double_bottom = self._detect_double_bottom()
        spring = self._detect_spring_washout()
        accumulation = self._detect_accumulation()
        vol_eval = self._evaluate_volume_behavior()

        # Hệ thống chấm điểm kỹ thuật (Thang điểm gốc 0 - 100)
        score = 50
        status = "Tích lũy sideway"
        pattern_name = "Tích lũy"

        if darvas["breakout"]:
            score += 35
            status = "🚀 Darvas Breakout (Volume Xác Nhận)"
            pattern_name = "Darvas Breakout"
        elif double_bottom:
            score += 30
            status = "📈 Mô hình 2 Đáy (W-Pattern + Cạn Cung)"
            pattern_name = "2 Đáy"
        elif spring:
            score += 25
            status = "🎯 Quét Đáy Rũ Bỏ (Spring + Dòng Tiền Lớn)"
            pattern_name = "Spring/Washout"
        elif accumulation:
            score += 20
            status = "📦 Siết Nền Tích Lũy (Cạn Cung Wyckoff)"
            pattern_name = "Accumulation"

        # Cộng điểm bổ sung qua xu hướng MA20 / MA50 & Volume phiên gần nhất
        latest = self.df.iloc[-1]
        if latest["close"] > latest.get("sma20", 0):
            score += 10
        if latest.get("sma20", 0) > latest.get("sma50", 0):
            score += 10
        if latest["volume"] > latest.get("vol_ma20", 0) * 1.2:
            score += 5

        score = int(np.clip(score, 0, 100))

        return {
            "pattern": pattern_name,
            "technical_status": status,
            "technical_score": score,
            "volume_analysis": vol_eval,
            "darvas_breakout": darvas["breakout"],
            "double_bottom": double_bottom,
            "spring_washout": spring,
            "accumulation": accumulation,
        }

    def _detect_darvas_box(self, window=40):
        """Hộp Darvas: Vượt 99.3% đỉnh hộp kèm volume > 1.35x MA20"""
        if len(self.df) < window:
            return {"breakout": False}
        recent = self.df.tail(window)
        highs = recent["high"].values
        closes = recent["close"].values
        volumes = recent["volume"].values

        box_top = float(np.max(highs[:-5]))
        current_close = closes[-1]
        current_vol = volumes[-1]
        avg_vol = float(recent["vol_ma20"].iloc[-1])

        is_breakout = bool(
            (current_close >= box_top * 0.993) and (current_vol > avg_vol * 1.35)
        )
        return {"breakout": is_breakout}

    def _detect_double_bottom(self, lookback=70):
        """Mô hình 2 Đáy: Khoảng cách 8-50 phiên, chênh lệch đáy tối đa 3%"""
        if len(self.df) < lookback:
            return False
        recent = self.df.tail(lookback).reset_index(drop=True)
        lows = recent["low"].values
        troughs = []
        for i in range(2, len(lows) - 2):
            if lows[i] <= lows[i - 1] and lows[i] <= lows[i + 1]:
                troughs.append((i, lows[i]))

        if len(troughs) < 2:
            return False
        t1, t2 = troughs[-2], troughs[-1]
        if not (8 <= t2[0] - t1[0] <= 50):
            return False
        if abs(t1[1] - t2[1]) / t1[1] > 0.03:
            return False

        between_highs = recent["high"].iloc[t1[0] : t2[0]]
        if len(between_highs) > 0:
            peak_price = between_highs.max()
            if peak_price < max(t1[1], t2[1]) * 1.025:
                return False
            current_close = recent["close"].iloc[-1]
            if current_close >= peak_price * 0.975:
                return True
        return False

    def _detect_spring_washout(self, lookback=45):
        """Quét đáy rũ bỏ (Spring): Nhúng thủng hỗ trợ, rút chân mạnh >= 50% biên độ, volume lớn"""
        if len(self.df) < lookback:
            return False
        recent = self.df.tail(lookback).reset_index(drop=True)
        lows = recent["low"].values
        closes = recent["close"].values
        highs = recent["high"].values
        volumes = recent["volume"].values
        support_level = np.min(lows[:-5])

        for i in range(len(recent) - 3, len(recent)):
            candle_range = highs[i] - lows[i]
            if candle_range == 0:
                continue
            body_position = (closes[i] - lows[i]) / candle_range
            if lows[i] < support_level * 0.992 and body_position >= 0.50:
                avg_vol = volumes[:-5].mean()
                if volumes[i] > avg_vol * 1.2:
                    return True
        return False

    def _detect_accumulation(self, lookback=45):
        """Tích lũy Wyckoff: Biên độ hẹp <= 15% + Khối lượng nửa sau cạn kiệt (< 85%)"""
        if len(self.df) < lookback:
            return False
        recent = self.df.tail(lookback)
        price_range_pct = (
            recent["high"].max() - recent["low"].min()
        ) / recent["close"].mean()
        if price_range_pct > 0.15:
            return False

        volumes = recent["volume"].values
        mid = len(volumes) // 2
        is_drying_volume = volumes[mid:].mean() < volumes[:mid].mean() * 0.85
        return bool(is_drying_volume)

    def _evaluate_volume_behavior(self):
        """Đánh giá chi tiết hành vi khối lượng giao dịch"""
        if len(self.df) < 20:
            return "Dữ liệu giao dịch chưa đủ để phân tích volume."

        recent_5 = self.df.tail(5)
        avg_vol_20 = self.df["vol_ma20"].iloc[-1]
        latest_vol = recent_5["volume"].iloc[-1]
        vol_ratio = (
            (latest_vol / avg_vol_20)
            if avg_vol_20 > 0
            else 1.0
        )

        if vol_ratio >= 1.5:
            return f"Thanh khoản đột biến mạnh ({vol_ratio:.1f}x trung bình 20 phiên) - Dòng tiền lớn đang tham gia chủ động."
        elif vol_ratio >= 1.1:
            return f"Thanh khoản gia tăng nhẹ ({vol_ratio:.1f}x trung bình 20 phiên) - Dòng tiền bắt đầu chú ý."
        elif vol_ratio <= 0.7:
            return f"Thanh khoản cạn kiệt ({vol_ratio:.1f}x trung bình 20 phiên) - Trạng thái No Supply (Cạn cung), rủi ro bán tháo thấp."
        else:
            return f"Thanh khoản ở mức ổn định tương đương trung bình 20 phiên."
