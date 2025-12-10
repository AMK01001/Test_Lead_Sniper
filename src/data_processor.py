# Этот модуль выполняет чистку, фильтрацию и нормализацию данных в точном соответствии с требованиями задания (раздел 3.3).

# data_processor.py
import pandas as pd
import numpy as np
import re

class DataProcessor:
    def __init__(self):
        # Целевые ОКВЭД для рекламных и BTL-агентств [citation:1][citation:4]
        self.target_okveds = ['73.11', '73.12', '73.13', '74.20', '90.03']
        self.min_revenue = 200  # Минимальная выручка в млн руб

    def normalize_data(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Приводит сырые данные к единому формату."""
        df = raw_df.copy()
        # 1. Нормализация выручки: строка "250 млн" -> число 250.0
        df['revenue'] = df['revenue'].apply(self._normalize_revenue_string)
        df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0)
        # 2. Нормализация года
        df['revenue_year'] = pd.to_numeric(df['revenue_year'], errors='coerce').fillna(2023)
        # 3. Очистка строковых полей от лишних пробелов и мусора
        string_cols = ['name', 'okved_main', 'segment_tag', 'description']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
        return df

    def _normalize_revenue_string(self, value) -> float:
        """Конвертирует строковое представление выручки в число."""
        if pd.isna(value): return 0.0
        # Убираем пробелы и находим число
        text = str(value).replace(' ', '').lower()
        # Регулярное выражение для поиска чисел, включая форматы "250.5" и "250,5"
        match = re.search(r'(\d+[.,]?\d*)', text)
        if match:
            num = float(match.group(1).replace(',', '.'))
            # Учитываем множители: "млрд", "млн", "тыс"
            if 'млрд' in text or 'миллиард' in text:
                return num * 1000
            elif 'тыс' in text or 'тысяч' in text:
                return num / 1000
            else:
                return num  # По умолчанию считаем, что это млн
        return 0.0

    def filter_companies(self, df: pd.DataFrame) -> pd.DataFrame:
        """Фильтрует компании по заданным критериям."""
        if df.empty: return df
        # 1. Дедупликация по ИНН
        df = df.drop_duplicates(subset=['inn'], keep='first')
        # 2. Фильтрация по выручке
        df = df[df['revenue'] >= self.min_revenue]
        # 3. Фильтрация по релевантному профилю (ОКВЭД или тегу)
        relevance_mask = (
            df['okved_main'].isin(self.target_okveds) |
            df['segment_tag'].str.contains('BTL|SOUVENIR|FULL_CYCLE|COMM_GROUP|EVENT', case=False, na=False)
        )
        df = df[relevance_mask]
        return df