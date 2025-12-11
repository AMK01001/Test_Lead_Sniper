import pandas as pd
import numpy as np
import re

class DataProcessor:
    def __init__(self):
        self.target_okveds = ['73.11', '73.12', '73.13', '74.20', '90.03']
        self.min_revenue = 200

    def normalize_data(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Приводит сырые данные к единому формату."""
        if raw_df.empty:
            return raw_df
            
        df = raw_df.copy()
        
        # 1. Очистка и нормализация строковых полей
        string_cols = ['name', 'okved_main', 'segment_tag', 'description', 
                      'region', 'contacts', 'source', 'site']
        
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace(['nan', 'None', 'NaT', 'NaN', ''], '')
        
        # 2. Нормализация выручки
        if 'revenue' in df.columns:
            df['revenue'] = df['revenue'].apply(self._normalize_revenue_value)
            df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0)
        
        # 3. Нормализация года
        if 'revenue_year' in df.columns:
            df['revenue_year'] = pd.to_numeric(df['revenue_year'], errors='coerce')
            df['revenue_year'] = df['revenue_year'].fillna(2023).astype(int)
        
        # 4. Нормализация ИНН
        if 'inn' in df.columns:
            df['inn'] = df['inn'].astype(str).str.strip()
            df['inn'] = df['inn'].apply(lambda x: re.sub(r'\D', '', x)[:10] if pd.notna(x) else '')
        
        # 5. Добавляем segment_tag если его нет
        if 'segment_tag' not in df.columns:
            df['segment_tag'] = 'BTL'
        
        return df

    def _normalize_revenue_value(self, value) -> float:
        """Конвертирует значение выручки в число."""
        if pd.isna(value):
            return 0.0
        
        try:
            # Если уже число
            if isinstance(value, (int, float)):
                return float(value)
            
            text = str(value).replace(' ', '').replace(',', '.').lower()
            
            # Извлекаем число
            match = re.search(r'(\d+\.?\d*)', text)
            if not match:
                return 0.0
            
            num = float(match.group(1))
            
            # Учитываем множители
            if 'млрд' in text or 'миллиард' in text:
                return num * 1000
            elif 'тыс' in text or 'тысяч' in text:
                return num / 1000
            else:
                return num  # По умолчанию считаем, что это млн
                
        except Exception:
            return 0.0

    def filter_companies(self, df: pd.DataFrame) -> pd.DataFrame:
        """Фильтрует компании по заданным критериям."""
        if df.empty:
            return df
        
        # Создаем копию для безопасной фильтрации
        result_df = df.copy()
        
        # 1. Дедупликация по ИНН
        result_df = result_df.drop_duplicates(subset=['inn'], keep='first')
        
        # 2. Фильтрация по выручке
        result_df = result_df[result_df['revenue'] >= self.min_revenue]
        
        # 3. Фильтрация по релевантному профилю
        has_target_okved = result_df['okved_main'].isin(self.target_okveds)
        
        # Проверяем segment_tag на наличие ключевых тегов
        has_relevant_tag = result_df['segment_tag'].str.contains(
            'BTL|SOUVENIR|FULL_CYCLE|COMM_GROUP|EVENT', 
            case=False, 
            na=False
        )
        
        # Проверяем описание/название на ключевые слова
        def check_keywords(text):
            if pd.isna(text):
                return False
            keywords = ['btl', 'промо', 'ивент', 'мерчендайз', 'сувенир', 'реклам']
            text_lower = str(text).lower()
            return any(keyword in text_lower for keyword in keywords)
        
        has_keywords = result_df['description'].apply(check_keywords) | \
                      result_df['name'].apply(check_keywords)
        
        # Комбинируем все критерии
        relevance_mask = has_target_okved | has_relevant_tag | has_keywords
        result_df = result_df[relevance_mask]
        
        return result_df