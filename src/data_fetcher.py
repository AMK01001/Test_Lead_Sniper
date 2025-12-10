# Этот модуль парсит отраслевой сайт и получает данные по API. 
# Именно здесь выполняется требование программной обработки хотя бы одного источника

# data_fetcher.py
import requests
import pandas as pd
import time
from bs4 import BeautifulSoup
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataFetcher:
    # Ключевые слова для определения сегмента компании (BTL, SOUVENIR и т.д.)
    SEGMENT_KEYWORDS = {
        'BTL': ['btl', 'below the line', 'промо', 'промо-акция', 'мерчендайзинг', 'дегустация'],
        'SOUVENIR': ['сувенир', 'промо-материал', 'корпоративный подарок'],
        'FULL_CYCLE': ['полный цикл', 'комплексные услуги', 'интегрированные коммуникации'],
        'COMM_GROUP': ['коммуникационная группа', 'холдинг'],
        'EVENT': ['ивент', 'event', 'мероприятие', 'конференция']
    }

    def __init__(self, checko_api_key):
        self.checko_api_key = checko_api_key
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def parse_industry_rating(self, url: str) -> pd.DataFrame:
        """Парсит страницу с рейтингом агентств и возвращает DataFrame с названиями компаний."""
        logger.info(f"Начинаем парсинг рейтинга: {url}")
        try:
            response = self.session.get(url, timeout=30)
            soup = BeautifulSoup(response.text, 'html.parser')
            # Пример поиска названий агентств. Селекторы нужно уточнить для конкретного сайта.
            company_elements = soup.select('.rating-table tr td:nth-child(2) a')
            company_names = [elem.text.strip() for elem in company_elements]

            df = pd.DataFrame(company_names, columns=['name'])
            df['rating_ref'] = url
            logger.info(f"Спарсили {len(df)} компаний")
            return df

        except Exception as e:
            logger.error(f"Ошибка при парсинге {url}: {e}")
            return pd.DataFrame()

    def fetch_company_via_api(self, query: str) -> dict:
        """Запрашивает данные о компании по названию через API Checko."""
        api_url = "https://api.checko.ru/v2/company"
        params = {'key': self.checko_api_key, 'query': query}
        try:
            response = self.session.get(api_url, params=params, timeout=15).json()
            if response.get('data'):
                data = response['data']
                # API возвращает ИНН, выручку, ОКВЭД, адрес и другие данные
                return {
                    'inn': data.get('inn'),
                    'name': data.get('name'),
                    'revenue': self._extract_revenue(data.get('financials', [])),
                    'revenue_year': 2023,  # Год можно уточнить из финансовой отчетности в ответе API
                    'okved_main': self._get_main_okved(data.get('okved', [])),
                    'employees': data.get('employees'),
                    'site': data.get('site'),
                    'region': data.get('address', {}).get('region'),
                    'contacts': self._format_contacts(data),
                    'source': 'checko_api'
                }
        except Exception as e:
            logger.warning(f"Не удалось получить данные для '{query}': {e}")
        return {}

    def _extract_revenue(self, financials: list) -> float:
        """Извлекает значение годовой выручки из данных API (в млн руб)."""
        if financials:
            for year_data in financials:
                if year_data.get('year') == 2023:
                    revenue = year_data.get('revenue', 0)
                    # Конвертируем из рублей в миллионы, если нужно
                    return revenue / 1_000_000 if revenue > 1_000_000 else revenue
        return 0.0

    def _get_main_okved(self, okved_list: list) -> str:
        """Возвращает основной код ОКВЭД."""
        return okved_list[0].get('code') if okved_list else ''

    def _format_contacts(self, data: dict) -> str:
        """Форматирует контактную информацию."""
        contacts = []
        if data.get('phone'): contacts.append(f"тел: {data['phone']}")
        if data.get('email'): contacts.append(f"email: {data['email']}")
        return '; '.join(contacts)

    def determine_segment_tag(self, company_name: str, description: str = '') -> str:
        """Определяет сегментные теги на основе названия и описания."""
        tags = set()
        full_text = f"{company_name} {description}".lower()
        for tag, keywords in self.SEGMENT_KEYWORDS.items():
            if any(keyword in full_text for keyword in keywords):
                tags.add(tag)
        return ','.join(sorted(tags)) if tags else 'BTL'  # По умолчанию BTL