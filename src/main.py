# Это точка входа, которая запускает весь процесс одной командой python src/main.py.

# main.py
import pandas as pd
import logging
from src.data_fetcher import DataFetcher
from src.data_processor import DataProcessor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # 1. Инициализация
    CHECKO_API_KEY = "YnFR1HbSIXBUnk6b"  # Нужно получить на checko.ru
    fetcher = DataFetcher(CHECKO_API_KEY)
    processor = DataProcessor()

    # 2. Получение "семенного" списка компаний
    seed_companies = fetcher.parse_industry_rating("https://www.sostav.ru/ratings/agency/")
    if seed_companies.empty:
        logger.error("Не удалось получить seed-список. Завершение работы.")
        return

    # 3. Обогащение данных через API
    enriched_data = []
    for _, row in seed_companies.iterrows():
        company_name = row['name']
        logger.info(f"Запрашиваем данные для: {company_name}")
        company_info = fetcher.fetch_company_via_api(company_name)
        if company_info:
            company_info['segment_tag'] = fetcher.determine_segment_tag(
                company_info['name'], 
                company_info.get('description', '')
            )
            company_info['rating_ref'] = row['rating_ref']
            enriched_data.append(company_info)
        time.sleep(1)  # Уважаем лимиты API

    # 4. Обработка и фильтрация
    raw_df = pd.DataFrame(enriched_data)
    clean_df = processor.normalize_data(raw_df)
    final_df = processor.filter_companies(clean_df)

    # 5. Сохранение результата
    output_path = "data/companies.csv"
    final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"✅ Задание выполнено! Собрано {len(final_df)} компаний. Файл сохранен: {output_path}")

    # Вывод краткой статистики
    if not final_df.empty:
        print(f"\n📊 Статистика по результату:")
        print(f"   • Всего компаний: {len(final_df)}")
        print(f"   • Средняя выручка: {final_df['revenue'].mean():.1f} млн руб")
        print(f"   • Распределение по тегам: {final_df['segment_tag'].value_counts().to_dict()}")

if __name__ == "__main__":
    main()