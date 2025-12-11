import pandas as pd
import logging
import time
from data_fetcher import DataFetcher
from data_processor import DataProcessor

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    # 1. Инициализация
    logger.info("=" * 60)
    logger.info("ЗАПУСК СБОРА ДАННЫХ О BTL АГЕНТСТВАХ")
    logger.info("=" * 60)
    
    # ВАЖНО: Получите новый ключ на checko.ru и замените этот!
    CHECKO_API_KEY = "YnFR1HbSIXBUnk6b"
    
    if CHECKO_API_KEY == "YnFR1HbSIXBUnk6b":
        logger.warning("API ключ не установлен! Будут использованы тестовые данные.")
    
    fetcher = DataFetcher(CHECKO_API_KEY)
    processor = DataProcessor()

    # 2. Получение "семенного" списка компаний
    logger.info("\n🔍 Этап 1: Получение списка компаний...")
    seed_companies = fetcher.parse_industry_rating("https://www.sostav.ru/ratings/agency/")
    
    if seed_companies.empty:
        logger.error("Не удалось получить seed-список. Завершение работы.")
        return
    
    logger.info(f"Получено {len(seed_companies)} компаний для обработки")
    logger.info(f"Примеры: {seed_companies['name'].head(3).tolist()}")

    # 3. Обогащение данных через API
    logger.info("\n🌐 Этап 2: Запрос данных через API...")
    enriched_data = []
    
    # В цикле обогащения данных:
    for idx, row in seed_companies.iterrows():
        company_inn = row['inn']
        company_name = row['name']  # Берём оригинальное название из seed-списка
    
        logger.info(f"[{idx+1}/{len(seed_companies)}] Запрашиваем данные для: {company_name} (ИНН: {company_inn})")
    
        # Передаём ОБА параметра: ИНН и оригинальное название
        company_info = fetcher.fetch_company_via_api_by_inn(company_inn, company_name)
    
        if company_info:
            # Определяем теги сегмента
            company_info['segment_tag'] = fetcher.determine_segment_tag(
                company_info['name'], 
                company_info.get('description', '')
            )
            company_info['rating_ref'] = row['rating_ref']
            enriched_data.append(company_info)
        
            logger.info(f"  ✓ Получено: {company_info['name']} - Выручка: {company_info['revenue']:.1f} млн руб")
        else:
            logger.warning(f"  ✗ Не удалось получить данные для {company_name}")
    
        # Пауза между запросами
        if idx < len(seed_companies) - 1:
            time.sleep(1.5)

    # 4. Обработка и фильтрация
    logger.info("\n🔄 Этап 3: Обработка данных...")
    
    if not enriched_data:
        logger.error("Не удалось получить данные ни по одной компании. Завершение работы.")
        return
    
    raw_df = pd.DataFrame(enriched_data)
    logger.info(f"Собрано сырых данных: {len(raw_df)} записей")
    
    clean_df = processor.normalize_data(raw_df)
    logger.info(f"После нормализации: {len(clean_df)} записей")
    
    final_df = processor.filter_companies(clean_df)
    logger.info(f"После фильтрации: {len(final_df)} записей")
    
    # 5. Сохранение результата
    logger.info("\n💾 Этап 4: Сохранение результатов...")
    output_path = "../data/companies.csv"
    
    try:
        final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"✅ Файл успешно сохранен: {output_path}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении файла: {e}")
        # Пробуем альтернативный путь
        output_path = "companies.csv"
        final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"Файл сохранен по альтернативному пути: {output_path}")

    # 6. Вывод статистики
    logger.info("\n📊 ИТОГОВАЯ СТАТИСТИКА:")
    logger.info("=" * 40)
    
    if not final_df.empty:
        print(f"\n{'='*60}")
        print("🎉 СБОР ДАННЫХ УСПЕШНО ЗАВЕРШЕН!")
        print(f"{'='*60}\n")
        
        print(f"📁 Файл с результатами: {output_path}")
        print(f"📈 Всего компаний в базе: {len(final_df)}")
        print(f"💰 Средняя выручка: {final_df['revenue'].mean():.1f} млн руб")
        print(f"📊 Диапазон выручки: от {final_df['revenue'].min():.1f} до {final_df['revenue'].max():.1f} млн руб")
        
        if 'segment_tag' in final_df.columns:
            print("\n🏷️  Распределение по тегам:")
            tag_counts = final_df['segment_tag'].value_counts()
            for tag, count in tag_counts.items():
                print(f"  • {tag}: {count} компаний")
        
        if 'source' in final_df.columns:
            print(f"\n📡 Источники данных: {', '.join(final_df['source'].unique())}")
        
        # Показываем первые 3 записи
        print(f"\n👀 Пример данных (первые 3 записи):")
        print(final_df[['name', 'inn', 'revenue', 'segment_tag']].head(3).to_string(index=False))
        
        print(f"\n{'='*60}")
        print("💡 Следующие шаги:")
        print("1. Получите реальный API ключ на checko.ru")
        print("2. Замените тестовые ИНН на реальные из list-org.com")
        print("3. Добавьте парсинг реальных рейтингов")
        print(f"{'='*60}")
    else:
        logger.warning("После фильтрации не осталось ни одной компании, соответствующей критериям.")

if __name__ == "__main__":
    main()