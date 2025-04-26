"""
Crawl Season Data Script - Optimized for Big Data Pipelines
"""

from src.config import settings
from src.f1_data_collector.collector import F1DataCollector
from data.utils import DataUtils
from pathlib import Path
import logging
from typing import Dict, Any
import pandas as pd

# Configure logging
logging.basicConfig(
    level=settings.LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def crawl_season(year: int) -> Dict[str, Any]:
    """Crawl và xử lý dữ liệu cho một mùa giải cụ thể"""
    collector = F1DataCollector()
    results = {}
    
    try:
        logger.info(f"Initializing crawl for season {year}")
        schedule = DataUtils.load_season_schedule(year)
        
        if schedule is None or schedule.empty:
            logger.warning(f"No schedule found for {year}")
            return {}
            
        valid_events = collector.filter_official_events(schedule)
        logger.info(f"Found {len(valid_events)} valid events in {year}")
        
        for _, event in valid_events.iterrows():
            event_name = event['EventName']
            event_date = event['EventDate']
            
            logger.debug(f"Processing event: {event_name} ({event_date})")
            try:
                # Collect và validate data
                event_data = collector.collect_event_data(year, event_name)
                if not DataUtils.validate_data_stats(event_data, settings.MIN_RECORDS_PER_EVENT):
                    raise ValueError("Insufficient event data")
                
                # Add metadata
                event_data = DataUtils.add_metadata_columns(event_data)
                event_data = DataUtils.enforce_schema(event_data, settings.feature_schema)
                
                # Generate output path
                output_path = Path(
                    settings.RAW_DATA_DIR,
                    f"year={year}",
                    f"event={DataUtils.sanitize_filename(event_name)}"
                )
                
                # Write data với partition
                DataUtils.write_parquet(
                    df=event_data,
                    path=str(output_path),
                    partition_cols=['year', 'event_name']
                )
                
                results[event_name] = {
                    "status": "success",
                    "path": str(output_path),
                    "records": event_data.shape[0],
                    "columns": list(event_data.columns)
                }
                
            except Exception as e:
                logger.error(f"Event {event_name} failed: {str(e)}")
                results[event_name] = {
                    "status": "failed",
                    "error": str(e)
                }
                continue
                
        return {year: results}
        
    except Exception as e:
        logger.critical(f"Season {year} crawl failed: {str(e)}", exc_info=True)
        return {year: {"status": "critical_error", "error": str(e)}}

def main():
    """Main execution flow với xử lý parallel tiềm năng"""
    final_results = {}
    
    for year in settings.SEASONS:
        logger.info(f"Starting processing for year {year}".center(60, '='))
        season_result = crawl_season(year)
        final_results.update(season_result)
        
        # Interim save để phục hồi sau lỗi
        DataUtils.save_json(final_results, settings.INTERIM_DIR / "crawl_progress.json")
    
    # Final output
    DataUtils.save_json(final_results, settings.PROCESSED_DIR / "crawl_summary.json")
    logger.info(f"Crawl completed. Results saved to {settings.PROCESSED_DIR}")

if __name__ == "__main__":
    main()
