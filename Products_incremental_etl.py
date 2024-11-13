import json
import logging
from datetime import datetime
from snowflake.snowpark import Session
from config import SnowflakeConfig

# logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProductsETL:
    def __init__(self, session):
        self.session = session
        self.setup_tables()

    def setup_tables(self):
        """Create required tables including staging and audit tables"""
        logger.info("Setting up tables if they don't exist \n")

        # Create staging table
        logger.info(self.session.sql("""
            CREATE TABLE IF NOT EXISTS ETL.ETL_SHI.stg_products (
                manufacturer STRING,
                sku STRING,
                category STRING,
                title STRING,
                details STRING,
                updated_on TIMESTAMP_NTZ,
                ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """).collect())

        # target table with history tracking
        logger.info(self.session.sql("""
            CREATE TABLE IF NOT EXISTS ETL.ETL_SHI.products_v2 (
                manufacturer STRING,
                sku STRING,
                category STRING,
                title STRING,
                details STRING,
                updated_on TIMESTAMP_NTZ,
                valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                valid_to TIMESTAMP_NTZ DEFAULT NULL,
                is_current BOOLEAN DEFAULT TRUE,
                ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (manufacturer, sku, valid_from)
            )
        """).collect())

        # Create audit table for logs
        logger.info(self.session.sql("""
            CREATE TABLE IF NOT EXISTS ETL.ETL_SHI.etl_audit_log (
                --audit_id NUMBER AUTOINCREMENT,
                table_name STRING,
                operation STRING,
                records_processed INTEGER,
                start_time TIMESTAMP_NTZ,
                end_time TIMESTAMP_NTZ,
                status STRING,
                error_message STRING
            )
        """).collect())

    def load_products_incremental(self, json_file):
        """Load product data incrementally from JSON file"""
        try:
            start_time = datetime.now()

            with open(json_file, 'r', encoding='utf-8') as f:
                products = json.load(f)
            
            # Prepare records for staging
            records = [
                {
                    'manufacturer': str(p['Manufacturer']),
                    'sku': str(p['SKU']),
                    'category': str(p['Category']),
                    'title': str(p['Title']),
                    'details': json.dumps(p['Details']),
                    'updated_on': datetime.fromisoformat(p['UpdatedOnUTC'].replace('Z', '+00:00'))
                }
                for p in products if all(k in p for k in ['Manufacturer', 'SKU', 'Category', 'Title', 'Details', 'UpdatedOnUTC'])
            ]

            if records:
                staging_df = self.session.create_dataframe(records)
                staging_df.write.mode('overwrite').saveAsTable('ETL.ETL_SHI.stg_products')


                print("Staging of data complete \n")
                #merge used to perform upserts into target table tgt
                merge_result = self.session.sql("""
                    MERGE INTO ETL.ETL_SHI.products_v2 tgt
                    USING (
                        SELECT * FROM ETL.ETL_SHI.stg_products
                    ) src
                    ON tgt.manufacturer = src.manufacturer 
                    AND tgt.sku = src.sku
                    AND tgt.is_current = TRUE --indicates most current record
                    WHEN MATCHED 
                        AND (tgt.updated_on < src.updated_on 
                             OR tgt.details != src.details 
                             OR tgt.category != src.category 
                             OR tgt.title != src.title)
                    THEN UPDATE SET 
                        valid_to = CURRENT_TIMESTAMP(),
                        is_current = FALSE
                  WHEN NOT MATCHED THEN
                    INSERT (manufacturer, sku, category, title, details, updated_on, valid_from, is_current)
                    VALUES (src.manufacturer, src.sku, src.category, src.title, src.details, src.updated_on, 
                            CURRENT_TIMESTAMP(), 
                            CASE 
                                WHEN src.updated_on = (SELECT MAX(updated_on) FROM ETL.ETL_SHI.stg_products WHERE manufacturer = src.manufacturer AND sku = src.sku) 
                                THEN TRUE
                                ELSE FALSE
                            END)
                            """).collect()

                # Insert new versions for updated records
                self.session.sql("""
                    INSERT INTO ETL.ETL_SHI.products_v2
                    SELECT 
                        src.manufacturer,
                        src.sku,
                        src.category,
                        src.title,
                        src.details,
                        src.updated_on,
                        CURRENT_TIMESTAMP() as valid_from,
                        NULL as valid_to, -- OR '9999-12-31'
                        TRUE as is_current,
                        CURRENT_TIMESTAMP() as ingestion_timestamp
                    FROM ETL.ETL_SHI.stg_products src
                    JOIN ETL.ETL_SHI.products_v2 tgt
                        ON src.manufacturer = tgt.manufacturer
                        AND src.sku = tgt.sku
                        AND tgt.valid_to = CURRENT_TIMESTAMP()
                """).collect()

                self.log_audit('products_v2', 'MERGE', len(records), start_time)

            else:
                logger.warning("No valid product records found to load.")

        except Exception as e:
            self.log_audit('products_v2', 'MERGE', 0, start_time, status='ERROR', error_message=str(e))
            logger.error(f"Error loading products_v2: {str(e)}")
            raise

    def log_audit(self, table_name, operation, records_processed, start_time, 
                 status='SUCCESS', error_message=None):
        """Log ETL operation to audit table"""
        try:
            end_time = datetime.now()
            audit_record = [{
                'table_name': table_name,
                'operation': operation,
                'records_processed': records_processed,
                'start_time': start_time,
                'end_time': end_time,
                'status': status,
                'error_message': error_message
            }]
            
            audit_df = self.session.create_dataframe(audit_record)
            audit_df.write.mode('append').saveAsTable('ETL.ETL_SHI.etl_audit_log')
            
        except Exception as e:
            logger.error(f"Error logging audit record: {str(e)}")

def main():
    try:
        snow_config = SnowflakeConfig()
        session = snow_config.create_session()

        # Create ETL pipeline
        etl = ProductsETL(session)
        
        # Load product data incrementally
        etl.load_products_incremental('products.json')
        
    except Exception as e:
        logger.error(f"Error in ETL process: {str(e)}")
        raise
    finally:
        session.close()

if __name__ == "__main__":
    main()
