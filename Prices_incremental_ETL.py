import logging
from datetime import datetime
from snowflake.snowpark import Session
import pandas as pd
from config import SnowflakeConfig

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PricesETL:
    def __init__(self, session):
        self.session = session
        self.setup_tables()

    def setup_tables(self):
        """Create required tables including staging and audit tables"""
        logger.info("Setting up tables if they don't exist...")

        # Create staging table
        logger.info(self.session.sql("""
            CREATE TABLE IF NOT EXISTS ETL.ETL_SHI.stg_prices (
                manufacturer STRING,
                sku STRING,
                price FLOAT,
                quantity INTEGER,
                distributor STRING,
                updated_on TIMESTAMP_NTZ,
                ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """).collect())

        # Create target table with history tracking
        logger.info(self.session.sql("""
            CREATE TABLE IF NOT EXISTS ETL.ETL_SHI.prices (
                manufacturer STRING,
                sku STRING,
                price FLOAT,
                quantity INTEGER,
                distributor STRING,
                updated_on TIMESTAMP_NTZ,
                valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                valid_to TIMESTAMP_NTZ DEFAULT NULL,
                is_current BOOLEAN DEFAULT TRUE,
                ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (manufacturer, sku, distributor, valid_from)
            )
        """).collect())

        # Create audit table
        logger.info(self.session.sql("""
            CREATE TABLE IF NOT EXISTS ETL.ETL_SHI.etl_audit_log (
                audit_id NUMBER AUTOINCREMENT,
                table_name STRING,
                operation STRING,
                records_processed INTEGER,
                records_inserted INTEGER,
                records_updated INTEGER,
                start_time TIMESTAMP_NTZ,
                end_time TIMESTAMP_NTZ,
                status STRING,
                error_message STRING
            )
        """).collect())

    def load_prices_incremental(self, csv_file, distributor):
        """Load price data incrementally from CSV file"""
        try:
            start_time = datetime.now()
            
            # Read CSV and prepare records
            df = pd.read_csv(csv_file, header=0, quotechar='"')
            df["distributor"] = distributor
            df["updated_on"] = datetime.now()

            if df.shape[0] > 0:
                # Load to staging
                staging_df = self.session.create_dataframe(df)
                staging_df.write.mode('overwrite').saveAsTable('ETL.ETL_SHI.stg_prices')

                # Perform merge operation
                merge_result = self.session.sql("""
                    MERGE INTO ETL.ETL_SHI.prices tgt
                    USING (
                        SELECT * FROM ETL.ETL_SHI.stg_prices
                    ) src
                    ON tgt.manufacturer = src.manufacturer 
                    AND tgt.sku = src.sku
                    AND tgt.distributor = src.distributor
                    AND tgt.is_current = TRUE
                    WHEN MATCHED 
                        AND (tgt.price != src.price 
                             OR tgt.quantity != src.quantity)
                    THEN UPDATE SET 
                        valid_to = CURRENT_TIMESTAMP(),
                        is_current = FALSE
                    WHEN NOT MATCHED THEN
                    INSERT (manufacturer, sku, price, quantity, distributor, updated_on, valid_from, is_current)
                    VALUES (src.manufacturer, src.sku, src.price, src.quantity, src.distributor, 
                           src.updated_on, CURRENT_TIMESTAMP(), TRUE)
                """).collect()

                # Insert new versions for updated records
                self.session.sql("""
                    INSERT INTO ETL.ETL_SHI.prices
                    SELECT 
                        src.manufacturer,
                        src.sku,
                        src.price,
                        src.quantity,
                        src.distributor,
                        src.updated_on,
                        CURRENT_TIMESTAMP() as valid_from,
                        NULL as valid_to,
                        TRUE as is_current,
                        CURRENT_TIMESTAMP() as ingestion_timestamp
                    FROM ETL.ETL_SHI.stg_prices src
                    JOIN ETL.ETL_SHI.prices tgt
                        ON src.manufacturer = tgt.manufacturer
                        AND src.sku = tgt.sku
                        AND src.distributor = tgt.distributor
                        AND tgt.valid_to = CURRENT_TIMESTAMP()
                """).collect()

                # Log
                self.log_audit('prices', 'MERGE', df.shape[0], start_time)

            else:
                logger.warning("No valid price records found to load.")

        except Exception as e:
            self.log_audit('prices', 'MERGE', 0, start_time, status='ERROR', error_message=str(e))
            logger.error(f"Error loading prices: {str(e)}")
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

        etl = PricesETL(session)
        
        # Load price data incrementally from multiple sources
        etl.load_prices_incremental('jillsjunk.csv', 'Jills Junk')
        etl.load_prices_incremental('samsstuff.csv', 'Sams Stuff')
        
    except Exception as e:
        logger.error(f"Error in ETL process: {str(e)}")
        raise
    finally:
        session.close()

if __name__ == "__main__":
    main()
