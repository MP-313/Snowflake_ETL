import json
import logging
from datetime import datetime
from snowflake.snowpark import Session
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SNOWFLAKE_ACCOUNT = 'dev'
SNOWFLAKE_USER = 'DEV'
SNOWFLAKE_PASSWORD = 'abcd'
SNOWFLAKE_DATABASE = 'DEV_RAW_DB'  
SNOWFLAKE_SCHEMA = 'RAW_STAGE'  
SNOWFLAKE_WAREHOUSE = 'DEV_WH'

# Create a dictionary for the connection options
connection_parameters = {
    "account": SNOWFLAKE_ACCOUNT,
    "user": SNOWFLAKE_USER,
    "password": SNOWFLAKE_PASSWORD,
    "database": SNOWFLAKE_DATABASE,
    "schema": SNOWFLAKE_SCHEMA,
    "warehouse": SNOWFLAKE_WAREHOUSE
}

# Snowflake session creation
session = Session.builder.configs(connection_parameters).create()
print("Snowflake session created successfully.")

class SimpleSnowflakeETL:
    def __init__(self, session):
        self.session = session
        self.setup_tables()

    def setup_tables(self):
        """Create required tables if they don't exist"""
        logger.info("Setting up tables if they don't exist...")

        logger.info(self.session.sql("""
            CREATE TABLE IF NOT EXISTS dev_raw_db.raw_stage.products (
                manufacturer STRING,
                sku STRING,
                category STRING,
                title STRING,
                details STRING,
                updated_on TIMESTAMP_NTZ
            )
        """).collect())

        logger.info(self.session.sql("""
            CREATE TABLE IF NOT EXISTS dev_raw_db.raw_stage.prices (
                manufacturer STRING,
                sku STRING,
                price FLOAT,
                quantity INTEGER,
                distributor STRING,
                updated_on TIMESTAMP_NTZ
            )
        """).collect())

    def load_products(self, json_file):
        """Load product data from JSON file"""
        logger.info(f"Processing file: {json_file}")

        try:
            # Read JSON data
            with open(json_file, 'r') as f:
                products = json.load(f)
            
            # Prepare records for Snowflake directly
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
                snowflake_df = self.session.create_dataframe(records)
                snowflake_df.write.mode('append').saveAsTable('dev_raw_db.raw_stage.products')
                logger.info("Data staged successfully into: dev_raw_db.raw_stage.products.")
            else:
                logger.warning("No valid product records found to load.")

        except Exception as e:
            logger.error(f"Error loading products: {str(e)}")
            raise

    def load_prices(self, csv_file, distributor):
        """Load price data from CSV file"""
        logger.info(f"Processing prices from {distributor}: {csv_file}")

        try:
            # Read CSV and prepare records
            df = pd.read_csv(csv_file, header=0, quotechar='"')
            df["distributor"] = distributor
            df["updated_on"] = datetime.now()

            # Convert pandas data  types to be compatible with snowflake
            for col in df.columns:
                if df[col].dtype == 'object' or pd.api.types.is_datetime64_any_dtype(df[col]):  # Check if the column is of object type
                    df[col] = df[col].astype(str)

            if df.shape[0] > 0:
                # Write the data directly to Snowflake
                snowflake_df = self.session.create_dataframe(df.to_dict(orient="records"))
                snowflake_df.write.mode('append').saveAsTable('dev_raw_db.raw_stage.prices')
                logger.info(f"Data staged successfully into: dev_raw_db.raw_stage.prices.")
            else:
                logger.warning("No valid price records found to load.")

        except Exception as e:
            logger.error(f"Error loading prices: {str(e)}")
            raise

    def generate_report(self):
        logger.info("Generating summary report...")
        try:
            # Collecting all statistics for the summary report
            stats = {
                'unique_product_records': self.session.sql("SELECT COUNT(DISTINCT sku) FROM dev_raw_db.raw_stage.products").collect()[0][0],
                'unique_manufacturers': self.session.sql("SELECT COUNT(DISTINCT manufacturer) FROM dev_raw_db.raw_stage.products").collect()[0][0],
                'unique_categories': self.session.sql("SELECT COUNT(DISTINCT category) FROM dev_raw_db.raw_stage.products").collect()[0][0],
                'unique_price_records': self.session.sql("SELECT COUNT(DISTINCT sku, distributor) FROM dev_raw_db.raw_stage.prices").collect()[0][0],
                'unique_distributors': self.session.sql("SELECT COUNT(DISTINCT distributor) FROM dev_raw_db.raw_stage.prices").collect()[0][0],
                'product_records_per_category': self.session.sql("""
                    SELECT category, COUNT(sku) AS product_count
                    FROM dev_raw_db.raw_stage.products
                    GROUP BY category
                """).collect(),
                'price_records_per_distributor': self.session.sql("""
                    SELECT distributor, COUNT(DISTINCT sku) AS price_count
                    FROM dev_raw_db.raw_stage.prices
                    GROUP BY distributor
                """).collect(),
                'product_records_per_manufacturer': self.session.sql("""
                    SELECT manufacturer, COUNT(sku) AS product_count
                    FROM dev_raw_db.raw_stage.products
                    GROUP BY manufacturer
                """).collect()
            }

            return stats

        except Exception as e:
            logger.error(f"Error generating report: {str(e)}")
            raise

def main():
    try:
        etl = SimpleSnowflakeETL(session)
        
        # Load product data
        etl.load_products('products.json')
        
        # Load price data
        etl.load_prices('jillsjunk.csv', 'Jills Junk')
        etl.load_prices('samsstuff.csv', 'Sams Stuff')
        
        # Generate and print report
        report = etl.generate_report()
        logger.info("Summary Report:")
        logger.info(json.dumps(report, indent=2))
        
        # Write report to a summary_report file
        with open('summary_report.txt', 'w') as f:
            f.write(json.dumps(report, indent=2))
        logger.info("Report written to summary_report.txt")
        
    except Exception as e:
        logger.error(f"Error in ETL process: {str(e)}")
        raise

if __name__ == "__main__":
    main()
