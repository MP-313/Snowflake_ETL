import json
import pandas as pd
from datetime import datetime
import logging
import os
from snowflake.snowpark import Session
from snowflake.snowpark import types
import numpy as np

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SNOWFLAKE_ACCOUNT = 'snowflake'
SNOWFLAKE_USER = 'DEV'
SNOWFLAKE_PASSWORD = 'abcd'
SNOWFLAKE_DATABASE = 'DEV_DB'  # Snowflake database name
SNOWFLAKE_SCHEMA = 'RAW_STAGE'  # Snowflake schema name
SNOWFLAKE_WAREHOUSE = 'NABLE_DEV_WH'

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
        
        # Create main tables with details as VARIANT to store semi-structured data
        logger.info(self.session.sql("""
            CREATE OR REPLACE TABLE dev_raw_db.raw_stage.products (
                manufacturer STRING,
                sku STRING,
                category STRING,
                title STRING,
                details STRING,  -- Store semi-structured data here
                updated_on TIMESTAMP_NTZ
            )
        """).collect())

        logger.info(self.session.sql("""
            CREATE OR REPLACE TABLE dev_raw_db.raw_stage.prices (
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
            # Read and validate products
            with open(json_file, 'r') as f:
                products = json.load(f)
            
            # Transform products into DataFrame
            product_data = []
            for p in products:
                if all(k in p for k in ['Manufacturer', 'SKU', 'Category', 'Title', 'Details', 'UpdatedOnUTC']):
                    product_data.append({
                        'manufacturer': str(p['Manufacturer']),
                        'sku': str(p['SKU']),
                        'category': str(p['Category']),
                        'title': str(p['Title']),
                        # Directly store details as a dictionary (semi-structured data)
                        'details': json.dumps(p['Details']),  # No json.dumps here
                        # Convert UpdatedOnUTC to datetime object and then to string for Snowflake compatibility
                        'updated_on': datetime.fromisoformat(p['UpdatedOnUTC'].replace('Z', '+00:00'))
                    })
                else:
                    logger.warning(f"Skipping invalid product: {p}")
            
            # Log a preview of the DataFrame for debugging
            if product_data:
                df = pd.DataFrame(product_data)
                df['updated_on'] = pd.to_datetime(df['updated_on']).astype(str)
                df['details'] = df['details'].astype(str)
                #logger.info(f"DataFrame head: {df.head()}")

                # Convert pandas DataFrame to Snowpark DataFrame
                records = df.to_dict(orient='records')
                #print(records)
                snowflake_df = session.create_dataframe(records)

                try:
                    snowflake_df.write.mode('append').saveAsTable('dev_raw_db.raw_stage.products')
                    logger.info(f"Data staged successfully into: dev_raw_db.raw_stage.products.")
                except Exception as e:
                    logger.error(f"Failed definition load with exception: {e}")
                    raise Exception(f"An error occurred while loading data into dev_raw_db.raw_stage.products Snowflake table:", str(e))

        except Exception as e:
            logger.error(f"Error loading products: {str(e)}")
            raise


    def load_prices(self, csv_file, distributor):
        """Load price data from CSV file"""
        logger.info(f"Processing prices from {distributor}: {csv_file}")
        
        try:
            # Read CSV directly into DataFrame
            df = pd.read_csv(csv_file, quotechar='"')
            
            # Add distributor and timestamp
            df['distributor'] = distributor
            df['updated_on'] = datetime.now()
            df['updated_on'] = df['updated_on'].astype(str)
            df['SKU'] = df['SKU'].astype(str)
            df['Manufacturer'] = df['Manufacturer'].astype(str)
            df['distributor'] = df['distributor'].astype(str)
            
            # Rename columns to match Snowflake
            df.columns = [col.lower() for col in df.columns]
            
            # Convert pandas DataFrame to Snowpark DataFrame
            records = df.to_dict(orient='records')
            snowflake_df = session.create_dataframe(records)

            try:
                snowflake_df.write.mode('append').saveAsTable('dev_raw_db.raw_stage.prices')
                logger.info(f"Data staged successfully into: dev_raw_db.raw_stage.prices.")
            except Exception as e:
                logger.error(f"Failed definition load with exception: {e}")
                raise Exception(f"An error occurred while loading data into dev_raw_db.raw_stage.prices Snowflake table:", str(e))

            
        except Exception as e:
            logger.error(f"Error loading prices: {str(e)}")
            raise

    def generate_report(self):
        logger.info("Generating summary report...")
        try:
            # Collecting all statistics for the summary report
            stats = {
                # Number of Unique Product Records
                'unique_product_records': self.session.sql("SELECT COUNT(DISTINCT sku) FROM dev_raw_db.raw_stage.products").collect()[0][0],
                
                # Number of Unique Manufacturers
                'unique_manufacturers': self.session.sql("SELECT COUNT(DISTINCT manufacturer) FROM dev_raw_db.raw_stage.products").collect()[0][0],
                
                # Number of Unique Categories
                'unique_categories': self.session.sql("SELECT COUNT(DISTINCT category) FROM dev_raw_db.raw_stage.products").collect()[0][0],
                
                # Number of Unique Price Records (unique by sku and distributor)
                'unique_price_records': self.session.sql("SELECT COUNT(DISTINCT sku, distributor) FROM dev_raw_db.raw_stage.prices").collect()[0][0],
                
                # Number of Unique Distributors
                'unique_distributors': self.session.sql("SELECT COUNT(DISTINCT distributor) FROM dev_raw_db.raw_stage.prices").collect()[0][0],
                
                # Number of Product Records per Category
                'product_records_per_category': self.session.sql("""
                    SELECT category, COUNT(sku) AS product_count
                    FROM dev_raw_db.raw_stage.products
                    GROUP BY category
                """).collect(),

                # Number of Price Records per Distributor
                'price_records_per_distributor': self.session.sql("""
                    SELECT distributor, COUNT(DISTINCT sku) AS price_count
                    FROM dev_raw_db.raw_stage.prices
                    GROUP BY distributor
                """).collect(),

                # Number of Product Records per Manufacturer
                'product_records_per_manufacturer': self.session.sql("""
                    SELECT manufacturer, COUNT(sku) AS product_count
                    FROM dev_raw_db.raw_stage.products
                    GROUP BY manufacturer
                """).collect()
            

            }

            # Returning the collected stats (You may format this further before returning as a PDF)
            return stats

        except Exception as e:
            logger.error(f"Error generating report: {str(e)}")
            raise


def main():
    try:
        # Create ETL pipeline
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
        
        # Write report to a text file
        with open('summary_report.txt', 'w') as f:
            f.write(json.dumps(report, indent=2))
        logger.info("Report written to summary_report.txt")
        
    except Exception as e:
        logger.error(f"Error in ETL process: {str(e)}")
        raise

if __name__ == "__main__":
    main()
