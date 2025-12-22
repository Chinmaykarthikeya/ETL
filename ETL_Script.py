import logging
import pandas as pd
import datetime as dt
import os


# ---------------------------------------------------------
# Creating the Configuration for the Log
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_process.log"), 
        logging.StreamHandler(sys.stdout)       
    ]
)

logger = logging.getLogger(__name__)

def run_etl_pipeline():
    
    logger.info(">>> ETL Process Started")
    sales_dir = "../Sales_Split"
    # ---------------------------------------------------------
    # Checking File Path
    # ---------------------------------------------------------
    try:
        if not os.path.exists(sales_dir):
            logger.error(f">>> Critical Error: Directory '{sales_dir}' not found.")
            return
        file_list = [f for f in os.listdir(sales_dir) if f.endswith('.csv')]
        file_count = len(file_list)
        logger.info(f">>> Scanning directory Ended.")
        logger.info(f">>> Found {file_count} sales files to process.")
        if file_count == 0:
            logger.warning(">>> No CSV files found in the directory. Stopping process.")
            return
    except Exception as e:
        logger.error(f">>> Error accessing directory '{sales_dir}': {e}")
        return
    # ---------------------------------------------------------
    # Loading and Merging the datasets
    # ---------------------------------------------------------
    try:
        data_frames = []
        
        logger.info(">>> Reading files ...")
        for filename in file_list:
            file_path = os.path.join(sales_dir, filename)
            try:
                # Read individual file
                df = pd.read_csv(file_path)
                data_frames.append(df)
            except Exception as e:
                logger.error(f">>> Failed to read file {filename}: {e}")
                continue
        if not data_frames:
            logger.error(">>> No valid dataframes loaded.")
            return
        all_sales_data = pd.concat(data_frames, ignore_index=True)
        total_rows = len(all_sales_data)
        logger.info(f">>> Successfully merged {len(data_frames)} files. Total sales records: {total_rows}")
        
    
        if total_rows > 0:
            logger.info(">>> Validation Passed: Sales data is not empty. Proceeding to Joins.")
        try:
                store_master = pd.read_csv('../store_master.csv')
                product_master = pd.read_csv('../product_master.csv')
                logger.info(f'>>> Total Rows in Store_Master Dataset {len(store_master)}')
                logger.info(f'>>> Total Rows in Product_Master Dataset {len(product_master)}')
        except FileNotFoundError as e:
                logger.error(f">>> Critical Error: Master file missing - {e}")
                return
        
        
        # -----------------------------------------------------
            # Left Joing the three datasets Extracted.
            # -----------------------------------------------------
        merged_df = pd.merge(all_sales_data, store_master, on='store_id', how='left')
        logger.info('>>> Joined Sales Data with Store Dataset')
        merged_df = pd.merge(merged_df, product_master, on='sku', how='left')
        logger.info(">>> Joined Sales Data with Product Dataset.")
        
        # -----------------------------------------------------
            # 5.Filtering for the store with active status column
            # -----------------------------------------------------
        if 'status' in merged_df.columns:
            active_sales_df = merged_df[merged_df['status'] == 'Active'].copy()
            logger.info(">>> Filtered for Active stores.")
        else:
            logger.warning(">>> Warning: 'status' column not found. Skipping filter.")
            active_sales_df = merged_df
        # -----------------------------------------------------
            # 6. Aggreating the Data with  sum() function
            # -----------------------------------------------------
        logger.info(">>> Aggregating data...")
        agg_list=['store_id', 'electronics_type', 'classification']
        name_qunty=['sales_qty', 'sales_value']
        aggregated_df = active_sales_df.groupby(
                agg_list
            )[name_qunty].sum().reset_index()
        
        # -----------------------------------------------------
            # Saving the Aggregated data
            # -----------------------------------------------------
        output_file = 'aggregated_sales.csv'
        aggregated_df.to_csv(output_file, index=False)
        logger.info(f">>> SUCCESS. Final output saved as '{output_file}'.")
        
    except Exception as e:
        logger.error(f">>> Error loading data: {e}")
        return
    
    
if __name__ == "__main__":
    run_etl_pipeline()