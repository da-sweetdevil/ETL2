from pathlib import Path
import time
import datetime
import logging
import argparse
import pandas as pd

start_time = time.time()
# DATA_PATH = Path("ETL2/data/orders_cities.xlsx")
TODAY = datetime.datetime.now().date()      # log file changes daily (new day → new log filename)
LOGS_FILE = Path(f"ETL2/logs/etl_orders_cities_{TODAY}.log")
# OUTPUT_PATH = Path("ETL2/output/orders_cities_clean.csv")

# Parsing..
def parsing_arguments():
    parser = argparse.ArgumentParser(description="ETL pipeline input/output")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    # parser.add_argument("--logfile", required=True) # user sets where to log
    return parser.parse_args()

# Logging..
def logging_setup():
    LOGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=LOGS_FILE,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filemode="a"
    )

def main():
    args = parsing_arguments()
    logging_setup()
    logging.info(f"ETL process started: Input => {args.input} | Output file => {args.output} | Logs File => {LOGS_FILE}")
            # log processed files
    
    # Data Import..
    try:
        df_raw = pd.read_excel(args.input, engine='openpyxl')
        logging.info(f"Input file successfully loaded!")
    except FileNotFoundError:
        logging.error(f"Input file not found: {args.input}")
        raise SystemExit(f"❌ Input file not found: {args.input}")

    # Unify names..
    df_clean = df_raw.copy()
    df_clean["city"] = (
        df_clean["city"]
            .str.lower()    # to generalize .replace
            .replace({"denvre": "denver", "nyc": "new york", "la": "los angeles"})
            .str.title()
    )
    logging.info(f"City names standarized!")

    # Cost DataType..
    df_clean["cost"] = (
        df_clean["cost"]
            .astype(str)
            .str.replace(r"[^\d.,-]", "", regex=True)
            .str.replace(",", ".", regex=False)
    )
    df_clean["cost"] = pd.to_numeric(df_clean["cost"], errors="coerce")
    logging.info(f"'cost' Datatype set to: {df_clean['cost'].dtype}!")

    # Drop nulls..
    df_clean = df_clean.dropna()    # in case a cell becomes NaN after transformation
    logging.info("Nulls removed!")
    logging.info(f"Rows before cleaning: {len(df_raw)} | Rows left: {len(df_clean)}")

    # Output Export..
    logging.info(f"Saving cleaned data to: {args.output}...")
    # OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_clean.to_csv(args.output, index=False)
    logging.info(f"Export Complete!")

    # Reviews to confirm the transformation
    logging.info(f"Preview of cleaned data:\n{df_clean.head()}")
    logging.info(f"Columns DataTypes:\n{df_clean.dtypes}")

if __name__ == "__main__":
    try:                    # Log unexpected errors if program fails
        main()
        duration = round(time.time() - start_time, 2)       # count process ETA
        print("✅ All Done!!")
        print(f"ETA: {duration} seconds")
        logging.info("Pipeline executed without fatal errors.")
        logging.info(f"ETL completed in {duration} seconds.")

    except Exception as e:
        logging.exception(e) # log error (Exception) content
        raise SystemExit(e) # print the Exception
    finally:
        delimiter = "_________________________________________________________"
        logging.info(delimiter)