from pathlib import Path
# import datetime
import logging
import argparse
import pandas as pd

# LOGs file parsed as an argument for execution

# DATA_PATH = Path("ETL2/data/orders_cities.xlsx")
# TODAY = datetime.datetime.now().date()      # log file changes daily (new day → new log filename)
# LOGS_FILE = Path(f"ETL2/logs/etl_orders_cities_{TODAY}.log")
# OUTPUT_PATH = Path("ETL2/output/orders_cities_clean_v2.csv")

# Parsing..
def parsing_arguments():
    parser = argparse.ArgumentParser(description="ETL pipeline input/output/logs")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--logfile", required=True) # user sets where to log
    return parser.parse_args()

# Logging..
def logging_setup(logfile):                         # logfile is the variable in the function
    # LOGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=logfile,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filemode="a"
    )

def main():
    args = parsing_arguments()
    logging_setup(args.logfile)                     # logs filename changed depending on parsed arguments
    logging.info(f"ETL process started: Input => {args.input} | Output file => {args.output} | Logs File => {args.logfile}")
            # log processed files
    
    # Data Import..
    try:
        df_raw = pd.read_excel(args.input, engine='openpyxl')
        logging.info(f"Input file successfully loaded!")
    except FileNotFoundError:
        logging.error(f"Input file not found: {args.input}")
        raise SystemExit(f"❌ Input file not found: {args.input}")

    # Drop nulls..
    df_clean = df_raw.dropna().copy()
    logging.info(f"Nulls removed! Rows left: {len(df_clean)}")

    # Unify names..
    df_clean["city"] = (
        df_clean["city"]
            .str.title()
            .replace({"Denvre": "Denver", "nyc": "new york", "la": "los angeles"})
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

    logging.info(f"'cost' Datatype set to: {df_clean["cost"].dtype}!")

    # Output Export..
    logging.info(f"Saving cleaned data to: {args.output}...")
    # OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_clean.to_csv(args.output, index=False)

    # print(df_clean)
    logging.info(f"Export Complete!")
    print("✅ All Done!!")

    # Reviews to confirm the transformation
    logging.info(f"Preview of cleaned data:\n{df_clean.head()}")
    logging.info(f"Columns DataTypes:\n{df_clean.dtypes}")

if __name__ == "__main__":
    try:                    # Log unexpected errors if program fails
        main()
        logging.info("Pipeline executed without fatal errors.")
    except Exception:
        logging.exception("Critical failure during ETL execution!")
        raise SystemExit(f"❌ Main function failed!")
delimiter = "_________________________________________________________"
logging.info(delimiter)