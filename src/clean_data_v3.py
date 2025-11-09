from pathlib import Path
import logging
import pandas as pd

DATA_PATH = Path("ETL2/data/orders_cities.xlsx")
LOGS_FILE = Path("ETL2/logs/etl_orders_cities.log")
OUTPUT_PATH = Path("ETL2/output/orders_cities_clean_v2.csv")

# track pipeline for debugging → logs saved for analysis
LOGS_FILE.parent.mkdir(parents=True, exist_ok=True)     # create log-file if not exist
logging.basicConfig(
    filename=LOGS_FILE,             # where logs are saved
    level=logging.INFO,             # minimum type to log
    format="%(asctime)s - %(levelname)s - %(message)s",      # timestamp - severity lev. - error desc.
    filemode="a"                    # force logs appending (add-up logs to avoid losing content)
)

# Data Import..
try:
    df_raw = pd.read_excel(DATA_PATH, engine='openpyxl')
    logging.info(f"Input file successfully loaded!")
except FileNotFoundError:
    logging.error(f"Input file not found: {DATA_PATH}")
    raise SystemExit(f"❌ Input file not found: {DATA_PATH}")

def main():
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
            .str.replace(r"[^\d.,-]", "", regex=True)    # remove all not digit/decimal
            .str.replace(",", ".", regex=False)          # normalize decimal commas
    )
    df_clean["cost"] = pd.to_numeric(df_clean["cost"], errors="coerce") # convert to float + delete what's not

    logging.info(f"'cost' Datatype set to: {df_clean["cost"].dtype}!")

    # Output Export..
    logging.info(f"Saving cleaned data to: {OUTPUT_PATH}...")
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_clean.to_csv(OUTPUT_PATH, index=False)

    # print(df_clean)
    logging.info(f"✅ Export Complete!")
    print("✅ All Done!!")

    # Reviews to confirm the transformation
    logging.info("\n📌 Preview of cleaned data:")
    logging.info(df_clean.head())
    logging.info("\n📌 Columns DataTypes:")
    logging.info(df_clean.dtypes)
    delimiter = "_________________________________________________________"     # to separate new logs
    logging.info(delimiter)

if __name__ == "__main__":      # Only execute program if this file is run directly by Python
    main()