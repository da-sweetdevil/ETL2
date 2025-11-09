from pathlib import Path
import pandas as pd

DATA_PATH = Path("ETL2/data/orders_cities.xlsx")
OUTPUT_PATH = Path("ETL2/output/orders_cities_clean.csv")

# Data Import..
try:
    df_raw = pd.read_excel(DATA_PATH, engine='openpyxl')
except FileNotFoundError:
    raise SystemExit(f"❌ Input file not found: {DATA_PATH}")

def main():
    # Drop nulls..
    df_clean_nulls = df_raw.dropna().copy()
    # slicing/filtering/dropping/subsetting a DataFrame
    # ➜ a view of the original data in memory NOT a brand-new DF
    # ➜ changes on the view affect the original
    # ➜ .copy() creates a new independent DF
    # --------
    # df_clean_nulls = df_raw.fillna({"cost":"unknown"})        # give defined value to nulls
    print(f"✅ Nulls removed! Rows left: {len(df_clean_nulls)}")

    # Unify names..
    df_clean_names = df_clean_nulls
    # df_clean_names["city"] = df_clean_nulls["city"].str.lower()
    # df_clean_names["city"] = df_clean_nulls["city"].str.upper()
    # df_clean_names["city"] = df_clean_nulls["city"].str.capitalize()  # only 1st letter
    df_clean_names["city"] = (
        df_clean_names["city"]
            .str.title()         # 1st on all words
            .replace({"Denvre": "Denver", "nyc": "new york", "la": "los angeles"})      # Correct known typos
    )
    print(f"✅ City names standarized!")

    # Cost DataType..
    df_clean_dtype = df_clean_names
    df_clean_dtype["cost"] = (
        df_clean_dtype["cost"]
            .astype(str)
            .str.replace(r"[^\d.,-]", "", regex=True)    # remove all not digit/decimal
            .str.replace(",", ".", regex=False)          # normalize decimal commas
    )
    df_clean_dtype["cost"] = pd.to_numeric(df_clean_dtype["cost"], errors="coerce")     # convert to float + delete what's not
                            # .astype(str)+ ' €' # convert to string to add curency symbol
    print(f"✅ 'cost' Datatype set to: {df_clean_dtype["cost"].dtype}!")

    # Output Export..
    print(f"💾 Saving cleaned data to: {OUTPUT_PATH}...")
    df_clean = df_clean_dtype
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_clean.to_csv(OUTPUT_PATH, index=False)

    # print(df_clean)
    print("🎯 Export complete!")
    print("✅ All Done!!")

if __name__ == "__main__":
    main()