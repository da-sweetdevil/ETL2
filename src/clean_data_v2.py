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
    # Use same variable → built-up modifications → avoid confusion & ease debugging
    # Drop nulls..
    df_clean = df_raw.dropna().copy()
    print(f"✅ Nulls removed! Rows left: {len(df_clean)}")

    # Unify names..
    df_clean["city"] = (
        df_clean["city"]
            .str.title()         # 1st on all words
            .replace({"Denvre": "Denver", "nyc": "new york", "la": "los angeles"})      # Correct known typos
    )
    print(f"✅ City names standarized!")

    # Cost DataType..
    df_clean["cost"] = (
        df_clean["cost"]
            .astype(str)
            .str.replace(r"[^\d.,-]", "", regex=True)    # remove all not digit/decimal
            .str.replace(",", ".", regex=False)          # normalize decimal commas
    )
    df_clean["cost"] = pd.to_numeric(df_clean["cost"], errors="coerce")     # convert to float + delete what's not
                            # .astype(str)+ ' €' # convert to string to add curency symbol
    print(f"✅ 'cost' Datatype set to: {df_clean["cost"].dtype}!")

    # Output Export..
    print(f"💾 Saving cleaned data to: {OUTPUT_PATH}...")
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_clean.to_csv(OUTPUT_PATH, index=False)

    # print(df_clean)
    print("🎯 Export complete!")
    print("✅ All Done!!")

    # Reviews to confirm the transformation
    print("\n📌 Preview of cleaned data:")      # \n = newline (space)
    print(df_clean.head())
    print("\n📌 Columns DataTypes:")
    print(df_clean.dtypes)

if __name__ == "__main__":      # Only execute program if this file is run directly by Python
    main()