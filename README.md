## ETL2 - Data Cleaning, Logging (Debugging), CLI Args Parsing ##
> Python script that loads data from EXCEL, rectifies raw data and creates CSV with clean data, all logging into file changed daily..

### "clean_data.py" Script that ->
   - Requires Input & Output files as syntax arguments
   - Reads an EXCEL file with delivery data (order_ID, client, city, cost) containing raw data
   - Transforms DataFrame:
      - Unify city names
      - Remove nulls
      - Re-export
      - Log process
   - Creates a new clean DataFrame
   - Exports to specified Output

   ## How to Run
   python src/clean_data.py --input data/orders_cities.xlsx --output output/orders_cities_clean.csv

   ## Output
   output/orders_cities_clean.csv

   ## Logs
   logs/etl_orders_cities_{DATE}.log

   ## Requirements
   - pathlib
   - time
   - datetime
   - argparse
   - logging
   - pandas
   - openpyxl


### Push to GitHub ->
   - Commit script + data + logs + README file + .gitignore file
   - Push to a new repository "ETL2"


### ====================== ###
|      Made By @ Y M E N     |
### ====================== ###