import polars as pl
import os
from tqdm import tqdm
import time
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

def extract_and_load_profeco(input_glob):
    print("🛒 Starting mass extraction and cleaning with Polars...")

    try:
        lf = pl.scan_csv(
            input_glob, 
            ignore_errors=True,
            schema_overrides={"precio": pl.String} 
        ) 
    except Exception as e:
        print(f"❌ Error leyendo archivos: {e}")
        return

    canasta_basica = 'aceite|arroz|azucar|frijol|huevo|leche|pan|tortilla|pollo|atun'

    """
    lf.select: It is used to select, transform or create new columns from a DataFrame. 
    Unlike Pandas, where you sometimes select data with df['column'], 
    in Polars you use .select() to define which data you want 
    to "pass" to the next stage of your analysis.

    pl.all() is for select all columns
    .name is for get the name of the columns
    .to_lowercase() is for convert the name of each column to lowercase

    .rename() is for rename the columns. 
    lambda col: col.strip().replace(' ', '_') is for remove the spaces and replace with underscores. 
    
    .filter() is for filter the rows of the DataFrame bases on a specific condition. 
    pl.col('producto') is for select the column named 'producto'
    .str.to_lowercase() is for convert the values of the column 'producto' to lowercase
    canasta_basica is a variable that contains a regular expression to match the products. 
        Each product is separated by a pipe (|) which means "or" in REGEX. 
        So, the filter will keep only the rows where the 'producto' column contains any of the 
        products listed in canasta_basica, regardless of case.
    .str.contains() is for check if the string in the 'producto' column contains any of the products
        listed in canasta_basica. It returns a boolean value (True or False) for each row, 
        and the .filter() method uses this to keep only the rows where the condition is True.
    .with_columns() is for create new columns or modify existing ones. 
    pl.col('precio') is for select the column named 'precio', an its the objective to convert it. 
        .str.replace_all(r'\$', '') is for remove the dollar signs from the 'precio' column. 
        .str.replace_all(',', '') is for remove the commas from the 'precio' column.
    .cast(pl.Float64, strict=False) is for convert the values of the 'precio' column to a 
        floating-point number (Float64). The strict=False argument allows the conversion to proceed 
        even if some values cannot be converted, which means that any non-convertible values will 
        be set to null instead of raising an error.
    .drop_nulls(subset=['precio']) is for remove any rows where the 'precio' column has null values. 
    .filter(pl.col('precio') <= 1000.0) is for filter the rows where the 'precio' column has values 
        less than or equal to 1000.0.
    """

    lf_clean = (
        lf.select(pl.all().name.to_lowercase()) 
          .rename(lambda col: col.strip().replace(' ', '_')) 
          .filter(
              pl.col('producto').str.to_lowercase().str.contains(canasta_basica)
          )
          .with_columns([
              pl.col('precio')
              .str.replace_all(r'\$', '')
              .str.replace_all(',', '')
              .cast(pl.Float64, strict=False)
          ])
          .drop_nulls(subset=['precio'])
          .filter(pl.col('precio') <= 1000.0)
    )

    '''
    with ... as pbar: is for create a progress bar using the tqdm library. 
        total=100 is for set the total value of the progress bar to 100, which means that the progress will be measured in percentage
        desc="Clean and Loading Data" is for set the description of the progress bar to "Clean and Loading Data"
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}" is for set the format of the progress bar, where:
        {l_bar} is for the left part of the bar, which includes the description and the percentage 
        {bar} is for the actual progress bar that fills up as the process advances
        {n_fmt} is for the current value of the progress (e.g., 50) 
        {total_fmt} is for the total value of the progress (e.g., 100)

    pl.collec() is for execute the lazy operations defined in lf_clean and collect the results into a DataFrame
    pbar.update(50) is for update the progress bar by 50 percentage points after the data cleaning step is completed
    os.environ.get("DATABASE_URL") is for get the value of the environment variable "DATABASE_URL", 
        which should contain the connection string for the database
    df.write_database() is for write the DataFrame to a database. The parameters are:
        table_name="raw_precios" is for specify the name of the table where the data will be stored
        connection=db_url is for specify the database connection string
        if_table_exists="replace" is for specify that if the table already exists, it should be replaced with the new data
    pbar.update(50) is for update the progress bar by another 50 percentage points after the data loading step is completed,
        which means that the entire process of cleaning and loading data is now complete.
    '''

    with tqdm(total=100, desc="Clean and Loading Data", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}") as pbar:
        df = lf_clean.collect()
        pbar.update(50) 
        
        db_url = os.environ.get("DATABASE_URL")
        if db_url:
            uniques_keys = ["producto", "cadena_comercial", "giro", "marca"]

        '''
        query = f"SELECT {', '.join(uniques_keys)} FROM raw_precios":
        Dynamically constructs a SQL SELECT statement. It uses the .join() method to convert the 
        list of primary keys into a comma-separated string. This technique is known as a 
        "Thin Extract," as it only requests the specific columns needed for comparison, optimizing 
        both network bandwidth and RAM usage.

        df_exist = pl.read_database(query=query, connection=db_url):
        Executes the query within the database engine and materializes the result into a Polars 
        DataFrame. This object captures the "current state" of records processed in previous runs, 
        serving as the ground-truth reference for validation.

        df_insert = df.join(df_exist, on=uniques_keys, how="anti"):
        Performs a relational algebra operation known as an Anti-Join. The Polars engine compares 
        the incoming data (df) against the existing records (df_exist) based on the specified keys. 
        The operation retains only the rows from the original set that do not have a match in the 
        database, effectively identifying truly new records.
        '''
        try: 
            query = f"SELECT {', '.join(uniques_keys)} FROM raw_precios"
            df_exist = pl.read_database_uri(query=query, uri=db_url)
            df_insert = df.join(df_exist, on=uniques_keys, how="anti")
            print(f"✅ Found {len(df_insert)} new records to insert.")
        except Exception as e:
            print(f"❌ Error reading existing data: {e}. The correct data will be inserted")
            df_insert = df
        if len(df_insert) > 0:
            df_insert.write_database(
            table_name="raw_precios",
            connection=db_url,
            if_table_exists="append" 
                )
        pbar.update(50) 

if __name__ == "__main__":
    CURRENT_DIR = Path(__file__).resolve().parent
    DATA_PATH = CURRENT_DIR.parent / "data" / "raw" / "Months" / "*.csv"
    extract_and_load_profeco(str(DATA_PATH))