import os
from dotenv import load_dotenv

load_dotenv()

os.environ["KAGGLE_USERNAME"] = os.getenv("KAGGLE_USERNAME")
os.environ["KAGGLE_KEY"] = os.getenv("KAGGLE_KEY")

import kaggle

def download_sales_data(dest_folder="data/"):
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(
        "kyanyoga/sample-sales-data",
        path=dest_folder,
        unzip=True
    )
    print(f"CSV descargado en {dest_folder}")