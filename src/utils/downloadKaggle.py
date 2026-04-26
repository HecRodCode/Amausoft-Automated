import os
import glob
import kaggle

def downloadKaggle(dest_folder="/opt/airflow/data/"):
    os.makedirs(dest_folder, exist_ok=True)
    target_file = os.path.join(dest_folder, "sales_data_sample.csv")

    if os.path.exists(target_file):
        print(f"[Server] The File already exists {dest_folder}. It won't download again.")
        return

    try:
        print(f"[Server] Downloading sales data from Kaggle...", flush=True)
        kaggle.api.authenticate()

        # Download the dataset
        kaggle.api.dataset_download_files(
            "kyanyoga/sample-sales-data",
            path=dest_folder,
            unzip=True
        )

        csv_files = glob.glob(os.path.join(dest_folder, "*.csv"))
        for f in csv_files:
            if "sales" in f.lower() and f != target_file:
                os.rename(f, target_file)
                print(f"[Server] File renamed to {target_file}")

        print(f"[Server] Sales data successfully processed.")

    except Exception as e:
        print(f"[Server] Error downloading from Kaggle: {e}", flush=True)