import pandas as pd


def load_data(file_path, encoding="utf-8"):
    """Carga los datos desde un archivo CSV."""
    df = pd.read_csv(file_path, encoding=encoding)
    return df

def basic_eda(df):
    """Realiza un análisis exploratorio de datos básico."""
    print("Primeras filas del dataset:")
    print(df.head())
    
    print("\nInformación del dataset:")
    print(df.info())
    
    print("\nEstadísticas descriptivas:")
    print(df.describe())
    
    print("\nValores nulos por columna:")
    print(df.isnull().sum())

    print("\nRepetidos por ORDERNUMBER:")
    print(df.duplicated(subset=["ORDERNUMBER"]).sum())
