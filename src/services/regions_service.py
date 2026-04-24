import httpx
import pandas as pd
import os

class RegionsService:
  def __init__(self):
    self.url = "https://restcountries.com/v3.1/all?fields=name,region,subregion,latlng"
    self.csv_path = "data/regions_master.csv"
    os.makedirs("data", exist_ok=True)

  def _exists(self):
    """Check if the file already contains data."""
    if not os.path.exists(self.csv_path):
      return False
    try:
      df = pd.read_csv(self.csv_path)
      return len(df) > 0
    except Exception:
      return False

  async def update_regions_dataset(self, force=False):
    """
    Download regional information.
    If 'force' is False, it respects the existing file.
    """
    if not force and self._exists():
      print(f"Using existing Regions Master in {self.csv_path}")
      return True

    print("Downloading regional data from the API...")
    try:
      async with httpx.AsyncClient() as client:
        response = await client.get(self.url, timeout=20.0, follow_redirects=True)

        if response.status_code == 200:
          countries_data = response.json()
          rows = []

          for country in countries_data:
            name_data = country.get('name', {})
            common_name = name_data.get('common', 'UNKNOWN')
            latlng = country.get('latlng', [None, None])

            rows.append({
              "country_name": common_name.upper(),
              "continent": country.get('region', 'N/A').upper(),
              "sub_region": country.get('subregion', 'N/A').upper(),
              "latitude": latlng[0] if len(latlng) > 0 else None,
              "longitude": latlng[1] if len(latlng) > 1 else None
            })

          df = pd.DataFrame(rows)
          df.to_csv(self.csv_path, index=False, encoding='utf-8')
          print(f"Updated Master of Regions: {len(df)} countries loaded.")
          return True
        else:
          print(f"Regions API Error: Code {response.status_code}")
          return False
    except Exception as e:
      print(f"Error getting regions: {e}")
      return False

regions_service = RegionsService()