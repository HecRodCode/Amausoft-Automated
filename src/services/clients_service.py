import asyncio
import httpx
import pandas as pd
import os
from datetime import datetime

class ClientsService:
  def __init__(self):
    self.external_data = {'last_value': None, 'last_update': None}
    self.url = 'https://randomuser.me/api/?results=100'
    self.csv_path = 'data/clients_dataset.csv'
    self.limit = 3000

  def _get_current_count(self):
    """ Returns the current number of clients """
    if not os.path.exists(self.csv_path):
      return 0

    try:
      df = pd.read_csv(self.csv_path, usecols=['client_id'])
      return len(df)
    except FileNotFoundError:
      return 0

  def _save_to_csv(self, json_data):
    """ Save the current clients data to csv """
    try:
      users = json_data.get('results', [])
      if not users: return

      current_id = self._get_current_count()
      current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

      rows = [{
        'client_id': current_id + i,
        'name': f"{u['name']['first']} {u['name']['last']}".upper(),
        "city": u['location']['city'].upper(),
        "country": u['location']['country'].upper(),
        "email": u['email'],
        "fetch_timestamp": current_datetime
      } for i, u in enumerate(users)]

      df_new = pd.DataFrame(rows)
      file_exists = os.path.isfile(self.csv_path)

      df_new.to_csv(
        self.csv_path,
        mode='a',
        index=False,
        header=not file_exists,
        encoding='utf-8'
      )

      print(f"{len(rows)} Saved clients. Current total: {self._get_current_count()}")
    except Exception as e:
      print(f"Error CSV processing: {e}")

  async def fetch_clients_periodically(self):
    print("Client Ingestion Service Started.", flush=True)
    async with httpx.AsyncClient() as client:
      while True:
        try:
          current_total = await asyncio.to_thread(self._get_current_count)

          # Limit check
          if current_total >= self.limit:
            print(f"Limit the {self.limit} reached. Ingestion stopped.")
            break

          remaining = self.limit - current_total
          fetch_n = min(100, remaining)

          response = await client.get(self.url, params={"results": fetch_n}, timeout=15.0)

          if response.status_code == 200:
            data = response.json()

            await asyncio.to_thread(self._save_to_csv, data)
          else:
            print(f'API Error: {response.status_code}')
        except Exception as e:
          print(f"Connection error: {e}")

      await asyncio.sleep(20)

clients_service = ClientsService()