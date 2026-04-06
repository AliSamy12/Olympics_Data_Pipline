import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import re
import os

def clean_td(text):
    text = text.strip()
    if re.fullmatch(r"\d{1,3}(,\d{3})+", text):
        text = text.replace(",", "")
    return text

def clean_text(text):
    text = text.strip().lower()
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", "_", text)
    return text

def get_th(table):
    rows = table.find_all('tr')
    first_row = rows[0].find_all('th')
    second_row = rows[1].find_all('th')
    headers = []
    i = 0
    for th in first_row:
        colspan = int(th.get('colspan', 1))
        text = th.get_text(" ", strip=True)
        rowspan = int(th.get('rowspan', 1))
        if colspan > 1:
            for n in range(colspan):
                if rowspan > 1:
                    headers.append(f"{text}")
                else:
                    if i < len(second_row):
                        s_th = second_row[i]
                        i += 1
                        s_text = s_th.get_text(" ", strip=True)
                        if not s_text:
                            img = s_th.find("img")
                            if img and img.get("alt"):
                                s_text = img["alt"]
                        headers.append(clean_text(s_text))
        else:
            headers.append(clean_text(text))
    return headers


def scrapper():
    url = "https://en.wikipedia.org/wiki/Lists_of_Olympic_medalists"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    response = requests.get(url, headers=headers)
    soup = bs(response.content, 'html.parser')
    output_dir = "/usr/local/airflow/dbt/seeds"
    os.makedirs(output_dir, exist_ok=True)

    Lists_of_Olympic_medalists = {}
    sports = soup.find_all("h3")
    tables = soup.find_all("table")

    for l in range(len(sports)):
        sport = sports[l].get_text(strip=True)
        Lists_of_Olympic_medalists[sport] = {
            "Header": [],
            "data": []
        }
        table = tables[l + 1]
        headers = get_th(table)

        # Deduplicate column names
        seen = {}
        clean_headers = []
        for h in headers:
            if h in seen:
                seen[h] += 1
                clean_headers.append(f"{h}_{seen[h]}")
            else:
                seen[h] = 0
                clean_headers.append(h)
        headers = clean_headers

        Lists_of_Olympic_medalists[sport]["Header"] = headers

        rows = table.find_all('tr')
        data = []
        for row in rows:
            cols = row.find_all('td')
            row_data = [clean_td(col.get_text(" ", strip=True)) for col in cols]
            if row_data:
                data.append(row_data)
        Lists_of_Olympic_medalists[sport]["data"] = data

        df = pd.DataFrame(data, columns=headers)
        file_name = sport.replace("/", "_").replace("\\", "_").replace(":", "_").replace(" ", "_") + ".csv"
        df.to_csv(os.path.join(output_dir, file_name), index=False)
        print(f"Saved: {file_name}")

    print("Scraping complete!")