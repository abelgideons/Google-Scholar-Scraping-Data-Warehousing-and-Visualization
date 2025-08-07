import os
import re
import time
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ## Service Account GCP

# ==== CONFIG ====
PROFILE_DIR = os.path.abspath("chrome_profile")
MAX_PAGES = None
WAIT_TIME = 5
DATASET_ID = "bronze_univ"

UNIV_URLS = [
    # Binus
    "https://scholar.google.com/citations?view_op=view_org&hl=en&org=7118194464740520364",
    # Dian Nuswantoro University
    "https://scholar.google.com/citations?view_op=view_org&hl=en&org=14236766997535578463",
    # Telkom Univ
    "https://scholar.google.com/citations?view_op=view_org&hl=en&org=6231097114314027752",
    # Tarumanegara University
    "https://scholar.google.com/citations?view_op=view_org&hl=en&org=9444447549188154848",
    # Additional University for another reference:
    # UPH
    "https://scholar.google.com/citations?view_op=view_org&hl=en&org=13340626219181466029"
]

bq_client = bigquery.Client()

# ==== DRIVER ====
def create_driver():
    options = Options()
    # options.add_argument(f"--user-data-dir={PROFILE_DIR}")

    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--remote-debugging-port=0")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--start-maximized")
    options.add_argument("--headless=new")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/114.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=options)


# ==== PARSE LIST AUTHOR ====
def parse_authors(html, university):
    soup = BeautifulSoup(html, "html.parser")
    authors = []

    for container in soup.select("div.gs_ai_chpr"):
        name_tag = container.select_one("h3.gs_ai_name a")
        name = name_tag.get_text(strip=True) if name_tag else "N/A"
        profile_url = "https://scholar.google.com" + name_tag["href"] if name_tag else "N/A"

        cited_tag = container.select_one("div.gs_ai_cby")
        citations = int(re.search(r"(\d+)", cited_tag.get_text()).group(1)) if cited_tag else 0

        interests = [i.get_text(strip=True) for i in container.select("div.gs_ai_int a")]

        authors.append({
            "university": university,
            "name": name,
            "profile_url": profile_url,
            "cited_by": citations,
            "research_interests": ", ".join(interests)
        })

    return authors

# ==== PARSE METRICS DOSEN ====
def parse_profile_metrics(driver):
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "gsc_rsb_st")))
        soup = BeautifulSoup(driver.page_source, "html.parser")

        def get_metric(label):
            cell = soup.find("td", string=label)
            if cell:
                all_val = cell.find_next_sibling("td").get_text(strip=True)
                since_val = cell.find_next_sibling("td").find_next_sibling("td").get_text(strip=True)
                return all_val, since_val
            return "0", "0"

        citations_all, citations_since = get_metric("Citations")
        h_all, h_since = get_metric("h-index")
        i10_all, i10_since = get_metric("i10-index")

        return {
            "citations_all": int(citations_all),
            "citations_since_2020": int(citations_since),
            "h_index_all": int(h_all),
            "h_index_since_2020": int(h_since),
            "i10_index_all": int(i10_all),
            "i10_index_since_2020": int(i10_since)
        }
    except:
        return {k: 0 for k in [
            "citations_all", "citations_since_2020",
            "h_index_all", "h_index_since_2020",
            "i10_index_all", "i10_index_since_2020"
        ]}

# ==== NORMALIZE NAMA TABLE ====
def normalize_table_name(univ):
    return "bronze_" + re.sub(r'[^a-zA-Z0-9]+', '_', univ.lower()).strip('_')

# ==== SCRAPE SATU UNIVERSITAS ====
def scrape_university(url):
    driver = create_driver()
    driver.get(url)
    print(f"Scraping {url}")

    all_authors = []
    page = 1
    university = None

    while True:
        time.sleep(WAIT_TIME)
        html = driver.page_source

        # Ambil nama universitas di halaman pertama
        if page == 1:
            soup = BeautifulSoup(html, "html.parser")
            uni_tag = soup.select_one("h2.gsc_authors_header")
            university = uni_tag.find(string=True, recursive=False).strip() if uni_tag else "N/A"
            print(f"University: {university}")

        authors = parse_authors(html, university)
        print(f"Halaman {page}: {len(authors)} author")
        all_authors.extend(authors)

        #stop kalau ada limit MAX_PAGES
        if MAX_PAGES is not None and page >= MAX_PAGES:
            print(f"Stop di halaman {page} (MAX_PAGES limit).")
            break

        # Klik tombol next kalau ada
        try:
            next_button = driver.find_element(By.CSS_SELECTOR, 'button.gs_btnPR')
            if not next_button.is_enabled():
                print("Halaman terakhir, stop scraping.")
                break
            ActionChains(driver).move_to_element(next_button).click().perform()
            page += 1
        except:
            print("Tombol next tidak ditemukan, stop scraping.")
            break

    # ==== AMBIL METRICS PER DOSEN ====
    enriched = []
    for author in all_authors:
        try:
            driver.get(author["profile_url"])
            time.sleep(2)
            metrics = parse_profile_metrics(driver)
            author.update(metrics)
            enriched.append(author)
            print(f"Metrics {author['name']} OK")
        except Exception as e:
            print(f"Gagal ambil metrics {author['name']}: {e}")
            enriched.append(author)

    driver.quit()
    df = pd.DataFrame(enriched)
    return df


# ==== MAIN ====
def main():
    for url in UNIV_URLS:
        scrape_university(url)

if __name__ == "__main__":
    main()
