import requests
from bs4 import BeautifulSoup
import re
import spacy
import pandas as pd
from urllib.parse import urljoin, urlparse
import time
import logging
import streamlit as st
from io import BytesIO
from duckduckgo_search import DDGS
import json
import os

# Load spaCy model
nlp = spacy.load("en_core_web_sm")

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# India-specific regex
EMAIL_REGEX = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"
PHONE_REGEX = r"(?:(?:\+|00)?91[\s\-]?)?[6-9]\d{9}"
HEADERS = {'User-Agent': 'Mozilla/5.0'}

# File paths
VISITED_FILE = 'visited_links.json'
ALL_URLS_FILE = 'all_urls.json'

# Functions to load and save visited links
def load_json(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            return set(json.load(f))
    return set()

def save_json(data_set, file_path):
    with open(file_path, 'w') as f:
        json.dump(list(data_set), f)

visited_links = load_json(VISITED_FILE)
all_urls = load_json(ALL_URLS_FILE)

def search_urls(keyword, country, max_results=100):
    query = f"{keyword} {country}"
    urls = []
    with DDGS() as ddgs:
        for r in ddgs.text(query, max_results=max_results):
            url = r['href']
            domain = urlparse(url).netloc.lower()
            if domain.endswith('.in') or 'india' in domain:
                urls.insert(0, url)
            else:
                urls.append(url)
    return urls[:max_results]

def get_links(base_url, max_pages=10):
    links = set()
    try:
        response = requests.get(base_url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.content, "html.parser")
        domain = urlparse(base_url).netloc

        for a_tag in soup.find_all("a", href=True):
            href = urljoin(base_url, a_tag['href'])
            if domain in href:
                links.add(href)
                if len(links) >= max_pages:
                    break
    except Exception as e:
        logging.error(f"Error getting links from {base_url}: {e}")
    return list(links)

def extract_entities(text):
    doc = nlp(text)
    names = [ent.text.strip() for ent in doc.ents if ent.label_ == "PERSON"]
    orgs = [ent.text.strip() for ent in doc.ents if ent.label_ == "ORG"]
    return names, orgs

def scrape_page(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.content, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        emails = list(set(re.findall(EMAIL_REGEX, text)))
        emails = [email for email in emails if not email.startswith(('noreply', 'no-reply', 'donotreply'))]
        phones = list(set(re.findall(PHONE_REGEX, text)))
        names, orgs = extract_entities(text)
        return {
            "url": url,
            "names": names,
            "orgs": orgs,
            "emails": emails,
            "phones": phones
        }
    except Exception as e:
        logging.error(f"Error scraping {url}: {e}")
        return None

def crawl_and_scrape(url_batch, max_pages=10):
    global visited_links
    results = []
    for base_url in url_batch:
        st.info(f"Crawling: {base_url}")
        pages = get_links(base_url, max_pages=max_pages)
        pages.insert(0, base_url)
        for page in pages:
            if page in visited_links:
                st.info(f"  → Skipping already visited: {page}")
                continue
            st.info(f"  → Scraping: {page}")
            data = scrape_page(page)
            if data:
                visited_links.add(page)
                for i in range(max(len(data["names"]), 1)):
                    name = data["names"][i] if i < len(data["names"]) else None
                    company = data["orgs"][0] if data["orgs"] else None
                    results.append({
                        "Person Name": name,
                        "Designation": None,
                        "Company": company,
                        "Email(s)": ", ".join(data["emails"]) if data["emails"] else None,
                        "Phone(s)": ", ".join(data["phones"]) if data["phones"] else None,
                        "Source URL": data["url"]
                    })
            time.sleep(1)
    save_json(visited_links, VISITED_FILE)
    return results

def deduplicate(results):
    seen = set()
    unique = []
    for item in results:
        key = (item['Email(s)'], item['Phone(s)'])
        if key not in seen:
            seen.add(key)
            unique.append(item)
    return unique

def clean_excel_string(val):
    if isinstance(val, str):
        return re.sub(r"[\x00-\x1F\x7F-\x9F]", "", val)
    return val

def save_to_excel(data):
    df = pd.DataFrame(data)
    df.fillna("None", inplace=True)
    df = df.applymap(clean_excel_string)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def save_to_csv(data):
    df = pd.DataFrame(data)
    df.fillna("None", inplace=True)
    df = df.applymap(clean_excel_string)
    output = BytesIO()
    df.to_csv(output, index=False)
    return output.getvalue()

def main():
    global visited_links, all_urls
    st.set_page_config(page_title="India Contact Scraper", page_icon="🇮🇳", layout="wide")
    st.title("🇮🇳 India-Specific Contact Scraper")
    st.write("Extract contacts (Phone & Email) from Indian websites based on your keywords.")

    reset_button = st.button("🔄 Reset Visited Links & All URLs")
    if reset_button:
        visited_links = set()
        all_urls = set()
        save_json(visited_links, VISITED_FILE)
        save_json(all_urls, ALL_URLS_FILE)
        st.success("✅ Reset successful!")

    keyword = st.text_input("Enter keyword (e.g., cardiologist, college):")
    num_websites = st.number_input("Number of websites to crawl per run (2-20):", min_value=2, max_value=20, value=5, step=1)
    max_pages = st.number_input("Max pages per website to crawl:", min_value=5, max_value=30, value=10, step=1)
    crawl_button = st.button("Start Crawling")

    if crawl_button and keyword:
        with st.spinner("🔍 Crawling and scraping in progress..."):
            # If all_urls not set, search and save
            if not all_urls:
                urls = search_urls(keyword, "India", max_results=100)
                all_urls = set(urls)
                save_json(all_urls, ALL_URLS_FILE)

            # Determine remaining URLs to crawl
            remaining_urls = list(all_urls - visited_links)
            if not remaining_urls:
                st.warning("🚨 No new URLs to crawl. Reset to start over.")
                return

            # Get batch to crawl
            batch = remaining_urls[:num_websites]
            st.info(f"🔎 Crawling next {len(batch)} new URLs.")

            data = crawl_and_scrape(batch, max_pages=max_pages)
            if not data:
                st.error("No contacts found. Try a different keyword or increase limits.")
                return

            deduped = deduplicate(data)
            st.success(f"✅ Found {len(deduped)} unique contacts!")
            st.dataframe(pd.DataFrame(deduped), height=400)

            col1, col2 = st.columns(2)
            col1.download_button(
                label="Download as Excel",
                data=save_to_excel(deduped),
                file_name="India_contacts.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            col2.download_button(
                label="Download as CSV",
                data=save_to_csv(deduped),
                file_name="India_contacts.csv",
                mime="text/csv"
            )
    elif crawl_button:
        st.error("Please enter a keyword to search.")

if __name__ == "__main__":
    main()
