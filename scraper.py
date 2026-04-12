"""
scraper.py — Scraping automat saptamanal de pe surse romanesti.
Ruleaza luni 03:00 prin cron job Render.
Foloseste acelasi agent AI (parser.py) pentru indexare.

Surse active:
  - Verdena        (Shopify JSON API — stabil)
  - SweetGarden    (BeautifulSoup)
  - OLX            (BeautifulSoup)
"""

import re
import time
import logging
from datetime import datetime

import requests
from bs4 import BeautifulSoup

import db
from parser import parse_item, deduct_vat

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'ro-RO,ro;q=0.9,en;q=0.8',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}
TIMEOUT  = 15
DELAY    = 2.0   # secunde intre requesturi


# ─── UTILITARE ────────────────────────────────────────────────────────────────

def fetch(url):
    """Fetch URL cu retry. Returneaza BeautifulSoup sau None."""
    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200:
                return BeautifulSoup(r.text, 'html.parser')
            if r.status_code in (403, 429, 503):
                log.warning(f"Blocat ({r.status_code}) la {url}")
                return None
            time.sleep(DELAY * (attempt + 1))
        except Exception as e:
            log.warning(f"Eroare fetch {url}: {e}")
            time.sleep(DELAY)
    return None


def fetch_json(url):
    """Fetch JSON URL cu retry. Returneaza dict/list sau None."""
    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (403, 429, 503):
                log.warning(f"Blocat ({r.status_code}) la {url}")
                return None
            time.sleep(DELAY * (attempt + 1))
        except Exception as e:
            log.warning(f"Eroare fetch_json {url}: {e}")
            time.sleep(DELAY)
    return None


def extract_price_ron(text):
    """Extrage valoare numerica RON din text (ex: '245,00 Lei' → 245.0)."""
    if not text:
        return None
    cleaned = text.replace('\xa0', ' ').replace(',', '.').strip()
    m = re.search(r'(\d{1,6}(?:\.\d{1,2})?)', cleaned)
    if m:
        val = float(m.group(1))
        # Sanity check: preturi rezonabile pentru landscaping
        if 1.0 <= val <= 50000.0:
            return val
    return None


def process_scraped_item(name, price_raw, source_name, source_url, vat_included=True):
    """
    Parseaza numele produsului cu agentul AI, deduce TVA,
    si salveaza in baza de date.
    """
    if not name or not price_raw:
        return False

    price = extract_price_ron(str(price_raw))
    if not price:
        return False

    keys, err = parse_item(name)
    if err or not keys:
        log.debug(f"Nu am putut parsa '{name}': {err}")
        return False

    # Deduce TVA
    price_net = deduct_vat(price, keys['category'], vat_included=vat_included)

    item_id, _ = db.get_or_create_item(keys)
    db.add_online_price(
        item_id    = item_id,
        price_min  = price_net,
        price_max  = price_net,
        price_avg  = price_net,
        source_name= source_name,
        source_url = source_url,
    )
    return True


# ─── SCRAPERE PE SURSE ────────────────────────────────────────────────────────

def scrape_verdena(source_id):
    """
    verdena.ro — magazin Shopify.
    Folosim JSON API: /collections/{slug}/products.json?limit=250
    Returneaza date structurate, fara scraping HTML.
    """
    base        = "https://verdena.ro"
    collections = [
        "conifere",
        "arbori-ornamentali",
        "arbusti-ornamentali",
        "plante-ornamentale",
        "gazon",
        "plante-acoperitoare-sol",
    ]
    count = 0
    for col in collections:
        url  = f"{base}/collections/{col}/products.json?limit=250"
        data = fetch_json(url)
        if not data or 'products' not in data:
            log.debug(f"Verdena/{col}: raspuns gol sau eroare")
            time.sleep(DELAY)
            continue
        for prod in data['products']:
            title    = prod.get('title', '')
            handle   = prod.get('handle', '')
            variants = prod.get('variants', [])
            if not title or not variants:
                continue
            price_str = variants[0].get('price', '')
            product_url = f"{base}/products/{handle}"
            ok = process_scraped_item(title, price_str, 'Verdena', product_url)
            if ok:
                count += 1
        time.sleep(DELAY)
    log.info(f"Verdena: {count} produse indexate")
    db.update_source_status(source_id, datetime.utcnow().isoformat())
    return count


def scrape_sweetgarden(source_id):
    """
    sweetgarden.ro — magazin plante ornamentale.
    URL: /plante-ornamentale (categoria principala)
    Container: .product | Nume: h3 | Pret: .cartPrice
    """
    base  = "https://www.sweetgarden.ro"
    pages = [
        "/plante-ornamentale",
        "/conifere",
        "/arbori-ornamentali",
        "/arbusti-ornamentali",
        "/gazon",
    ]
    count = 0
    for page in pages:
        soup = fetch(base + page)
        if not soup:
            time.sleep(DELAY)
            continue
        cards = soup.select('.product')
        log.debug(f"SweetGarden{page}: {len(cards)} carduri gasite")
        for card in cards[:50]:
            name_el  = card.select_one('h3')
            # Incearca .cartPrice, apoi .priceBox, apoi orice element cu 'lei'
            price_el = card.select_one('.cartPrice, .priceBox, [class*="price"], [class*="Price"]')
            link_el  = card.select_one('a[href]')
            if name_el and price_el:
                href     = link_el['href'] if link_el else page
                full_url = href if href.startswith('http') else base + href
                ok = process_scraped_item(
                    name_el.get_text(strip=True),
                    price_el.get_text(strip=True),
                    'SweetGarden', full_url
                )
                if ok:
                    count += 1
        time.sleep(DELAY)
    log.info(f"SweetGarden: {count} produse indexate")
    db.update_source_status(source_id, datetime.utcnow().isoformat())
    return count


def scrape_olx(source_id):
    """
    OLX — cautari produse landscaping.
    Selectori confirmati: [data-cy="l-card"] / [data-testid="ad-title"] / [data-testid="ad-price"]
    """
    base  = "https://www.olx.ro"
    terms = [
        "conifere",
        "arbori ornamentali",
        "gazon rulou",
        "piatra decorativa",
        "arbusti ornamentali",
        "tuia",
        "brad ornamental",
    ]
    count = 0
    for term in terms:
        url  = f"{base}/oferte/q-{term.replace(' ', '-')}/"
        soup = fetch(url)
        if not soup:
            time.sleep(DELAY)
            continue
        cards = soup.select('[data-cy="l-card"]')
        log.debug(f"OLX '{term}': {len(cards)} carduri gasite")
        for card in cards[:20]:
            name_el  = card.select_one('[data-testid="ad-title"]')
            price_el = card.select_one('[data-testid="ad-price"]')
            link_el  = card.select_one('a[href]')
            # Fallback la selectori vechi daca lipsesc
            if not name_el:
                name_el = card.select_one('h6, h4, [class*="title"]')
            if not price_el:
                price_el = card.select_one('[class*="price"], p')
            if name_el and price_el:
                href     = link_el['href'] if link_el else url
                full_url = href if href.startswith('http') else base + href
                ok = process_scraped_item(
                    name_el.get_text(strip=True),
                    price_el.get_text(strip=True),
                    'OLX', full_url,
                    vat_included=False  # OLX = pret fara TVA de obicei
                )
                if ok:
                    count += 1
        time.sleep(DELAY)
    log.info(f"OLX: {count} produse indexate")
    db.update_source_status(source_id, datetime.utcnow().isoformat())
    return count


def scrape_planteo(source_id):
    """
    planteo.ro — magazin de plante (Shopify).
    Folosim JSON API similar cu Verdena.
    """
    base        = "https://www.planteo.ro"
    collections = [
        "conifere",
        "arbori",
        "arbusti",
        "gazon",
    ]
    count = 0
    for col in collections:
        url  = f"{base}/collections/{col}/products.json?limit=250"
        data = fetch_json(url)
        if not data or 'products' not in data:
            # Fallback HTML daca JSON nu merge
            soup = fetch(f"{base}/collections/{col}")
            if not soup:
                time.sleep(DELAY)
                continue
            for card in soup.select('.product-item, [class*="product-card"], .grid__item')[:30]:
                name_el  = card.select_one('h2, h3, .product-item__title, [class*="title"]')
                price_el = card.select_one('.price, [class*="price"]')
                link_el  = card.select_one('a[href]')
                if name_el and price_el:
                    href     = link_el['href'] if link_el else f"/collections/{col}"
                    full_url = href if href.startswith('http') else base + href
                    ok = process_scraped_item(
                        name_el.get_text(strip=True),
                        price_el.get_text(strip=True),
                        'Planteo', full_url
                    )
                    if ok:
                        count += 1
            time.sleep(DELAY)
            continue
        for prod in data['products']:
            title    = prod.get('title', '')
            handle   = prod.get('handle', '')
            variants = prod.get('variants', [])
            if not title or not variants:
                continue
            price_str   = variants[0].get('price', '')
            product_url = f"{base}/products/{handle}"
            ok = process_scraped_item(title, price_str, 'Planteo', product_url)
            if ok:
                count += 1
        time.sleep(DELAY)
    log.info(f"Planteo: {count} produse indexate")
    db.update_source_status(source_id, datetime.utcnow().isoformat())
    return count


# ─── ORCHESTRATOR ─────────────────────────────────────────────────────────────

SCRAPERS = {
    'Verdena':     scrape_verdena,
    'SweetGarden': scrape_sweetgarden,
    'OLX':         scrape_olx,
    'Planteo':     scrape_planteo,
}


def run_all_scrapers():
    """
    Ruleaza toti scraperele active.
    Apelat automat luni 03:00 sau manual din admin.
    """
    log.info("=== Incepe scraping saptamanal ===")
    sources = db.get_scraping_sources()
    total   = 0

    for src in sources:
        if not src['active']:
            continue
        name   = src['name']
        src_id = src['id']

        scraper_fn = SCRAPERS.get(name)
        if not scraper_fn:
            log.info(f"Sarer {name} (nu are scraper implementat sau e blocat)")
            db.update_source_status(src_id, datetime.utcnow().isoformat(), "Fara scraper")
            continue

        try:
            n = scraper_fn(src_id)
            total += n
        except Exception as e:
            log.error(f"Eroare la {name}: {e}")
            db.update_source_status(src_id, datetime.utcnow().isoformat(), str(e)[:200])

    log.info(f"=== Scraping finalizat. Total produse: {total} ===")
    return total


if __name__ == '__main__':
    db.init_db()
    run_all_scrapers()
