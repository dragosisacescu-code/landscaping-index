"""
scraper.py — Scraping automat saptamanal de pe 16 surse romanesti.
Ruleaza luni 03:00 prin cron job Render.
Foloseste acelasi agent AI (parser.py) pentru indexare.
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

def scrape_planteo(source_id):
    """planteo.ro — categorii plante."""
    base = "https://www.planteo.ro"
    categories = [
        "/plante-ornamentale/conifere",
        "/plante-ornamentale/arbori",
        "/plante-ornamentale/arbusti",
        "/gazon",
    ]
    count = 0
    for cat in categories:
        soup = fetch(base + cat)
        if not soup:
            continue
        for card in soup.select('.product-card, .product-item, [class*="product"]')[:30]:
            name_el  = card.select_one('[class*="name"], [class*="title"], h2, h3')
            price_el = card.select_one('[class*="price"], .pret')
            link_el  = card.select_one('a[href]')
            if name_el and price_el:
                url = base + link_el['href'] if link_el else base + cat
                ok = process_scraped_item(
                    name_el.get_text(strip=True),
                    price_el.get_text(strip=True),
                    'Planteo', url
                )
                if ok:
                    count += 1
        time.sleep(DELAY)
    log.info(f"Planteo: {count} produse indexate")
    db.update_source_status(source_id, datetime.utcnow().isoformat())
    return count


def scrape_robakker(source_id):
    """robakker.ro — pepiniera online."""
    base = "https://www.robakker.ro"
    pages = [
        "/conifere", "/arbori-ornamentali",
        "/arbusti-ornamentali", "/plante-acoperitoare",
    ]
    count = 0
    for page in pages:
        soup = fetch(base + page)
        if not soup:
            continue
        for card in soup.select('.product, .plant-item, [class*="product"]')[:30]:
            name_el  = card.select_one('h2, h3, .name, [class*="title"]')
            price_el = card.select_one('.price, [class*="price"], .pret')
            link_el  = card.select_one('a[href]')
            if name_el and price_el:
                url = base + link_el['href'] if link_el else base + page
                ok = process_scraped_item(
                    name_el.get_text(strip=True),
                    price_el.get_text(strip=True),
                    'Robakker', url
                )
                if ok:
                    count += 1
        time.sleep(DELAY)
    log.info(f"Robakker: {count} produse indexate")
    db.update_source_status(source_id, datetime.utcnow().isoformat())
    return count


def scrape_verdena(source_id):
    """verdena.ro — plante online."""
    base = "https://www.verdena.ro"
    pages = ["/conifere", "/arbori", "/arbusti", "/gazon"]
    count = 0
    for page in pages:
        soup = fetch(base + page)
        if not soup:
            continue
        for card in soup.select('.product-miniature, .product, [class*="product"]')[:30]:
            name_el  = card.select_one('.product-title, h2, h3, .name')
            price_el = card.select_one('.price, [class*="price"]')
            link_el  = card.select_one('a[href]')
            if name_el and price_el:
                url = base + link_el['href'] if link_el else base + page
                ok = process_scraped_item(
                    name_el.get_text(strip=True),
                    price_el.get_text(strip=True),
                    'Verdena', url
                )
                if ok:
                    count += 1
        time.sleep(DELAY)
    log.info(f"Verdena: {count} produse indexate")
    db.update_source_status(source_id, datetime.utcnow().isoformat())
    return count


def scrape_sweetgarden(source_id):
    """sweetgarden.ro."""
    base = "https://www.sweetgarden.ro"
    soup = fetch(base + "/shop")
    if not soup:
        db.update_source_status(source_id, datetime.utcnow().isoformat(), "Blocat")
        return 0
    count = 0
    for card in soup.select('.product-item, .woocommerce-product, [class*="product"]')[:40]:
        name_el  = card.select_one('h2, h3, .woocommerce-loop-product__title')
        price_el = card.select_one('.price, .woocommerce-Price-amount')
        link_el  = card.select_one('a[href]')
        if name_el and price_el:
            url = link_el['href'] if link_el else base
            ok = process_scraped_item(
                name_el.get_text(strip=True),
                price_el.get_text(strip=True),
                'SweetGarden', url
            )
            if ok:
                count += 1
    log.info(f"SweetGarden: {count} produse indexate")
    db.update_source_status(source_id, datetime.utcnow().isoformat())
    return count


def scrape_olx(source_id):
    """OLX — cautari produse landscaping."""
    base  = "https://www.olx.ro"
    terms = ["conifere", "arbori ornamentali", "gazon rulou", "piatra decorativa"]
    count = 0
    for term in terms:
        url  = f"{base}/oferte/q-{term.replace(' ', '-')}/"
        soup = fetch(url)
        if not soup:
            time.sleep(DELAY)
            continue
        for card in soup.select('[data-cy="l-card"], .offer-wrapper, [class*="listing"]')[:20]:
            name_el  = card.select_one('h6, h4, [class*="title"]')
            price_el = card.select_one('[class*="price"], p[class*="price"]')
            link_el  = card.select_one('a[href]')
            if name_el and price_el:
                href = link_el['href'] if link_el else url
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


def scrape_emag(source_id):
    """eMAG — produse gradina."""
    base  = "https://www.emag.ro"
    pages = [
        "/conifere/c",
        "/arbori-ornamentali/c",
        "/gazon/c",
        "/piatra-decorativa/c",
    ]
    count = 0
    for page in pages:
        soup = fetch(base + page)
        if not soup:
            time.sleep(DELAY)
            continue
        for card in soup.select('.card-item, .product-card, [class*="product-card"]')[:20]:
            name_el  = card.select_one('.card-title, .product-title, h2')
            price_el = card.select_one('.product-new-price, [class*="price"]')
            link_el  = card.select_one('a[href]')
            if name_el and price_el:
                href = link_el['href'] if link_el else base + page
                full_url = href if href.startswith('http') else base + href
                ok = process_scraped_item(
                    name_el.get_text(strip=True),
                    price_el.get_text(strip=True),
                    'eMAG', full_url
                )
                if ok:
                    count += 1
        time.sleep(DELAY)
    log.info(f"eMAG: {count} produse indexate")
    db.update_source_status(source_id, datetime.utcnow().isoformat())
    return count


def scrape_generic(source_id, name, base_url, search_paths):
    """Scraper generic pentru surse cu structura HTML standard."""
    count = 0
    for path in search_paths:
        soup = fetch(base_url + path)
        if not soup:
            time.sleep(DELAY)
            continue
        # Incearca selectori comuni
        for selector in ['.product', '.item', '[class*="product"]', 'article']:
            cards = soup.select(selector)
            if cards:
                for card in cards[:25]:
                    name_el  = card.select_one('h2, h3, h4, .title, .name, [class*="title"]')
                    price_el = card.select_one('.price, [class*="price"], .pret, [class*="pret"]')
                    link_el  = card.select_one('a[href]')
                    if name_el and price_el:
                        href = link_el['href'] if link_el else base_url + path
                        full_url = href if href.startswith('http') else base_url + href
                        ok = process_scraped_item(
                            name_el.get_text(strip=True),
                            price_el.get_text(strip=True),
                            name, full_url
                        )
                        if ok:
                            count += 1
                break
        time.sleep(DELAY)
    log.info(f"{name}: {count} produse indexate")
    db.update_source_status(source_id, datetime.utcnow().isoformat())
    return count


# ─── ORCHESTRATOR ─────────────────────────────────────────────────────────────

SCRAPERS = {
    'Planteo':         scrape_planteo,
    'Robakker':        scrape_robakker,
    'Verdena':         scrape_verdena,
    'SweetGarden':     scrape_sweetgarden,
    'OLX':             scrape_olx,
    'eMAG':            scrape_emag,
    # Generic scrapers
    'Gradina Max':     lambda sid: scrape_generic(sid, 'Gradina Max',     'https://www.gradinamax.ro',      ['/plante', '/conifere', '/arbori']),
    'Garden Services': lambda sid: scrape_generic(sid, 'Garden Services', 'https://www.gardenservices.ro',  ['/shop', '/produse']),
    'Parcuri':         lambda sid: scrape_generic(sid, 'Parcuri',         'https://www.parcuri.ro',          ['/plante', '/produse']),
    'Sieberz':         lambda sid: scrape_generic(sid, 'Sieberz',         'https://www.sieberz.ro',          ['/plante', '/arbori']),
    'Yurta':           lambda sid: scrape_generic(sid, 'Yurta',           'https://www.yurta.ro',            ['/plante', '/shop']),
    'Pepiniera Mizil': lambda sid: scrape_generic(sid, 'Pepiniera Mizil', 'https://www.pepinieram.ro',       ['/produse', '/plante']),
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
            # Retaileri mari — de obicei blocati, sarim silentios
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
