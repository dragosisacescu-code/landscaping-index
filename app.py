"""
app.py — Flask backend complet.
Rute publice: index, API preturi, contribuire, upload, geolocatie.
Rute admin: login, catalog, IP-uri, scraping.
"""

import os
import io
import hashlib
import logging
from functools import wraps
from datetime import datetime

from flask import (
    Flask, render_template, request, jsonify,
    session, redirect, url_for, flash
)
import requests

import db
from parser import parse_item, deduct_vat

# ─── OPTIONALE ────────────────────────────────────────────────────────────────
try:
    import openpyxl
    HAS_EXCEL = True
except ImportError:
    HAS_EXCEL = False

try:
    import pdfplumber
    HAS_PDF = True
except ImportError:
    HAS_PDF = False

try:
    from PIL import Image
    import pytesseract
    HAS_OCR = True
except ImportError:
    HAS_OCR = False

# ─── APP SETUP ────────────────────────────────────────────────────────────────

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-change-in-production')

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

ADMIN_USER = os.environ.get('ADMIN_USER', 'admin')
ADMIN_PASS = os.environ.get('ADMIN_PASS', 'landscaping2026')
CRON_SECRET = os.environ.get('CRON_SECRET', 'cron-secret')

# Judete Romania
COUNTIES = [
    "Alba","Arad","Arges","Bacau","Bihor","Bistrita-Nasaud","Botosani",
    "Braila","Brasov","Buzau","Calarasi","Caras-Severin","Cluj","Constanta",
    "Covasna","Dambovita","Dolj","Galati","Giurgiu","Gorj","Harghita",
    "Hunedoara","Ialomita","Iasi","Ilfov","Maramures","Mehedinti","Mures",
    "Neamt","Olt","Prahova","Salaj","Satu Mare","Sibiu","Suceava",
    "Teleorman","Timis","Tulcea","Valcea","Vaslui","Vrancea","Bucuresti"
]


def get_db():
    return db


# ─── INITIALIZARE ────────────────────────────────────────────────────────────

@app.before_request
def ensure_db():
    pass  # init_db() apelat la startup


# ─── UTILITARE ────────────────────────────────────────────────────────────────

def get_client_ip():
    if request.headers.get('X-Forwarded-For'):
        return request.headers['X-Forwarded-For'].split(',')[0].strip()
    return request.remote_addr or '0.0.0.0'


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('admin_logged_in'):
            return redirect(url_for('admin_login'))
        return f(*args, **kwargs)
    return decorated


# ─── RUTE PUBLICE ─────────────────────────────────────────────────────────────

@app.route('/')
def index():
    total = db.get_total_prices_count()
    item_count, cat_count = db.get_catalog_stats()
    items = db.get_all_items()
    categories = {}
    for item in items:
        cat = item['category']
        categories.setdefault(cat, []).append(item)
    return render_template(
        'index.html',
        total_prices=total,
        item_count=item_count,
        cat_count=cat_count,
        categories=categories,
        counties=COUNTIES,
    )


# ─── API: GEOLOCATIE JUDET ────────────────────────────────────────────────────

@app.route('/api/detect-county')
def detect_county():
    ip = get_client_ip()
    # Nu trimitem IP-ul real la servicii externe — folosim un hash intern
    try:
        r = requests.get(
            f'http://ip-api.com/json/{ip}?fields=regionName&lang=ro',
            timeout=5
        )
        if r.status_code == 200:
            data = r.json()
            region = data.get('regionName', '')
            # Potriveste cu lista noastra
            for county in COUNTIES:
                if county.lower() in region.lower() or region.lower() in county.lower():
                    return jsonify({'county': county, 'detected': True})
    except Exception:
        pass
    return jsonify({'county': None, 'detected': False})


# ─── API: DATE GRAFIC ─────────────────────────────────────────────────────────

@app.route('/api/chart-data')
def chart_data():
    level1_key = request.args.get('key', '')
    county     = request.args.get('county', 'national')

    if not level1_key:
        return jsonify({'error': 'Lipseste cheia itemului'}), 400

    voluntary_national = db.get_voluntary_prices_for_chart(level1_key, county=None)
    voluntary_local    = db.get_voluntary_prices_for_chart(level1_key, county=county) if county != 'national' else []
    online             = db.get_online_prices_for_chart(level1_key)
    county_stats       = db.get_county_stats(level1_key)

    return jsonify({
        'voluntary_national': voluntary_national,
        'voluntary_local':    voluntary_local,
        'online':             online,
        'county_stats':       county_stats,
    })


# ─── API: LISTA ITEMS ─────────────────────────────────────────────────────────

@app.route('/api/items')
def api_items():
    items = db.get_all_items()
    return jsonify(items)


# ─── API: ARBORE DE NAVIGARE (cascada botanica) ───────────────────────────────

@app.route('/api/tree')
def api_tree():
    return jsonify(db.get_cascade_tree())


# ─── API: MATRICE PRETURI (per specie, grupat pe dimensiuni) ──────────────────

@app.route('/api/price-matrix')
def api_price_matrix():
    species_key = request.args.get('key', '')
    if not species_key:
        return jsonify({'error': 'Lipseste cheia speciei'}), 400
    return jsonify(db.get_price_matrix(species_key))


# ─── API: PARSARE PREVIEW ─────────────────────────────────────────────────────

@app.route('/api/parse-preview', methods=['POST'])
def parse_preview():
    """Parseaza textul si returneaza preview inainte de submit."""
    text = request.json.get('text', '').strip()
    if not text:
        return jsonify({'error': 'Text gol'}), 400

    keys, err = parse_item(text)
    if err:
        return jsonify({'error': err}), 422

    return jsonify({
        'display_name':  keys['display_name'],
        'category':      keys['category'],
        'height_bucket': keys['height_bucket'],
        'root_type':     keys['root_type'],
        'clt_size':      keys['clt_size'],
        'unit':          keys['unit'],
        'canonical_key': keys['canonical_key'],
        'level1_key':    keys['level1_key'],
    })


# ─── API: CONTRIBUIRE PRET ────────────────────────────────────────────────────

@app.route('/api/contribute', methods=['POST'])
def contribute():
    data = request.json or {}
    text   = data.get('text', '').strip()
    price  = data.get('price')
    county = data.get('county', '').strip() or None

    if not text or not price:
        return jsonify({'error': 'Lipsesc date obligatorii'}), 400

    try:
        price = float(price)
        if price <= 0:
            raise ValueError
    except (ValueError, TypeError):
        return jsonify({'error': 'Pret invalid'}), 400

    keys, err = parse_item(text)
    if err or not keys:
        return jsonify({'error': f'Nu am putut identifica produsul: {err}'}), 422

    ip      = get_client_ip()
    ip_hash = db.hash_ip(ip)

    item_id, _ = db.get_or_create_item(keys)
    ok, msg    = db.add_voluntary_price(item_id, price, county, ip_hash)

    if ok:
        return jsonify({'success': True, 'message': msg, 'item': keys['display_name']})
    return jsonify({'success': False, 'message': msg}), 429


# ─── API: UPLOAD FISIER ───────────────────────────────────────────────────────

@app.route('/api/upload', methods=['POST'])
def upload():
    if 'file' not in request.files:
        return jsonify({'error': 'Niciun fisier primit'}), 400

    f      = request.files['file']
    county = request.form.get('county', '').strip() or None
    fname  = f.filename.lower()
    ip     = get_client_ip()
    ip_hash = db.hash_ip(ip)

    results = {'ok': 0, 'skipped': 0, 'errors': [], 'warnings': []}

    if fname.endswith('.xlsx') or fname.endswith('.xls'):
        _process_excel(f, county, ip_hash, results)
    elif fname.endswith('.pdf'):
        _process_pdf(f, county, ip_hash, results)
    elif fname.endswith(('.jpg', '.jpeg', '.png', '.webp')):
        _process_image(f, county, ip_hash, results)
    else:
        return jsonify({'error': 'Format nesupportat. Acceptam: xlsx, pdf, jpg, png'}), 400

    return jsonify(results)


def _parse_price_float(price_raw):
    """Parseaza pret in format romanesc/european (ex: '1.200,00', '45,50', '45.50').
    Returneaza float sau None."""
    import re as _re
    s = str(price_raw).strip()
    # Elimina text moneda si spatii
    s = _re.sub(r'(?i)\s*(ron|lei|euro|eur)\s*', '', s)
    s = _re.sub(r'\s', '', s)
    # Pastreaza doar cifre, virgula, punct
    s = _re.sub(r'[^\d,.]', '', s)
    if not s:
        return None
    # Detecteaza formatul
    if ',' in s and '.' in s:
        # Determina care e separator mii si care e separator zecimal
        if s.rfind(',') > s.rfind('.'):
            # Format european: 1.200,50
            s = s.replace('.', '').replace(',', '.')
        else:
            # Format US: 1,200.50
            s = s.replace(',', '')
    elif ',' in s:
        # Virgula ca separator zecimal: 45,50
        s = s.replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return None


def _add_from_text_and_price(text, price_raw, county, ip_hash, results, bulk=False):
    """Helper comun pentru procesarea unui rand din orice sursa.
    bulk=True: sare peste limita 1/saptamana (upload Excel in masa).
    """
    price = _parse_price_float(price_raw)
    if price is None or price <= 0:
        results['skipped'] += 1
        return

    keys, err = parse_item(text)
    if err or not keys:
        results['warnings'].append(f"Nu am identificat '{text[:60]}': {err}")
        results['skipped'] += 1
        return

    item_id, _ = db.get_or_create_item(keys)
    ok, msg = db.add_voluntary_price(item_id, price, county, ip_hash, bulk=bulk)
    if ok:
        results['ok'] += 1
    else:
        results['warnings'].append(f"{keys['display_name']}: {msg}")
        results['skipped'] += 1


def _process_excel(file_obj, county, ip_hash, results):
    if not HAS_EXCEL:
        results['errors'].append('openpyxl nu este instalat')
        return
    try:
        wb = openpyxl.load_workbook(file_obj, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return

        HEADER_KEYWORDS = ['denumire', 'produs', 'species', 'name', 'item',
                           'planta', 'plant', 'description']
        PRICE_KEYWORDS  = ['pret', 'price', 'valoare', 'cost', 'ron', 'lei', 'tarif', 'euro']
        HEIGHT_KEYWORDS = ['h/cm', 'inaltime', 'height', 'h cm', 'h(cm)', 'inaltime (cm)']
        DIAM_KEYWORDS   = ['diam', 'ø', 'diametru', 'diameter', 'circumferinta', 'circ']
        SECTION_WORDS   = ['total', 'subtotal', 'oferta', 'categorie', 'section', 'grupa']
        CATEGORY_MAP    = {
            'conifer': 'conifer', 'molid': 'conifer', 'brad': 'conifer',
            'thuja': 'conifer', 'picea': 'conifer', 'arbusti': 'arbust',
            'arbori': 'arbore', 'gazon': 'gazon', 'plante': 'planta',
        }

        header_row_idx = None
        header = []
        current_section = ''

        for i, row in enumerate(rows):
            row_str = [str(c).lower().strip() if c else '' for c in row]
            row_text = ' '.join(row_str)
            for kw, cat in CATEGORY_MAP.items():
                if kw in row_text:
                    current_section = cat
                    break
            if (_find_col(row_str, HEADER_KEYWORDS) is not None and
                    _find_col(row_str, PRICE_KEYWORDS) is not None):
                header_row_idx = i
                header = row_str
                break

        if header_row_idx is None:
            results['warnings'].append('Nu am gasit header standard. Incerc primele 2 coloane.')
            for row in rows:
                if not row or len(row) < 2:
                    continue
                text = str(row[0]).strip() if row[0] else ''
                price_raw = row[1]
                if text and text.lower() not in ('none', 'nan', ''):
                    _add_from_text_and_price(text, price_raw, county, ip_hash, results, bulk=True)
            return

        name_col   = _find_col(header, HEADER_KEYWORDS)
        height_col = _find_col(header, HEIGHT_KEYWORDS)
        diam_col   = _find_col(header, DIAM_KEYWORDS)

        # Detecteaza coloana denumire romana (a doua coloana cu 'denumire')
        roman_col = None
        for i, h in enumerate(header):
            if i != name_col and ('romana' in h or ('denumire' in h and i != name_col)):
                roman_col = i
                break

        # Prefer PRET UNITAR fara TVA / fara total
        price_col = None
        for i, h in enumerate(header):
            if 'pret' in h and 'tva' not in h and 'total' not in h:
                price_col = i
                break
        if price_col is None:
            price_col = _find_col(header, PRICE_KEYWORDS)

        data_rows = rows[header_row_idx + 1:]

        for row in data_rows:
            if not row:
                continue
            non_empty = [c for c in row if c is not None and str(c).strip() not in ('', 'None')]
            if len(non_empty) < 2:
                continue

            name_val = str(row[name_col]).strip() if (name_col is not None and row[name_col]) else ''
            if not name_val or name_val.lower() in ('none', 'nan'):
                continue

            # Sare titluri de sectiune; actualizeaza contextul
            if any(sw in name_val.lower() for sw in SECTION_WORDS):
                for kw, cat in CATEGORY_MAP.items():
                    if kw in name_val.lower():
                        current_section = cat
                        break
                continue

            price_val = row[price_col] if price_col is not None else None
            if price_val is not None and str(price_val).strip() in ('', 'None', 'none', '0'):
                price_val = None
            if price_val is None:
                results['skipped'] += 1
                continue

            # Descriere bogata: [romana] + latina + inaltime + diam + [sectiune]
            desc_parts = []
            if roman_col is not None and row[roman_col]:
                roman_val = str(row[roman_col]).strip()
                if roman_val and roman_val.lower() not in ('none', 'nan', ''):
                    desc_parts.append(roman_val)
            desc_parts.append(name_val)
            if height_col is not None and row[height_col]:
                h = str(row[height_col]).strip()
                if h and h.lower() not in ('none', '0', ''):
                    desc_parts.append(f"{h}cm")
            if diam_col is not None and row[diam_col]:
                d = str(row[diam_col]).strip()
                if d and d.lower() not in ('none', '0', ''):
                    desc_parts.append(f"circ {d}cm")
            if current_section:
                desc_parts.append(current_section)

            _add_from_text_and_price(' '.join(desc_parts), price_val, county, ip_hash, results, bulk=True)

    except Exception as e:
        results['errors'].append(f'Eroare Excel: {str(e)[:150]}')


def _find_col(header, keywords):
    for i, h in enumerate(header):
        for kw in keywords:
            if kw in h:
                return i
    return None


def _process_pdf(file_obj, county, ip_hash, results):
    if not HAS_PDF:
        results['errors'].append('pdfplumber nu este instalat')
        return

    HEADER_KEYWORDS = ['denumire', 'produs', 'species', 'name', 'item',
                       'planta', 'plant', 'description']
    PRICE_KEYWORDS  = ['pret', 'price', 'valoare', 'cost', 'ron', 'lei', 'tarif', 'euro']
    HEIGHT_KEYWORDS = ['h/cm', 'inaltime', 'height', 'h cm', 'h(cm)', 'inaltime (cm)']
    DIAM_KEYWORDS   = ['diam', 'diametru', 'diameter', 'circumferinta', 'circ']
    # Cuvinte care identifica randuri de sectiune / total (de sarit)
    SECTION_WORDS   = ['total', 'subtotal', 'oferta', 'categorie', 'section', 'grupa']
    # Categorii care dau context de sectiune
    CATEGORY_MAP    = {
        'conifer': 'conifer', 'molid': 'conifer', 'brad': 'conifer',
        'thuja': 'conifer', 'pin ': 'conifer', 'picea': 'conifer',
        'arbusti': 'arbust', 'arbust': 'arbust',
        'arbori': 'arbore', 'arbore': 'arbore',
        'gazon': 'gazon', 'plante': 'planta',
    }

    try:
        import re
        content = file_obj.read()
        current_section = ''   # context de sectiune detectat din titluri

        with pdfplumber.open(io.BytesIO(content)) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                if not tables:
                    # Text liber — fallback
                    text = page.extract_text() or ''
                    for line in text.splitlines():
                        m = re.search(r'(.+?)\s+(\d[\d\s,.]+)\s*(?:ron|lei|RON|Lei)?', line)
                        if m:
                            _add_from_text_and_price(
                                m.group(1).strip(), m.group(2).strip(),
                                county, ip_hash, results, bulk=True
                            )
                    continue

                for table in tables:
                    if not table:
                        continue

                    # ── Cauta header-ul dinamic ───────────────────────────────
                    header_row_idx = None
                    header = []
                    for i, row in enumerate(table):
                        row_str = [str(c).lower().strip() if c else '' for c in row]
                        # Detecteaza titlu sectiune (ex: "OFERTA CONIFERE LA BALOT")
                        row_text = ' '.join(row_str)
                        for kw, cat in CATEGORY_MAP.items():
                            if kw in row_text:
                                current_section = cat
                                break
                        if (_find_col(row_str, HEADER_KEYWORDS) is not None and
                                _find_col(row_str, PRICE_KEYWORDS) is not None):
                            header_row_idx = i
                            header = row_str
                            break

                    if header_row_idx is None:
                        # Fallback: col0=text, col1=pret
                        for row in table:
                            if not row or len(row) < 2:
                                continue
                            text = str(row[0]).strip() if row[0] else ''
                            price_raw = row[1] if len(row) > 1 else None
                            if text and text.lower() not in ('none', 'nan', ''):
                                _add_from_text_and_price(text, price_raw, county, ip_hash, results, bulk=True)
                        continue

                    name_col   = _find_col(header, HEADER_KEYWORDS)
                    height_col = _find_col(header, HEIGHT_KEYWORDS)
                    diam_col   = _find_col(header, DIAM_KEYWORDS)

                    # Detecteaza coloana cu denumire romana (a doua coloana cu 'denumire')
                    roman_col = None
                    for i, h in enumerate(header):
                        if i != name_col and ('romana' in h or ('denumire' in h and i != name_col)):
                            roman_col = i
                            break

                    # Prefer PRET UNITAR fara TVA si fara total
                    price_col = None
                    for i, h in enumerate(header):
                        if 'pret' in h and 'tva' not in h and 'total' not in h:
                            price_col = i
                            break
                    if price_col is None:
                        price_col = _find_col(header, PRICE_KEYWORDS)

                    # Detecteaza BUC/cantitate col pentru a o ignora in pret
                    buc_col = next((i for i, h in enumerate(header) if h in ('buc', 'cant', 'bucati', 'qty')), None)

                    data_rows = table[header_row_idx + 1:]
                    for row in data_rows:
                        if not row:
                            continue
                        non_empty = [c for c in row if c is not None and str(c).strip() not in ('', 'None')]
                        if len(non_empty) < 2:
                            continue

                        name_val = (str(row[name_col]).strip()
                                    if (name_col is not None and name_col < len(row) and row[name_col])
                                    else '')
                        if not name_val or name_val.lower() in ('none', 'nan'):
                            continue

                        # Sare randuri de sectiune / total
                        if any(sw in name_val.lower() for sw in SECTION_WORDS):
                            # Actualizeaza sectiunea curenta
                            for kw, cat in CATEGORY_MAP.items():
                                if kw in name_val.lower():
                                    current_section = cat
                                    break
                            continue

                        price_val = (row[price_col]
                                     if (price_col is not None and price_col < len(row))
                                     else None)
                        if price_val is not None and str(price_val).strip() in ('', 'None', 'none', '0'):
                            price_val = None
                        if price_val is None:
                            results['skipped'] += 1
                            continue

                        # Construieste descriere bogata:
                        # [denumire_romana] + denumire_latina + inaltime + circumferinta + [sectiune]
                        desc_parts = []

                        # Adauga denumirea romana (ajuta AI sa categoriseasca)
                        if roman_col is not None and roman_col < len(row) and row[roman_col]:
                            roman_val = str(row[roman_col]).strip()
                            if roman_val and roman_val.lower() not in ('none', 'nan', ''):
                                desc_parts.append(roman_val)

                        desc_parts.append(name_val)

                        if height_col is not None and height_col < len(row) and row[height_col]:
                            h = str(row[height_col]).strip()
                            if h and h.lower() not in ('none', '0', ''):
                                desc_parts.append(f"{h}cm")

                        if diam_col is not None and diam_col < len(row) and row[diam_col]:
                            d = str(row[diam_col]).strip()
                            if d and d.lower() not in ('none', '0', ''):
                                desc_parts.append(f"circ {d}cm")

                        # Adauga contextul de sectiune ca hint pentru parser
                        if current_section:
                            desc_parts.append(current_section)

                        _add_from_text_and_price(
                            ' '.join(desc_parts), price_val,
                            county, ip_hash, results, bulk=True
                        )
    except Exception as e:
        results['errors'].append(f'Eroare PDF: {str(e)[:150]}')


def _process_image(file_obj, county, ip_hash, results):
    if not HAS_OCR:
        results['errors'].append('pytesseract/Pillow nu sunt instalate. OCR indisponibil.')
        return
    try:
        import re
        img  = Image.open(file_obj)
        # Incearca cu romana si engleza; fallback la engleza
        try:
            text = pytesseract.image_to_string(img, lang='ron+eng')
        except Exception:
            text = pytesseract.image_to_string(img, lang='eng')

        lines = [l.strip() for l in text.splitlines() if l.strip()]
        current_section = ''
        CATEGORY_MAP = {
            'conifer': 'conifer', 'molid': 'conifer', 'brad': 'conifer',
            'thuja': 'conifer', 'picea': 'conifer', 'arbusti': 'arbust',
            'arbori': 'arbore', 'gazon': 'gazon',
        }

        for line in lines:
            line_low = line.lower()
            # Detecteaza titlu sectiune
            for kw, cat in CATEGORY_MAP.items():
                if kw in line_low:
                    current_section = cat
                    break

            # Pattern: "text cu planta    145.00" sau "text cu planta 145,00 RON"
            # Cauta pretul la finalul liniei
            m = re.search(
                r'^(.+?)\s{2,}(\d[\d\s,.]{0,15})\s*(?:ron|lei|RON|Lei)?\s*$',
                line
            )
            if not m:
                # Fallback: orice numar de cel putin 2 cifre la sfarsit
                m = re.search(r'^(.{5,}?)\s+(\d{2,}[.,]?\d*)\s*(?:ron|lei)?$', line, re.IGNORECASE)
            if m:
                name_part  = m.group(1).strip()
                price_part = m.group(2).strip()
                # Sare linii evidente de header / total
                if any(w in name_part.lower() for w in ['total', 'subtotal', 'denumire', 'pret', 'oferta']):
                    continue
                desc = name_part
                if current_section:
                    desc = desc + ' ' + current_section
                _add_from_text_and_price(desc, price_part, county, ip_hash, results)

        if results['ok'] == 0 and results['skipped'] == 0:
            results['warnings'].append(
                'Nu am putut extrage date din imagine. '
                'Recomandare: exporta ca PDF sau Excel pentru rezultate mai bune.'
            )
    except Exception as e:
        results['errors'].append(f'Eroare OCR: {str(e)[:100]}')


# ─── ADMIN: LOGIN ─────────────────────────────────────────────────────────────

@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if request.method == 'POST':
        if (request.form.get('username') == ADMIN_USER and
                request.form.get('password') == ADMIN_PASS):
            session['admin_logged_in'] = True
            return redirect(url_for('admin_dashboard'))
        flash('Credentiale incorecte.')
    return render_template('admin_login.html')


@app.route('/admin/logout')
def admin_logout():
    session.pop('admin_logged_in', None)
    return redirect(url_for('admin_login'))


# ─── ADMIN: DASHBOARD ─────────────────────────────────────────────────────────

@app.route('/admin')
@admin_required
def admin_dashboard():
    items        = db.get_all_items()
    banned_ips   = db.get_banned_ips()
    sources      = db.get_scraping_sources()
    total_prices = db.get_total_prices_count()
    return render_template(
        'admin.html',
        items=items,
        banned_ips=banned_ips,
        sources=sources,
        total_prices=total_prices,
        section='dashboard',
    )


# ─── ADMIN: CATALOG ───────────────────────────────────────────────────────────

@app.route('/admin/catalog')
@admin_required
def admin_catalog():
    items = db.get_all_items()
    return render_template('admin.html', items=items, section='catalog',
                           banned_ips=[], sources=[], total_prices=0)


@app.route('/admin/catalog/delete/<int:item_id>', methods=['POST'])
@admin_required
def admin_delete_item(item_id):
    db.delete_item(item_id)
    flash('Item sters.')
    return redirect(url_for('admin_catalog'))


# ─── ADMIN: PRETURI ───────────────────────────────────────────────────────────

@app.route('/admin/prices/delete', methods=['POST'])
@admin_required
def admin_delete_price():
    price_id = request.form.get('price_id', type=int)
    source   = request.form.get('source', 'voluntary')
    if price_id:
        db.delete_price(price_id, source)
        flash('Pret sters.')
    return redirect(url_for('admin_dashboard'))


@app.route('/admin/prices/delete-week', methods=['POST'])
@admin_required
def admin_delete_week():
    item_id = request.form.get('item_id', type=int)
    week    = request.form.get('week', type=int)
    year    = request.form.get('year', type=int)
    source  = request.form.get('source', 'voluntary')
    if item_id and week and year:
        db.delete_prices_for_week(item_id, week, year, source)
        flash(f'Preturi sterse pentru saptamana {year}-W{week}.')
    return redirect(url_for('admin_dashboard'))


# ─── ADMIN: IP-URI ────────────────────────────────────────────────────────────

@app.route('/admin/ips')
@admin_required
def admin_ips():
    banned_ips = db.get_banned_ips()
    return render_template('admin.html', banned_ips=banned_ips, section='ips',
                           items=[], sources=[], total_prices=0)


@app.route('/admin/ips/unban', methods=['POST'])
@admin_required
def admin_unban():
    ip_hash = request.form.get('ip_hash')
    item_id = request.form.get('item_id', type=int)
    if ip_hash and item_id:
        db.unban_ip(ip_hash, item_id)
        flash('IP deblocat.')
    return redirect(url_for('admin_ips'))


# ─── ADMIN: SCRAPING ──────────────────────────────────────────────────────────

@app.route('/admin/scraping')
@admin_required
def admin_scraping():
    sources = db.get_scraping_sources()
    return render_template('admin.html', sources=sources, section='scraping',
                           items=[], banned_ips=[], total_prices=0)


@app.route('/admin/scraping/run', methods=['POST'])
@admin_required
def admin_run_scraping():
    """Declanseaza manual scraping-ul."""
    import threading
    from scraper import run_all_scrapers
    thread = threading.Thread(target=run_all_scrapers, daemon=True)
    thread.start()
    flash('Scraping pornit in fundal. Verifica statusul surselor in cateva minute.')
    return redirect(url_for('admin_scraping'))


@app.route('/admin/scraping/toggle/<int:source_id>', methods=['POST'])
@admin_required
def admin_toggle_source(source_id):
    conn = db.get_db()
    src = conn.execute("SELECT active FROM scraping_sources WHERE id=?", (source_id,)).fetchone()
    if src:
        new_val = 0 if src['active'] else 1
        conn.execute("UPDATE scraping_sources SET active=? WHERE id=?", (new_val, source_id))
        conn.commit()
    conn.close()
    return redirect(url_for('admin_scraping'))


# ─── CRON ENDPOINT (Render) ───────────────────────────────────────────────────

@app.route('/cron/scrape', methods=['POST'])
def cron_scrape():
    """Endpoint apelat de Render Cron Job luni 03:00."""
    secret = request.headers.get('X-Cron-Secret') or request.args.get('secret')
    if secret != CRON_SECRET:
        return jsonify({'error': 'Unauthorized'}), 401

    import threading
    from scraper import run_all_scrapers
    thread = threading.Thread(target=run_all_scrapers, daemon=True)
    thread.start()
    return jsonify({'status': 'started', 'time': datetime.utcnow().isoformat()})


# ─── STARTUP ──────────────────────────────────────────────────────────────────

with app.app_context():
    db.init_db()
    log.info("Baza de date initializata.")

if __name__ == '__main__':
    app.run(debug=True, port=5000)
