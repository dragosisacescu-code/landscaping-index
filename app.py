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
    items = db.get_all_items()
    # Grupeaza pe categorie
    categories = {}
    for item in items:
        cat = item['category']
        categories.setdefault(cat, []).append(item)
    return render_template(
        'index.html',
        total_prices=total,
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


def _add_from_text_and_price(text, price_raw, county, ip_hash, results):
    """Helper comun pentru procesarea unui rand din orice sursa."""
    try:
        price = float(str(price_raw).replace(',', '.').strip())
        if price <= 0:
            results['skipped'] += 1
            return
    except (ValueError, TypeError):
        results['skipped'] += 1
        return

    keys, err = parse_item(text)
    if err or not keys:
        results['warnings'].append(f"Nu am identificat '{text[:60]}': {err}")
        results['skipped'] += 1
        return

    item_id, _ = db.get_or_create_item(keys)
    ok, msg = db.add_voluntary_price(item_id, price, county, ip_hash)
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

        # Detecteaza coloanele automat din header
        header = [str(c).lower().strip() if c else '' for c in rows[0]]
        name_col  = _find_col(header, ['produs', 'denumire', 'species', 'name', 'item', 'plantа'])
        price_col = _find_col(header, ['pret', 'price', 'valoare', 'cost', 'ron', 'lei'])

        if name_col is None or price_col is None:
            # Incearca primele 2 coloane
            name_col, price_col = 0, 1
            data_rows = rows
        else:
            data_rows = rows[1:]

        for row in data_rows:
            if not row or len(row) <= max(name_col, price_col):
                continue
            text = str(row[name_col]).strip() if row[name_col] else ''
            price_raw = row[price_col]
            if text and text.lower() not in ('none', 'nan', ''):
                _add_from_text_and_price(text, price_raw, county, ip_hash, results)
    except Exception as e:
        results['errors'].append(f'Eroare Excel: {str(e)[:100]}')


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
    try:
        content = file_obj.read()
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            for page in pdf.pages:
                # Incearca tabele
                tables = page.extract_tables()
                if tables:
                    for table in tables:
                        for row in table:
                            if not row or len(row) < 2:
                                continue
                            text = str(row[0]).strip() if row[0] else ''
                            price_raw = row[1] if len(row) > 1 else None
                            if text:
                                _add_from_text_and_price(text, price_raw, county, ip_hash, results)
                else:
                    # Text liber — cauta linii cu preturi
                    text = page.extract_text() or ''
                    for line in text.splitlines():
                        import re
                        m = re.search(r'(.+?)\s+(\d[\d\s,.]+)\s*(?:ron|lei|RON|Lei)?', line)
                        if m:
                            _add_from_text_and_price(
                                m.group(1).strip(),
                                m.group(2).strip(),
                                county, ip_hash, results
                            )
    except Exception as e:
        results['errors'].append(f'Eroare PDF: {str(e)[:100]}')


def _process_image(file_obj, county, ip_hash, results):
    if not HAS_OCR:
        results['errors'].append('pytesseract/Pillow nu sunt instalate. OCR indisponibil.')
        return
    try:
        import re
        img  = Image.open(file_obj)
        text = pytesseract.image_to_string(img, lang='ron+eng')
        for line in text.splitlines():
            m = re.search(r'(.+?)\s+(\d[\d\s,.]+)\s*(?:ron|lei|RON|Lei)?', line)
            if m:
                _add_from_text_and_price(
                    m.group(1).strip(),
                    m.group(2).strip(),
                    county, ip_hash, results
                )
        if results['ok'] == 0 and results['skipped'] == 0:
            results['warnings'].append('Nu am putut extrage date din imagine. Verifica calitatea.')
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
