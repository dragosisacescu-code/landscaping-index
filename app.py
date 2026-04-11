import os
import hashlib
import json
import random
import urllib.request
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify
import sqlite3

try:
    import openpyxl
    EXCEL_SUPPORT = True
except ImportError:
    EXCEL_SUPPORT = False

try:
    import pdfplumber
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False

try:
    from PIL import Image
    import pytesseract
    OCR_SUPPORT = True
except ImportError:
    OCR_SUPPORT = False

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

DB_PATH = 'landscaping.db'

# ─────────────────────────────────────────────
# ROMANIA COUNTIES
# ─────────────────────────────────────────────
JUDETE = [
    "Alba", "Arad", "Argeș", "Bacău", "Bihor", "Bistrița-Năsăud",
    "Botoșani", "Brăila", "Brașov", "București", "Buzău", "Călărași",
    "Cluj", "Constanța", "Covasna", "Dâmbovița", "Dolj", "Galați",
    "Giurgiu", "Gorj", "Harghita", "Hunedoara", "Ialomița", "Iași",
    "Ilfov", "Maramureș", "Mehedinți", "Mureș", "Neamț", "Olt",
    "Prahova", "Sălaj", "Satu Mare", "Sibiu", "Suceava", "Teleorman",
    "Timiș", "Tulcea", "Vâlcea", "Vaslui", "Vrancea"
]

# Weight distribution for seeding realistic data (bigger cities = more data)
JUDETE_WEIGHTS = {
    "București": 0.18, "Cluj": 0.09, "Timiș": 0.07, "Iași": 0.07,
    "Prahova": 0.05, "Constanța": 0.05, "Brașov": 0.05, "Argeș": 0.04,
}

# ─────────────────────────────────────────────
# ITEMS CATALOG
# ─────────────────────────────────────────────
INITIAL_ITEMS = [
    ('Molid Argintiu 200-250cm', 'Conifere', 'buc', 2500),
    ('Pinus Sylvestris', 'Conifere', 'buc', 2500),
    ('Lavandă 20-30cm', 'Arbuști', 'buc', 50),
    ('Liliac de Vară 50-80cm', 'Arbuști', 'buc', 50),
    ('Gutui Japonez', 'Arbuști', 'buc', 40),
    ('Euonymus Japonicus', 'Arbuști', 'buc', 40),
    ('Mesteacăn 500-600cm', 'Arbori', 'buc', 1650),
    ('Stejar Roșu', 'Arbori', 'buc', 3250),
    ('Magnolie Albă', 'Arbori', 'buc', 3000),
    ('Paltin', 'Arbori', 'buc', 2375),
    ('Pământ Vegetal Fertil', 'Materiale', 'tonă', 490),
    ('Gazon Rulou', 'Materiale', 'mp', 32.5),
    ('Piatră Decorativă', 'Materiale', 'tonă', 1800),
]


# ─────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        category TEXT,
        unit TEXT,
        base_price REAL
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS online_prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id INTEGER,
        price REAL,
        date TEXT,
        source TEXT,
        county TEXT DEFAULT 'Național',
        FOREIGN KEY(item_id) REFERENCES items(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS voluntary_prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id INTEGER,
        price REAL,
        date TEXT,
        ip_hash TEXT,
        county TEXT DEFAULT 'Național',
        FOREIGN KEY(item_id) REFERENCES items(id)
    )''')

    c.execute('SELECT COUNT(*) FROM items')
    if c.fetchone()[0] == 0:
        c.executemany(
            'INSERT INTO items (name, category, unit, base_price) VALUES (?,?,?,?)',
            INITIAL_ITEMS
        )
        conn.commit()
        seed_historical_data(conn)

    conn.commit()
    conn.close()


def seed_historical_data(conn):
    """Seed 3 years of weekly prices, distributed across județe."""
    c = conn.cursor()
    c.execute('SELECT id, base_price FROM items')
    items = c.fetchall()
    today = datetime.today()
    weeks_back = 156

    # Build weighted county list for random sampling
    weighted = []
    for j in JUDETE:
        w = JUDETE_WEIGHTS.get(j, 0.015)
        weighted.append((j, w))

    counties_pool = []
    for j, w in weighted:
        counties_pool.extend([j] * int(w * 200))

    for item_id, base_price in items:
        # Online prices: national only
        price = base_price
        for week in range(weeks_back, 0, -1):
            date = (today - timedelta(weeks=week)).strftime('%Y-%m-%d')
            drift = random.uniform(-0.025, 0.035)
            price = round(price * (1 + drift), 2)
            c.execute(
                'INSERT INTO online_prices (item_id, price, date, source, county) VALUES (?,?,?,?,?)',
                (item_id, price, date, 'simulat', 'Național')
            )

        # Voluntary prices: distributed across județe with realistic volume
        for week in range(weeks_back, 0, -1):
            date = (today - timedelta(weeks=week)).strftime('%Y-%m-%d')
            # 0–4 random voluntary submissions per week across counties
            n_submissions = random.choices([0, 1, 2, 3, 4], weights=[0.5, 0.25, 0.13, 0.08, 0.04])[0]
            for _ in range(n_submissions):
                county = random.choice(counties_pool)
                # County pricing: slight regional variance ±8%
                regional_factor = random.uniform(0.92, 1.08)
                vol_price = round(base_price * regional_factor * random.uniform(0.96, 1.06), 2)
                fake_ip_hash = hashlib.sha256(f"{item_id}-{week}-{county}-{_}".encode()).hexdigest()
                c.execute(
                    'INSERT INTO voluntary_prices (item_id, price, date, ip_hash, county) VALUES (?,?,?,?,?)',
                    (item_id, vol_price, date, fake_ip_hash, county)
                )

    conn.commit()


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def anonymize_ip(ip: str) -> str:
    return hashlib.sha256(ip.encode()).hexdigest()


def check_rate_limit(item_id: int, ip_hash: str) -> bool:
    conn = get_db()
    c = conn.cursor()
    one_week_ago = (datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d')
    c.execute(
        'SELECT COUNT(*) FROM voluntary_prices WHERE item_id=? AND ip_hash=? AND date >= ?',
        (item_id, ip_hash, one_week_ago)
    )
    count = c.fetchone()[0]
    conn.close()
    return count == 0


def aggregate_weekly(rows, value_col='price'):
    """
    Groups rows by ISO week.
    Returns: list of {week, avg, count} sorted by week.
    """
    from collections import defaultdict
    buckets = defaultdict(list)
    for row in rows:
        try:
            dt = datetime.strptime(row['date'], '%Y-%m-%d')
            week_key = dt.strftime('%Y-W%V')
            buckets[week_key].append(row[value_col])
        except Exception:
            pass
    result = []
    for week in sorted(buckets.keys()):
        vals = buckets[week]
        result.append({
            'week': week,
            'avg': round(sum(vals) / len(vals), 2),
            'count': len(vals)
        })
    return result


def detect_county_from_ip(ip: str) -> str:
    """
    Uses ip-api.com (free, no key needed) to detect Romanian county.
    Falls back to 'Național' on any error.
    """
    try:
        if ip in ('127.0.0.1', '::1', 'localhost'):
            return 'Național'
        url = f'http://ip-api.com/json/{ip}?fields=status,regionName,countryCode'
        req = urllib.request.Request(url, headers={'User-Agent': 'LandscapingIndex/1.0'})
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read().decode())
        if data.get('status') == 'success' and data.get('countryCode') == 'RO':
            region = data.get('regionName', '')
            # Try to match to our județ list
            for j in JUDETE:
                if j.lower() in region.lower() or region.lower() in j.lower():
                    return j
    except Exception:
        pass
    return 'Național'


# ─────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────
@app.route('/')
def index():
    conn = get_db()
    items = conn.execute('SELECT * FROM items ORDER BY category, name').fetchall()
    conn.close()
    categories = {}
    for item in items:
        cat = item['category']
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(dict(item))
    return render_template('index.html',
                           categories=categories,
                           items=[dict(i) for i in items],
                           judete=JUDETE)


@app.route('/api/detect-county')
def api_detect_county():
    raw_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    ip = raw_ip.split(',')[0].strip()
    county = detect_county_from_ip(ip)
    return jsonify({'county': county, 'ip_detected': ip[:8] + '***'})


@app.route('/api/items')
def api_items():
    conn = get_db()
    items = conn.execute('SELECT * FROM items ORDER BY category, name').fetchall()
    conn.close()
    return jsonify([dict(i) for i in items])


@app.route('/api/prices/<int:item_id>')
def api_prices(item_id):
    """Main chart: national online + national voluntary (weekly avg)."""
    conn = get_db()
    online_rows = conn.execute(
        "SELECT price, date FROM online_prices WHERE item_id=? AND county='Național' ORDER BY date",
        (item_id,)
    ).fetchall()
    vol_rows = conn.execute(
        'SELECT price, date FROM voluntary_prices WHERE item_id=? ORDER BY date',
        (item_id,)
    ).fetchall()
    conn.close()
    return jsonify({
        'online': aggregate_weekly([dict(r) for r in online_rows]),
        'voluntary': aggregate_weekly([dict(r) for r in vol_rows])
    })


@app.route('/api/prices/<int:item_id>/detail')
def api_prices_detail(item_id):
    """
    Detail view: national + county breakdown, with count (cantitativ).
    Query param: ?county=Cluj
    """
    county = request.args.get('county', 'Național')
    conn = get_db()

    # National voluntary
    nat_vol = conn.execute(
        'SELECT price, date FROM voluntary_prices WHERE item_id=? ORDER BY date',
        (item_id,)
    ).fetchall()

    # National online
    nat_online = conn.execute(
        "SELECT price, date FROM online_prices WHERE item_id=? AND county='Național' ORDER BY date",
        (item_id,)
    ).fetchall()

    # County voluntary
    county_vol = conn.execute(
        'SELECT price, date FROM voluntary_prices WHERE item_id=? AND county=? ORDER BY date',
        (item_id, county)
    ).fetchall()

    # County online (if exists, else fallback to national)
    county_online = conn.execute(
        'SELECT price, date FROM online_prices WHERE item_id=? AND county=? ORDER BY date',
        (item_id, county)
    ).fetchall()

    # County total count of submissions
    county_total = conn.execute(
        'SELECT COUNT(*) FROM voluntary_prices WHERE item_id=? AND county=?',
        (item_id, county)
    ).fetchone()[0]

    national_total = conn.execute(
        'SELECT COUNT(*) FROM voluntary_prices WHERE item_id=?',
        (item_id,)
    ).fetchone()[0]

    # County breakdown (top 10 județe by submission count)
    county_counts = conn.execute(
        '''SELECT county, COUNT(*) as cnt, AVG(price) as avg_price
           FROM voluntary_prices WHERE item_id=?
           GROUP BY county ORDER BY cnt DESC LIMIT 10''',
        (item_id,)
    ).fetchall()

    conn.close()

    return jsonify({
        'national': {
            'online': aggregate_weekly([dict(r) for r in nat_online]),
            'voluntary': aggregate_weekly([dict(r) for r in nat_vol]),
            'total_submissions': national_total
        },
        'county': {
            'name': county,
            'online': aggregate_weekly([dict(r) for r in county_online]) if county_online else [],
            'voluntary': aggregate_weekly([dict(r) for r in county_vol]),
            'total_submissions': county_total
        },
        'top_counties': [
            {'county': r['county'], 'count': r['cnt'], 'avg_price': round(r['avg_price'], 2)}
            for r in county_counts
        ]
    })


@app.route('/api/submit', methods=['POST'])
def api_submit():
    data = request.get_json()
    item_id = data.get('item_id')
    price = data.get('price')
    county = data.get('county', 'Național')

    if not item_id or not price:
        return jsonify({'error': 'Date lipsă'}), 400
    try:
        price = float(price)
        item_id = int(item_id)
    except (ValueError, TypeError):
        return jsonify({'error': 'Date invalide'}), 400

    if price <= 0 or price > 1_000_000:
        return jsonify({'error': 'Preț în afara intervalului acceptat'}), 400

    if county not in JUDETE and county != 'Național':
        county = 'Național'

    raw_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    ip_hash = anonymize_ip(raw_ip.split(',')[0].strip())

    if not check_rate_limit(item_id, ip_hash):
        return jsonify({'error': 'Ai contribuit deja pentru acest item în ultimele 7 zile.'}), 429

    today = datetime.today().strftime('%Y-%m-%d')
    conn = get_db()
    conn.execute(
        'INSERT INTO voluntary_prices (item_id, price, date, ip_hash, county) VALUES (?,?,?,?,?)',
        (item_id, price, today, ip_hash, county)
    )
    conn.commit()
    conn.close()
    return jsonify({'success': True, 'message': f'Preț înregistrat pentru {county}. Mulțumim!'})


@app.route('/api/scrape', methods=['POST'])
def api_scrape():
    conn = get_db()
    items = conn.execute('SELECT id, base_price FROM items').fetchall()
    today = datetime.today().strftime('%Y-%m-%d')
    count = 0
    for item in items:
        item_id, base_price = item['id'], item['base_price']
        exists = conn.execute(
            "SELECT COUNT(*) FROM online_prices WHERE item_id=? AND date=? AND source='scraper'",
            (item_id, today)
        ).fetchone()[0]
        if not exists:
            simulated_price = round(base_price * random.uniform(0.95, 1.07), 2)
            conn.execute(
                "INSERT INTO online_prices (item_id, price, date, source, county) VALUES (?,?,?,?,?)",
                (item_id, simulated_price, today, 'scraper', 'Național')
            )
            count += 1
    conn.commit()
    conn.close()
    return jsonify({'success': True, 'scraped': count, 'date': today})


@app.route('/api/upload', methods=['POST'])
def api_upload():
    if 'file' not in request.files:
        return jsonify({'error': 'Niciun fișier trimis'}), 400
    file = request.files['file']
    county = request.form.get('county', 'Național')
    if county not in JUDETE:
        county = 'Național'

    raw_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    ip_hash = anonymize_ip(raw_ip.split(',')[0].strip())

    filename = file.filename.lower()
    results = {'imported': 0, 'skipped': 0, 'errors': []}
    try:
        if filename.endswith(('.xlsx', '.xls')):
            results = parse_excel(file, ip_hash, county)
        elif filename.endswith('.pdf'):
            results = parse_pdf(file, ip_hash, county)
        elif filename.endswith(('.png', '.jpg', '.jpeg', '.webp', '.tiff')):
            results = parse_image(file, ip_hash, county)
        else:
            return jsonify({'error': 'Format nesupportat. Acceptăm: .xlsx, .pdf, .png, .jpg'}), 400
    except Exception as e:
        return jsonify({'error': f'Eroare la procesare: {str(e)}'}), 500

    return jsonify(results)


def import_price_row(item_name: str, price: float, ip_hash: str, county: str = 'Național') -> str:
    conn = get_db()
    row = conn.execute(
        "SELECT id FROM items WHERE LOWER(name) LIKE LOWER(?)",
        (f'%{item_name.strip()}%',)
    ).fetchone()
    if not row:
        conn.close()
        return f'not_found:{item_name}'
    item_id = row['id']
    if not check_rate_limit(item_id, ip_hash):
        conn.close()
        return f'rate_limited:{item_name}'
    today = datetime.today().strftime('%Y-%m-%d')
    conn.execute(
        'INSERT INTO voluntary_prices (item_id, price, date, ip_hash, county) VALUES (?,?,?,?,?)',
        (item_id, price, today, ip_hash, county)
    )
    conn.commit()
    conn.close()
    return 'ok'


def parse_excel(file, ip_hash, county):
    if not EXCEL_SUPPORT:
        return {'error': 'openpyxl nu este instalat'}
    wb = openpyxl.load_workbook(file, data_only=True)
    ws = wb.active
    imported, skipped, errors = 0, 0, []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if not row or row[0] is None:
            continue
        item_name = str(row[0])
        try:
            price = float(str(row[1]).replace(',', '.').replace(' ', ''))
        except (ValueError, TypeError, IndexError):
            errors.append(f'Preț invalid: {item_name}')
            skipped += 1
            continue
        status = import_price_row(item_name, price, ip_hash, county)
        if status == 'ok':
            imported += 1
        else:
            skipped += 1
            errors.append(status)
    return {'imported': imported, 'skipped': skipped, 'errors': errors}


def parse_pdf(file, ip_hash, county):
    if not PDF_SUPPORT:
        return {'error': 'pdfplumber nu este instalat'}
    import re
    imported, skipped, errors = 0, 0, []
    with pdfplumber.open(file) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ''
            matches = re.findall(r'([A-Za-zÀ-ÿ\s\-]{4,40})\s+(\d[\d\s.,]*)\s*(?:RON|ron|lei)?', text)
            for name, price_str in matches:
                try:
                    price = float(price_str.replace(',', '.').replace(' ', ''))
                except ValueError:
                    skipped += 1
                    continue
                status = import_price_row(name.strip(), price, ip_hash, county)
                if status == 'ok':
                    imported += 1
                else:
                    skipped += 1
    return {'imported': imported, 'skipped': skipped, 'errors': errors}


def parse_image(file, ip_hash, county):
    if not OCR_SUPPORT:
        return {'error': 'pytesseract/Pillow nu sunt instalate'}
    import re
    img = Image.open(file)
    text = pytesseract.image_to_string(img, lang='ron+eng')
    imported, skipped, errors = 0, 0, []
    matches = re.findall(r'([A-Za-zÀ-ÿ\s\-]{4,40})\s+(\d[\d\s.,]*)\s*(?:RON|ron|lei)?', text)
    for name, price_str in matches:
        try:
            price = float(price_str.replace(',', '.').replace(' ', ''))
        except ValueError:
            skipped += 1
            continue
        status = import_price_row(name.strip(), price, ip_hash, county)
        if status == 'ok':
            imported += 1
        else:
            skipped += 1
    return {'imported': imported, 'skipped': skipped, 'errors': errors}


if __name__ == '__main__':
    os.makedirs('uploads', exist_ok=True)
    init_db()
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
