import os
import sqlite3
import hashlib
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify
import openpyxl
import pdfplumber
from io import BytesIO
 
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB
 
DATABASE = os.path.join(os.path.dirname(__file__), 'landscaping.db')
 
# ─── DATABASE ────────────────────────────────────────────────────────────────
 
def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn
 
def init_db():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS items (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            name     TEXT NOT NULL,
            category TEXT NOT NULL,
            unit     TEXT DEFAULT 'buc'
        );
        CREATE TABLE IF NOT EXISTS online_prices (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id   INTEGER NOT NULL,
            price     REAL NOT NULL,
            source    TEXT,
            week_date TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (item_id) REFERENCES items(id)
        );
        CREATE TABLE IF NOT EXISTS voluntary_prices (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id   INTEGER NOT NULL,
            price     REAL NOT NULL,
            ip_hash   TEXT NOT NULL,
            week_date TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (item_id) REFERENCES items(id)
        );
 
        -- Înregistrează fiecare tentativă blocată (deviație prea mare)
        CREATE TABLE IF NOT EXISTS ip_violations (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ip_hash         TEXT NOT NULL,
            item_id         INTEGER NOT NULL,
            attempted_price REAL NOT NULL,
            last_price      REAL NOT NULL,
            deviation_pct   REAL NOT NULL,
            week_date       TEXT NOT NULL,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
 
        -- Banuri active (30 zile sau până piața confirmă prețul)
        CREATE TABLE IF NOT EXISTS ip_bans (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ip_hash         TEXT NOT NULL,
            item_id         INTEGER NOT NULL,
            attempted_price REAL NOT NULL,
            banned_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            active          INTEGER DEFAULT 1
        );
    ''')
 
    if conn.execute('SELECT COUNT(*) FROM items').fetchone()[0] == 0:
        seed_data = [
            ('Molid Argintiu 200-250cm', 'Conifere', 'buc',   2500),
            ('Pinus Sylvestris',         'Conifere', 'buc',   2500),
            ('Lavandă 20-30cm',          'Arbuști',  'buc',     50),
            ('Liliac de Vară 50-80cm',   'Arbuști',  'buc',     50),
            ('Gutui Japonez',            'Arbuști',  'buc',     40),
            ('Euonymus Japonicus',       'Arbuști',  'buc',     40),
            ('Mesteacăn 500-600cm',      'Arbori',   'buc',   1650),
            ('Stejar Roșu',              'Arbori',   'buc',   3250),
            ('Magnolie Albă',            'Arbori',   'buc',   3000),
            ('Paltin',                   'Arbori',   'buc',   2375),
            ('Pământ Vegetal Fertil',    'Materiale','tonă',   490),
            ('Gazon Rulou',              'Materiale','mp',    32.5),
            ('Piatră Decorativă',        'Materiale','tonă',  1800),
        ]
        week = get_week_date()
        for name, cat, unit, price in seed_data:
            conn.execute('INSERT INTO items (name,category,unit) VALUES (?,?,?)', (name, cat, unit))
            conn.commit()
            item = conn.execute('SELECT id FROM items WHERE name=?', (name,)).fetchone()
            conn.execute(
                'INSERT INTO online_prices (item_id,price,source,week_date) VALUES (?,?,?,?)',
                (item['id'], price, 'PLANTE 2025.pdf', week)
            )
        conn.commit()
    conn.close()
 
# ─── HELPERS ─────────────────────────────────────────────────────────────────
 
def hash_ip(ip):
    salt = os.environ.get('IP_SALT', 'landscaping-ro-2026')
    return hashlib.sha256(f"{salt}{ip}".encode()).hexdigest()[:20]
 
def get_week_date():
    return datetime.now().strftime('%G-W%V')
 
def is_rate_limited(ip_hash, item_id):
    conn = get_db()
    n = conn.execute(
        'SELECT COUNT(*) FROM voluntary_prices WHERE ip_hash=? AND item_id=? AND week_date=?',
        (ip_hash, item_id, get_week_date())
    ).fetchone()[0]
    conn.close()
    return n > 0
 
def check_and_enforce_rules(ip_hash, item_id, new_price):
    """
    Sistem anti-manipulare în 3 straturi.
 
    Stratul 1 — Prima abatere:  toleranță ±10% față de ultimul preț propriu.
    Stratul 2 — Abaterile 2+:  toleranță ±5%  față de ultimul preț propriu.
    Stratul 3 — A 3-a abatere: ban 30 zile.
 
    Deblocare anticipată (după cele 30 zile):
      - Media voluntară a produsului ≥ 20 prețuri
      - Și media se află în intervalul ±10% față de prețul pe care IP-ul încerca să îl trimită.
 
    Returnează (permis: bool, mesaj_eroare: str | None)
    """
    conn = get_db()
 
    # ── 1. Verifică ban activ ──────────────────────────────────────────────
    ban = conn.execute(
        '''SELECT id, attempted_price, banned_at FROM ip_bans
           WHERE ip_hash=? AND item_id=? AND active=1
           ORDER BY banned_at DESC LIMIT 1''',
        (ip_hash, item_id)
    ).fetchone()
 
    if ban:
        banned_at      = datetime.fromisoformat(ban['banned_at'])
        days_elapsed   = (datetime.now() - banned_at).days
        attempted_price = ban['attempted_price']
 
        if days_elapsed >= 30:
            # Verifică dacă piața a confirmat prețul (min 20 prețuri, deviere ≤10%)
            market = conn.execute(
                'SELECT AVG(price), COUNT(*) FROM voluntary_prices WHERE item_id=?',
                (item_id,)
            ).fetchone()
            market_avg   = market[0]
            market_count = market[1]
 
            if (market_count >= 20
                    and market_avg is not None
                    and abs(market_avg - attempted_price) / attempted_price <= 0.10):
                # Piața a ajuns din urmă — deblocare
                conn.execute('UPDATE ip_bans SET active=0 WHERE id=?', (ban['id'],))
                conn.commit()
                # Continuă procesarea submisiei curente
            else:
                conn.close()
                if market_count < 20:
                    detail = f"piața are doar {market_count}/20 prețuri necesare pentru deblocare"
                else:
                    detail = (f"media pieței ({market_avg:.2f} RON) nu a confirmat "
                              f"prețul tău ({attempted_price:.2f} RON)")
                return False, (
                    f"🔒 Blocat — {detail}. "
                    f"Vei fi deblocat când piața (min. 20 contribuții) validează prețul tău."
                )
        else:
            remaining = 30 - days_elapsed
            conn.close()
            return False, f"🔒 Blocat {remaining} zile rămase din cauza tentativelor repetate."
 
    # ── 2. Recuperează ultimul preț acceptat al acestui IP ─────────────────
    last_row = conn.execute(
        '''SELECT price FROM voluntary_prices
           WHERE ip_hash=? AND item_id=?
           ORDER BY created_at DESC LIMIT 1''',
        (ip_hash, item_id)
    ).fetchone()
 
    if last_row is None:
        conn.close()
        return True, None   # Prima submisie — orice preț e OK
 
    last_price = last_row[0]
    deviation  = abs(new_price - last_price) / last_price
 
    # ── 3. Numără violările anterioare pentru acest IP+item ────────────────
    violation_count = conn.execute(
        'SELECT COUNT(*) FROM ip_violations WHERE ip_hash=? AND item_id=?',
        (ip_hash, item_id)
    ).fetchone()[0]
 
    # Prag dinamic: 10% la prima abatere, 5% la următoarele
    threshold = 0.10 if violation_count == 0 else 0.05
 
    if deviation <= threshold:
        conn.close()
        return True, None   # În limita admisă
 
    # ── 4. Înregistrează violarea ──────────────────────────────────────────
    conn.execute(
        '''INSERT INTO ip_violations
           (ip_hash, item_id, attempted_price, last_price, deviation_pct, week_date)
           VALUES (?, ?, ?, ?, ?, ?)''',
        (ip_hash, item_id, new_price, last_price, round(deviation * 100, 2), get_week_date())
    )
    conn.commit()
    new_violation_count = violation_count + 1
 
    # ── 5. A 3-a violara → ban 30 zile ────────────────────────────────────
    if new_violation_count >= 3:
        conn.execute(
            '''INSERT INTO ip_bans (ip_hash, item_id, attempted_price)
               VALUES (?, ?, ?)''',
            (ip_hash, item_id, new_price)
        )
        conn.commit()
        conn.close()
        return False, (
            "🚫 Ai fost blocat 30 de zile din cauza tentativelor repetate de "
            "manipulare a prețului. Vei fi deblocat anticipat dacă piața "
            "(min. 20 contribuții) ajunge la prețul pe care l-ai propus."
        )
 
    # ── 6. Avertisment cu tentative rămase ────────────────────────────────
    low              = round(last_price * (1 - threshold), 2)
    high             = round(last_price * (1 + threshold), 2)
    attempts_left    = 3 - new_violation_count
    threshold_label  = "10%" if violation_count == 0 else "5%"
    conn.close()
    return False, (
        f"⚠️ Variație de {deviation*100:.1f}% față de ultimul tău preț "
        f"({last_price:.2f} RON) depășește pragul de {threshold_label}. "
        f"Interval acceptat: {low} – {high} RON. "
        f"{'Atenție: ' + str(attempts_left) + ' tentative rămase înainte de blocare 30 zile.' if attempts_left <= 2 else ''}"
    )
 
def get_client_ip():
    ip = request.headers.get('X-Forwarded-For', request.remote_addr) or '0.0.0.0'
    return ip.split(',')[0].strip()
 
# ─── ROUTES ──────────────────────────────────────────────────────────────────
 
@app.route('/')
def index():
    conn = get_db()
    items = conn.execute('SELECT * FROM items ORDER BY category, name').fetchall()
    conn.close()
    return render_template('index.html', items=items)
 
 
@app.route('/api/items')
def api_items():
    conn = get_db()
    items = conn.execute('SELECT * FROM items ORDER BY category, name').fetchall()
    result = []
    for item in items:
        op = conn.execute(
            'SELECT AVG(price) FROM online_prices WHERE item_id=?', (item['id'],)
        ).fetchone()[0]
        vp = conn.execute(
            'SELECT AVG(price) FROM voluntary_prices WHERE item_id=?', (item['id'],)
        ).fetchone()[0]
        vc = conn.execute(
            'SELECT COUNT(*) FROM voluntary_prices WHERE item_id=?', (item['id'],)
        ).fetchone()[0]
        result.append({
            'id':              item['id'],
            'name':            item['name'],
            'category':        item['category'],
            'unit':            item['unit'],
            'online_price':    round(op, 2) if op else None,
            'voluntary_price': round(vp, 2) if vp else None,
            'voluntary_count': vc,
        })
    conn.close()
    return jsonify(result)
 
 
@app.route('/api/chart-data')
def api_chart_data():
    """Agregare lunară pe 3 ani pentru grafic."""
    conn = get_db()
    now = datetime.now()
    labels, online_vals, voluntary_vals = [], [], []
 
    for i in range(35, -1, -1):
        d = now - timedelta(days=i * 30)
        labels.append(d.strftime('%b %Y'))
        yr = d.strftime('%G')
        # weeks in this approximate month
        w_start = int((d - timedelta(days=15)).strftime('%V'))
        w_end   = int((d + timedelta(days=15)).strftime('%V'))
 
        def avg_in_month(table):
            rows = conn.execute(
                f"SELECT price FROM {table} WHERE week_date LIKE ?", (f"{yr}-W%",)
            ).fetchall()
            prices = [r[0] for r in rows]
            return round(sum(prices) / len(prices), 2) if prices else None
 
        online_vals.append(avg_in_month('online_prices'))
        voluntary_vals.append(avg_in_month('voluntary_prices'))
 
    conn.close()
    return jsonify({'labels': labels, 'online': online_vals, 'voluntary': voluntary_vals})
 
 
@app.route('/api/submit-price', methods=['POST'])
def submit_price():
    data = request.get_json(silent=True) or {}
    item_id = data.get('item_id')
    price   = data.get('price')
 
    if not item_id or not price:
        return jsonify({'error': 'Date lipsă (item_id, price)'}), 400
    try:
        item_id = int(item_id)
        price   = float(price)
    except (ValueError, TypeError):
        return jsonify({'error': 'Valori invalide'}), 400
    if price <= 0:
        return jsonify({'error': 'Prețul trebuie să fie > 0'}), 400
 
    ip_hash = hash_ip(get_client_ip())
 
    if is_rate_limited(ip_hash, item_id):
        return jsonify({'error': '⚠️ Ai înregistrat deja un preț pentru acest item săptămâna aceasta.'}), 429
 
    allowed, err_msg = check_and_enforce_rules(ip_hash, item_id, price)
    if not allowed:
        return jsonify({'error': err_msg}), 422
 
    conn = get_db()
    conn.execute(
        'INSERT INTO voluntary_prices (item_id,price,ip_hash,week_date) VALUES (?,?,?,?)',
        (item_id, price, ip_hash, get_week_date())
    )
    conn.commit()
    conn.close()
    return jsonify({'success': True, 'message': '✅ Preț înregistrat! Mulțumim pentru contribuție.'})
 
 
@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'Niciun fișier trimis'}), 400
    file = request.files['file']
    if not file.filename:
        return jsonify({'error': 'Fișier fără nume'}), 400
 
    fname    = file.filename.lower()
    ip_hash  = hash_ip(get_client_ip())
    week     = get_week_date()
    imported = 0
    errors   = []
    conn     = get_db()
 
    def try_import(name_raw, price_raw):
        nonlocal imported
        try:
            price = float(str(price_raw).replace('RON','').replace('lei','').replace(' ','').replace(',','.'))
        except (ValueError, TypeError):
            errors.append(f"Preț invalid: {price_raw}")
            return
        item = conn.execute(
            'SELECT id FROM items WHERE name LIKE ?', (f'%{str(name_raw).strip()}%',)
        ).fetchone()
        if not item:
            errors.append(f"Produs negăsit: {name_raw}")
            return
        if not is_rate_limited(ip_hash, item['id']):
            allowed, err_msg = check_and_enforce_rules(ip_hash, item['id'], price)
            if not allowed:
                errors.append(f"{err_msg} | produs: {name_raw}")
            else:
                conn.execute(
                    'INSERT INTO voluntary_prices (item_id,price,ip_hash,week_date) VALUES (?,?,?,?)',
                    (item['id'], price, ip_hash, week)
                )
                imported += 1
 
    try:
        if fname.endswith(('.xlsx', '.xls')):
            wb = openpyxl.load_workbook(BytesIO(file.read()), data_only=True)
            ws = wb.active
            for row in ws.iter_rows(min_row=2, values_only=True):
                if row[0] and row[1]:
                    try_import(row[0], row[1])
 
        elif fname.endswith('.pdf'):
            with pdfplumber.open(BytesIO(file.read())) as pdf:
                for page in pdf.pages:
                    for table in (page.extract_tables() or []):
                        for row in table[1:]:
                            if row and len(row) >= 2 and row[0] and row[1]:
                                try_import(row[0], row[1])
        else:
            return jsonify({'error': 'Tip de fișier nesupportat. Folosiți .xlsx sau .pdf'}), 400
 
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({'error': f'Eroare la procesare: {str(e)}'}), 500
    finally:
        conn.close()
 
    return jsonify({'success': True, 'imported': imported, 'errors': errors[:10]})
 
 
# ─── STARTUP ─────────────────────────────────────────────────────────────────
 
init_db()
 
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
