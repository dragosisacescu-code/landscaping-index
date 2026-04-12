"""
db.py — Schema SQLite si toate operatiunile CRUD.
Include logica anti-manipulare si indexarea ierarhica pe 3 niveluri.
"""

import os
import sqlite3
import hashlib
from datetime import datetime, timedelta
from parser import trimmed_mean

DB_PATH = os.path.join(os.environ.get('DB_DIR', '.'), 'landscaping.db')

# ─── CONEXIUNE ────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ─── INITIALIZARE SCHEMA ──────────────────────────────────────────────────────

def init_db():
    conn = get_db()
    c = conn.cursor()

    c.executescript("""
    CREATE TABLE IF NOT EXISTS items (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        species         TEXT NOT NULL,
        category        TEXT NOT NULL DEFAULT 'Necunoscut',
        unit            TEXT NOT NULL DEFAULT 'buc',
        height_bucket   TEXT,
        diameter_bucket TEXT,
        circ_bucket     TEXT,
        root_type       TEXT,
        clt_size        TEXT,
        canonical_key   TEXT UNIQUE NOT NULL,
        level1_key      TEXT NOT NULL,
        level2_key      TEXT NOT NULL,
        display_name    TEXT NOT NULL,
        created_at      TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS prices_voluntary (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id     INTEGER NOT NULL REFERENCES items(id),
        price       REAL NOT NULL,
        county      TEXT,
        ip_hash     TEXT NOT NULL,
        week_number INTEGER NOT NULL,
        year        INTEGER NOT NULL,
        created_at  TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS prices_online (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id      INTEGER NOT NULL REFERENCES items(id),
        price_min    REAL NOT NULL,
        price_max    REAL NOT NULL,
        price_avg    REAL NOT NULL,
        source_name  TEXT,
        source_url   TEXT,
        week_number  INTEGER NOT NULL,
        year         INTEGER NOT NULL,
        created_at   TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS ip_violations (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        ip_hash        TEXT NOT NULL,
        item_id        INTEGER NOT NULL REFERENCES items(id),
        violation_count INTEGER NOT NULL DEFAULT 0,
        banned_until   TEXT,
        last_price     REAL,
        updated_at     TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(ip_hash, item_id)
    );

    CREATE TABLE IF NOT EXISTS scraping_sources (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        name         TEXT NOT NULL,
        base_url     TEXT NOT NULL,
        active       INTEGER NOT NULL DEFAULT 1,
        last_scraped TEXT,
        last_error   TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_prices_vol_item  ON prices_voluntary(item_id);
    CREATE INDEX IF NOT EXISTS idx_prices_vol_week  ON prices_voluntary(week_number, year);
    CREATE INDEX IF NOT EXISTS idx_prices_vol_ip    ON prices_voluntary(ip_hash);
    CREATE INDEX IF NOT EXISTS idx_prices_onl_item  ON prices_online(item_id);
    CREATE INDEX IF NOT EXISTS idx_items_level1     ON items(level1_key);
    CREATE INDEX IF NOT EXISTS idx_items_level2     ON items(level2_key);
    CREATE INDEX IF NOT EXISTS idx_violations_ip    ON ip_violations(ip_hash, item_id);
    """)

    # Populeaza surse daca nu exista
    existing = c.execute("SELECT COUNT(*) FROM scraping_sources").fetchone()[0]
    if existing == 0:
        sources = [
            ('Hornbach',        'https://www.hornbach.ro'),
            ('Dedeman',         'https://www.dedeman.ro'),
            ('Leroy Merlin',    'https://www.leroymerlin.ro'),
            ('Brico Depot',     'https://www.bricodepot.ro'),
            ('OLX',             'https://www.olx.ro'),
            ('eMAG',            'https://www.emag.ro'),
            ('Planteo',         'https://www.planteo.ro'),
            ('Gradina Max',     'https://www.gradinamax.ro'),
            ('Robakker',        'https://www.robakker.ro'),
            ('Verdena',         'https://www.verdena.ro'),
            ('SweetGarden',     'https://www.sweetgarden.ro'),
            ('Garden Services', 'https://www.gardenservices.ro'),
            ('Parcuri',         'https://www.parcuri.ro'),
            ('Sieberz',         'https://www.sieberz.ro'),
            ('Yurta',           'https://www.yurta.ro'),
            ('Pepiniera Mizil', 'https://www.pepinieram.ro'),
        ]
        c.executemany(
            "INSERT INTO scraping_sources (name, base_url) VALUES (?, ?)",
            sources
        )

    conn.commit()
    conn.close()


# ─── UTILITARE ────────────────────────────────────────────────────────────────

def hash_ip(ip):
    return hashlib.sha256(ip.encode()).hexdigest()[:32]

def current_week():
    now = datetime.utcnow()
    iso = now.isocalendar()
    return iso[1], iso[0]  # (week_number, year)


# ─── ITEMS ────────────────────────────────────────────────────────────────────

def get_or_create_item(keys):
    """
    Cauta item dupa canonical_key.
    Daca nu exista, il creeaza si returneaza id-ul.
    """
    conn = get_db()
    c = conn.cursor()
    row = c.execute(
        "SELECT id FROM items WHERE canonical_key = ?",
        (keys['canonical_key'],)
    ).fetchone()

    if row:
        conn.close()
        return row['id'], False  # (id, was_created)

    c.execute("""
        INSERT INTO items
            (species, category, unit, height_bucket, diameter_bucket,
             circ_bucket, root_type, clt_size, canonical_key,
             level1_key, level2_key, display_name)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        keys['species'], keys['category'], keys['unit'],
        keys['height_bucket'], keys['diameter_bucket'], keys['circ_bucket'],
        keys['root_type'], keys['clt_size'], keys['canonical_key'],
        keys['level1_key'], keys['level2_key'], keys['display_name']
    ))
    item_id = c.lastrowid
    conn.commit()
    conn.close()
    return item_id, True


def get_all_items():
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM items ORDER BY category, species, height_bucket"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def delete_item(item_id):
    conn = get_db()
    conn.execute("DELETE FROM prices_voluntary WHERE item_id = ?", (item_id,))
    conn.execute("DELETE FROM prices_online WHERE item_id = ?", (item_id,))
    conn.execute("DELETE FROM ip_violations WHERE item_id = ?", (item_id,))
    conn.execute("DELETE FROM items WHERE id = ?", (item_id,))
    conn.commit()
    conn.close()


# ─── ANTI-MANIPULARE ──────────────────────────────────────────────────────────

def check_manipulation(ip_hash, item_id, new_price):
    """
    Verifica daca submiterea respecta regulile anti-manipulare.
    Returneaza (ok: bool, message: str).

    Reguli:
    - Prima abatere:        ±10% fata de ultimul pret al aceluiasi IP
    - Abaterile urmatoare:  ±5%
    - A 3-a abatere:        ban 30 zile
    - Deblocare anticipata: minim 20 preturi confirma noul pret (±10%)
    """
    conn = get_db()
    c = conn.cursor()

    row = c.execute(
        "SELECT * FROM ip_violations WHERE ip_hash = ? AND item_id = ?",
        (ip_hash, item_id)
    ).fetchone()

    # Verifica ban activ
    if row and row['banned_until']:
        banned_until = datetime.fromisoformat(row['banned_until'])
        if datetime.utcnow() < banned_until:
            # Verifica deblocare prin 20 confirmari
            all_prices = c.execute(
                "SELECT price FROM prices_voluntary WHERE item_id = ?",
                (item_id,)
            ).fetchall()
            prices_list = [r['price'] for r in all_prices]
            if len(prices_list) >= 20:
                close = [p for p in prices_list if abs(p - new_price) / new_price <= 0.10]
                if len(close) >= 20:
                    # Deblocam
                    c.execute(
                        "UPDATE ip_violations SET banned_until=NULL, violation_count=0 WHERE ip_hash=? AND item_id=?",
                        (ip_hash, item_id)
                    )
                    conn.commit()
                else:
                    days_left = (banned_until - datetime.utcnow()).days + 1
                    conn.close()
                    return False, f"Esti blocat {days_left} zile. Piata nu a confirmat inca pretul tau."
            else:
                days_left = (banned_until - datetime.utcnow()).days + 1
                conn.close()
                return False, f"Esti blocat {days_left} zile din cauza variatiilor prea mari."

    # Nu exista istoric pentru acest IP+item → prima submitere, libera
    if not row or row['last_price'] is None:
        conn.close()
        return True, "OK"

    last_price  = row['last_price']
    violations  = row['violation_count']
    threshold   = 0.10 if violations == 0 else 0.05
    deviation   = abs(new_price - last_price) / last_price

    if deviation <= threshold:
        conn.close()
        return True, "OK"

    # Abatere detectata
    new_violations = violations + 1
    if new_violations >= 3:
        banned_until = (datetime.utcnow() + timedelta(days=30)).isoformat()
        c.execute("""
            INSERT INTO ip_violations (ip_hash, item_id, violation_count, banned_until, last_price)
            VALUES (?,?,?,?,?)
            ON CONFLICT(ip_hash, item_id) DO UPDATE SET
                violation_count=excluded.violation_count,
                banned_until=excluded.banned_until,
                updated_at=datetime('now')
        """, (ip_hash, item_id, new_violations, banned_until, last_price))
        conn.commit()
        conn.close()
        return False, "A 3-a abatere. Ai fost blocat 30 de zile."

    c.execute("""
        INSERT INTO ip_violations (ip_hash, item_id, violation_count, last_price)
        VALUES (?,?,?,?)
        ON CONFLICT(ip_hash, item_id) DO UPDATE SET
            violation_count=excluded.violation_count,
            updated_at=datetime('now')
    """, (ip_hash, item_id, new_violations, last_price))
    conn.commit()
    conn.close()

    low  = round(last_price * (1 - threshold), 2)
    high = round(last_price * (1 + threshold), 2)
    return False, (
        f"Variatie prea mare fata de pretul tau anterior ({last_price:.2f} RON). "
        f"Intervalul acceptat: {low:.2f} – {high:.2f} RON. "
        f"Abatere {new_violations}/3."
    )


# ─── PRETURI VOLUNTARE ────────────────────────────────────────────────────────

def add_voluntary_price(item_id, price, county, ip_hash, bulk=False):
    """
    Adauga un pret voluntar dupa validarea regulii 1/saptamana/IP
    si a regulii anti-manipulare.
    bulk=True: sare peste limita 1/saptamana (folosit la upload Excel in masa).
    Returneaza (ok: bool, message: str).
    """
    week, year = current_week()
    conn = get_db()
    c = conn.cursor()

    if not bulk:
        # Regula 1 actiune / item / saptamana / IP
        existing = c.execute("""
            SELECT id FROM prices_voluntary
            WHERE item_id=? AND ip_hash=? AND week_number=? AND year=?
        """, (item_id, ip_hash, week, year)).fetchone()

        if existing:
            conn.close()
            return False, "Ai contribuit deja pentru acest produs saptamana aceasta."

    conn.close()

    # Anti-manipulare
    ok, msg = check_manipulation(ip_hash, item_id, price)
    if not ok:
        return False, msg

    # Salveaza pretul
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        INSERT INTO prices_voluntary (item_id, price, county, ip_hash, week_number, year)
        VALUES (?,?,?,?,?,?)
    """, (item_id, price, county, ip_hash, week, year))

    # Actualizeaza last_price in violations
    c.execute("""
        INSERT INTO ip_violations (ip_hash, item_id, violation_count, last_price)
        VALUES (?,?,0,?)
        ON CONFLICT(ip_hash, item_id) DO UPDATE SET
            last_price=excluded.last_price,
            updated_at=datetime('now')
    """, (ip_hash, item_id, price))

    conn.commit()
    conn.close()
    return True, "Pret inregistrat cu succes!"


def get_voluntary_prices_for_chart(level1_key, county=None):
    """
    Returneaza saptamanal media trunchiata pentru grafic.
    level: 'national' sau judet specific.
    """
    conn = get_db()
    c = conn.cursor()

    if county and county != 'national':
        rows = c.execute("""
            SELECT pv.week_number, pv.year, pv.price
            FROM prices_voluntary pv
            JOIN items i ON i.id = pv.item_id
            WHERE i.level1_key = ? AND pv.county = ?
            ORDER BY pv.year, pv.week_number
        """, (level1_key, county)).fetchall()
    else:
        rows = c.execute("""
            SELECT pv.week_number, pv.year, pv.price
            FROM prices_voluntary pv
            JOIN items i ON i.id = pv.item_id
            WHERE i.level1_key = ?
            ORDER BY pv.year, pv.week_number
        """, (level1_key,)).fetchall()

    conn.close()

    # Grupeaza pe saptamana
    weeks = {}
    for r in rows:
        key = f"{r['year']}-W{r['week_number']:02d}"
        weeks.setdefault(key, []).append(r['price'])

    return [
        {'label': k, 'price': trimmed_mean(v), 'count': len(v)}
        for k, v in sorted(weeks.items())
    ]


# ─── PRETURI ONLINE ───────────────────────────────────────────────────────────

def add_online_price(item_id, price_min, price_max, price_avg, source_name, source_url):
    week, year = current_week()
    conn = get_db()
    conn.execute("""
        INSERT INTO prices_online
            (item_id, price_min, price_max, price_avg, source_name, source_url, week_number, year)
        VALUES (?,?,?,?,?,?,?,?)
    """, (item_id, price_min, price_max, price_avg, source_name, source_url, week, year))
    conn.commit()
    conn.close()


def get_online_prices_for_chart(level1_key):
    """
    Returneaza saptamanal min/max/medie din toate sursele.
    """
    conn = get_db()
    rows = conn.execute("""
        SELECT po.week_number, po.year,
               po.price_min, po.price_max, po.price_avg,
               po.source_name, po.source_url
        FROM prices_online po
        JOIN items i ON i.id = po.item_id
        WHERE i.level1_key = ?
        ORDER BY po.year, po.week_number
    """, (level1_key,)).fetchall()
    conn.close()

    weeks = {}
    for r in rows:
        key = f"{r['year']}-W{r['week_number']:02d}"
        weeks.setdefault(key, {
            'mins': [], 'maxs': [], 'avgs': [],
            'sources': []
        })
        weeks[key]['mins'].append(r['price_min'])
        weeks[key]['maxs'].append(r['price_max'])
        weeks[key]['avgs'].append(r['price_avg'])
        weeks[key]['sources'].append({
            'name': r['source_name'],
            'url':  r['source_url'],
            'min':  r['price_min'],
            'max':  r['price_max'],
        })

    result = []
    for k, v in sorted(weeks.items()):
        result.append({
            'label':    k,
            'price':    trimmed_mean(v['avgs']),
            'min':      min(v['mins']),
            'max':      max(v['maxs']),
            'sources':  v['sources'],
            'count':    len(v['avgs']),
        })
    return result


# ─── STATISTICI ───────────────────────────────────────────────────────────────

def get_total_prices_count():
    conn = get_db()
    vol = conn.execute("SELECT COUNT(*) FROM prices_voluntary").fetchone()[0]
    onl = conn.execute("SELECT COUNT(*) FROM prices_online").fetchone()[0]
    conn.close()
    return vol + onl


def get_county_stats(item_level1_key):
    """Top judete dupa numar de contributii pentru un item."""
    conn = get_db()
    rows = conn.execute("""
        SELECT pv.county, COUNT(*) as cnt, AVG(pv.price) as avg_price
        FROM prices_voluntary pv
        JOIN items i ON i.id = pv.item_id
        WHERE i.level1_key = ? AND pv.county IS NOT NULL
        GROUP BY pv.county
        ORDER BY cnt DESC
        LIMIT 10
    """, (item_level1_key,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─── ADMIN ────────────────────────────────────────────────────────────────────

def delete_price(price_id, source='voluntary'):
    conn = get_db()
    table = 'prices_voluntary' if source == 'voluntary' else 'prices_online'
    conn.execute(f"DELETE FROM {table} WHERE id = ?", (price_id,))
    conn.commit()
    conn.close()


def delete_prices_for_week(item_id, week_number, year, source='voluntary'):
    conn = get_db()
    table = 'prices_voluntary' if source == 'voluntary' else 'prices_online'
    conn.execute(
        f"DELETE FROM {table} WHERE item_id=? AND week_number=? AND year=?",
        (item_id, week_number, year)
    )
    conn.commit()
    conn.close()


def get_banned_ips():
    conn = get_db()
    rows = conn.execute("""
        SELECT iv.ip_hash, iv.violation_count, iv.banned_until, iv.updated_at,
               i.display_name as item_name
        FROM ip_violations iv
        JOIN items i ON i.id = iv.item_id
        WHERE iv.banned_until IS NOT NULL
          AND iv.banned_until > datetime('now')
        ORDER BY iv.updated_at DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def unban_ip(ip_hash, item_id):
    conn = get_db()
    conn.execute(
        "UPDATE ip_violations SET banned_until=NULL, violation_count=0 WHERE ip_hash=? AND item_id=?",
        (ip_hash, item_id)
    )
    conn.commit()
    conn.close()


def get_scraping_sources():
    conn = get_db()
    rows = conn.execute("SELECT * FROM scraping_sources ORDER BY name").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_source_status(source_id, last_scraped=None, last_error=None):
    conn = get_db()
    if last_error:
        conn.execute(
            "UPDATE scraping_sources SET last_scraped=?, last_error=? WHERE id=?",
            (last_scraped, last_error, source_id)
        )
    else:
        conn.execute(
            "UPDATE scraping_sources SET last_scraped=?, last_error=NULL WHERE id=?",
            (last_scraped, source_id)
        )
    conn.commit()
    conn.close()
