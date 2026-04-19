"""
db.py — Schema PostgreSQL si toate operatiunile CRUD.
Include logica anti-manipulare si indexarea ierarhica pe 3 niveluri.
"""

import os
import psycopg2
import psycopg2.extras
import hashlib
from datetime import datetime, timedelta
from parser import trimmed_mean

DATABASE_URL = os.environ.get('DATABASE_URL', '')

# ─── CONEXIUNE ────────────────────────────────────────────────────────────────

def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    return conn

def _cur(conn):
    """Cursor care returneaza randuri ca dictionare."""
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

def _row(row):
    """Converteste un rand din DB la dict, cu datetime -> ISO string."""
    result = {}
    for k, v in dict(row).items():
        if isinstance(v, datetime):
            result[k] = v.isoformat()
        else:
            result[k] = v
    return result


# ─── INITIALIZARE SCHEMA ──────────────────────────────────────────────────────

def init_db():
    conn = get_db()
    c = _cur(conn)

    for stmt in [
        """CREATE TABLE IF NOT EXISTS items (
            id              SERIAL PRIMARY KEY,
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
            created_at      TIMESTAMP NOT NULL DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS prices_voluntary (
            id          SERIAL PRIMARY KEY,
            item_id     INTEGER NOT NULL REFERENCES items(id),
            price       REAL NOT NULL,
            county      TEXT,
            ip_hash     TEXT NOT NULL,
            week_number INTEGER NOT NULL,
            year        INTEGER NOT NULL,
            created_at  TIMESTAMP NOT NULL DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS prices_online (
            id           SERIAL PRIMARY KEY,
            item_id      INTEGER NOT NULL REFERENCES items(id),
            price_min    REAL NOT NULL,
            price_max    REAL NOT NULL,
            price_avg    REAL NOT NULL,
            source_name  TEXT,
            source_url   TEXT,
            week_number  INTEGER NOT NULL,
            year         INTEGER NOT NULL,
            created_at   TIMESTAMP NOT NULL DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS ip_violations (
            id              SERIAL PRIMARY KEY,
            ip_hash         TEXT NOT NULL,
            item_id         INTEGER NOT NULL REFERENCES items(id),
            violation_count INTEGER NOT NULL DEFAULT 0,
            banned_until    TIMESTAMP,
            last_price      REAL,
            updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
            UNIQUE(ip_hash, item_id)
        )""",
        """CREATE TABLE IF NOT EXISTS scraping_sources (
            id           SERIAL PRIMARY KEY,
            name         TEXT NOT NULL,
            base_url     TEXT NOT NULL,
            active       INTEGER NOT NULL DEFAULT 1,
            last_scraped TIMESTAMP,
            last_error   TEXT
        )""",
        "CREATE INDEX IF NOT EXISTS idx_prices_vol_item ON prices_voluntary(item_id)",
        "CREATE INDEX IF NOT EXISTS idx_prices_vol_week ON prices_voluntary(week_number, year)",
        "CREATE INDEX IF NOT EXISTS idx_prices_vol_ip   ON prices_voluntary(ip_hash)",
        "CREATE INDEX IF NOT EXISTS idx_prices_onl_item ON prices_online(item_id)",
        "CREATE INDEX IF NOT EXISTS idx_items_level1    ON items(level1_key)",
        "CREATE INDEX IF NOT EXISTS idx_items_level2    ON items(level2_key)",
        "CREATE INDEX IF NOT EXISTS idx_violations_ip   ON ip_violations(ip_hash, item_id)",
    ]:
        c.execute(stmt)

    # Populeaza surse daca nu exista
    c.execute("SELECT COUNT(*) as cnt FROM scraping_sources")
    if c.fetchone()['cnt'] == 0:
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
            "INSERT INTO scraping_sources (name, base_url) VALUES (%s, %s)",
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
    c = _cur(conn)
    c.execute(
        "SELECT id FROM items WHERE canonical_key = %s",
        (keys['canonical_key'],)
    )
    row = c.fetchone()

    if row:
        conn.close()
        return row['id'], False  # (id, was_created)

    c.execute("""
        INSERT INTO items
            (species, category, unit, height_bucket, diameter_bucket,
             circ_bucket, root_type, clt_size, canonical_key,
             level1_key, level2_key, display_name)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id
    """, (
        keys['species'], keys['category'], keys['unit'],
        keys['height_bucket'], keys['diameter_bucket'], keys['circ_bucket'],
        keys['root_type'], keys['clt_size'], keys['canonical_key'],
        keys['level1_key'], keys['level2_key'], keys['display_name']
    ))
    item_id = c.fetchone()['id']
    conn.commit()
    conn.close()
    return item_id, True


def get_all_items():
    conn = get_db()
    c = _cur(conn)
    c.execute("SELECT * FROM items ORDER BY category, species, height_bucket")
    rows = c.fetchall()
    conn.close()
    return [_row(r) for r in rows]


def delete_item(item_id):
    conn = get_db()
    c = _cur(conn)
    c.execute("DELETE FROM prices_voluntary WHERE item_id = %s", (item_id,))
    c.execute("DELETE FROM prices_online WHERE item_id = %s", (item_id,))
    c.execute("DELETE FROM ip_violations WHERE item_id = %s", (item_id,))
    c.execute("DELETE FROM items WHERE id = %s", (item_id,))
    conn.commit()
    conn.close()


# ─── ANTI-MANIPULARE ──────────────────────────────────────────────────────────

def check_manipulation(ip_hash, item_id, new_price):
    """
    Verifica daca submiterea respecta regulile anti-manipulare.
    Returneaza (ok: bool, message: str).
    """
    conn = get_db()
    c = _cur(conn)

    c.execute(
        "SELECT * FROM ip_violations WHERE ip_hash = %s AND item_id = %s",
        (ip_hash, item_id)
    )
    row = c.fetchone()

    # Verifica ban activ
    if row and row['banned_until']:
        banned_until_dt = row['banned_until']  # psycopg2 returneaza datetime direct
        if datetime.utcnow() < banned_until_dt:
            # Verifica deblocare prin 20 confirmari
            c.execute(
                "SELECT price FROM prices_voluntary WHERE item_id = %s",
                (item_id,)
            )
            prices_list = [r['price'] for r in c.fetchall()]
            if len(prices_list) >= 20:
                close = [p for p in prices_list if abs(p - new_price) / new_price <= 0.10]
                if len(close) >= 20:
                    c.execute(
                        "UPDATE ip_violations SET banned_until=NULL, violation_count=0 WHERE ip_hash=%s AND item_id=%s",
                        (ip_hash, item_id)
                    )
                    conn.commit()
                else:
                    days_left = (banned_until_dt - datetime.utcnow()).days + 1
                    conn.close()
                    return False, f"Esti blocat {days_left} zile. Piata nu a confirmat inca pretul tau."
            else:
                days_left = (banned_until_dt - datetime.utcnow()).days + 1
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
        banned_until = datetime.utcnow() + timedelta(days=30)
        c.execute("""
            INSERT INTO ip_violations (ip_hash, item_id, violation_count, banned_until, last_price)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT(ip_hash, item_id) DO UPDATE SET
                violation_count=EXCLUDED.violation_count,
                banned_until=EXCLUDED.banned_until,
                updated_at=NOW()
        """, (ip_hash, item_id, new_violations, banned_until, last_price))
        conn.commit()
        conn.close()
        return False, "A 3-a abatere. Ai fost blocat 30 de zile."

    c.execute("""
        INSERT INTO ip_violations (ip_hash, item_id, violation_count, last_price)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT(ip_hash, item_id) DO UPDATE SET
            violation_count=EXCLUDED.violation_count,
            updated_at=NOW()
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
    bulk=True: sare peste limita 1/saptamana.
    Returneaza (ok: bool, message: str).
    """
    week, year = current_week()
    conn = get_db()
    c = _cur(conn)

    if not bulk:
        c.execute("""
            SELECT id FROM prices_voluntary
            WHERE item_id=%s AND ip_hash=%s AND week_number=%s AND year=%s
        """, (item_id, ip_hash, week, year))
        if c.fetchone():
            conn.close()
            return False, "Ai contribuit deja pentru acest produs saptamana aceasta."

    conn.close()

    # Anti-manipulare
    ok, msg = check_manipulation(ip_hash, item_id, price)
    if not ok:
        return False, msg

    # Salveaza pretul
    conn = get_db()
    c = _cur(conn)
    c.execute("""
        INSERT INTO prices_voluntary (item_id, price, county, ip_hash, week_number, year)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, (item_id, price, county, ip_hash, week, year))

    # Actualizeaza last_price in violations
    c.execute("""
        INSERT INTO ip_violations (ip_hash, item_id, violation_count, last_price)
        VALUES (%s,%s,0,%s)
        ON CONFLICT(ip_hash, item_id) DO UPDATE SET
            last_price=EXCLUDED.last_price,
            updated_at=NOW()
    """, (ip_hash, item_id, price))

    conn.commit()
    conn.close()
    return True, "Pret inregistrat cu succes!"


def get_voluntary_prices_for_chart(level1_key, county=None):
    """
    Returneaza saptamanal media trunchiata pentru grafic.
    """
    conn = get_db()
    c = _cur(conn)

    if county and county != 'national':
        c.execute("""
            SELECT pv.week_number, pv.year, pv.price
            FROM prices_voluntary pv
            JOIN items i ON i.id = pv.item_id
            WHERE i.level1_key = %s AND pv.county = %s
            ORDER BY pv.year, pv.week_number
        """, (level1_key, county))
    else:
        c.execute("""
            SELECT pv.week_number, pv.year, pv.price
            FROM prices_voluntary pv
            JOIN items i ON i.id = pv.item_id
            WHERE i.level1_key = %s
            ORDER BY pv.year, pv.week_number
        """, (level1_key,))

    rows = c.fetchall()
    conn.close()

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
    c = _cur(conn)
    c.execute("""
        INSERT INTO prices_online
            (item_id, price_min, price_max, price_avg, source_name, source_url, week_number, year)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, (item_id, price_min, price_max, price_avg, source_name, source_url, week, year))
    conn.commit()
    conn.close()


def get_online_prices_for_chart(level1_key):
    conn = get_db()
    c = _cur(conn)
    c.execute("""
        SELECT po.week_number, po.year,
               po.price_min, po.price_max, po.price_avg,
               po.source_name, po.source_url
        FROM prices_online po
        JOIN items i ON i.id = po.item_id
        WHERE i.level1_key = %s
        ORDER BY po.year, po.week_number
    """, (level1_key,))
    rows = c.fetchall()
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
            'label':   k,
            'price':   trimmed_mean(v['avgs']),
            'min':     min(v['mins']),
            'max':     max(v['maxs']),
            'sources': v['sources'],
            'count':   len(v['avgs']),
        })
    return result


# ─── STATISTICI ───────────────────────────────────────────────────────────────

def get_total_prices_count():
    conn = get_db()
    c = _cur(conn)
    c.execute("SELECT COUNT(*) as cnt FROM prices_voluntary")
    vol = c.fetchone()['cnt']
    c.execute("SELECT COUNT(*) as cnt FROM prices_online")
    onl = c.fetchone()['cnt']
    conn.close()
    return vol + onl


def get_county_stats(item_level1_key):
    conn = get_db()
    c = _cur(conn)
    c.execute("""
        SELECT pv.county, COUNT(*) as cnt, AVG(pv.price) as avg_price
        FROM prices_voluntary pv
        JOIN items i ON i.id = pv.item_id
        WHERE i.level1_key = %s AND pv.county IS NOT NULL
        GROUP BY pv.county
        ORDER BY cnt DESC
        LIMIT 10
    """, (item_level1_key,))
    rows = c.fetchall()
    conn.close()
    return [_row(r) for r in rows]


# ─── ADMIN ────────────────────────────────────────────────────────────────────

def delete_price(price_id, source='voluntary'):
    conn = get_db()
    c = _cur(conn)
    table = 'prices_voluntary' if source == 'voluntary' else 'prices_online'
    c.execute(f"DELETE FROM {table} WHERE id = %s", (price_id,))
    conn.commit()
    conn.close()


def delete_prices_for_week(item_id, week_number, year, source='voluntary'):
    conn = get_db()
    c = _cur(conn)
    table = 'prices_voluntary' if source == 'voluntary' else 'prices_online'
    c.execute(
        f"DELETE FROM {table} WHERE item_id=%s AND week_number=%s AND year=%s",
        (item_id, week_number, year)
    )
    conn.commit()
    conn.close()


def get_banned_ips():
    conn = get_db()
    c = _cur(conn)
    c.execute("""
        SELECT iv.ip_hash, iv.violation_count, iv.banned_until, iv.updated_at,
               i.display_name as item_name
        FROM ip_violations iv
        JOIN items i ON i.id = iv.item_id
        WHERE iv.banned_until IS NOT NULL
          AND iv.banned_until > NOW()
        ORDER BY iv.updated_at DESC
    """)
    rows = c.fetchall()
    conn.close()
    return [_row(r) for r in rows]


def unban_ip(ip_hash, item_id):
    conn = get_db()
    c = _cur(conn)
    c.execute(
        "UPDATE ip_violations SET banned_until=NULL, violation_count=0 WHERE ip_hash=%s AND item_id=%s",
        (ip_hash, item_id)
    )
    conn.commit()
    conn.close()


def get_scraping_sources():
    conn = get_db()
    c = _cur(conn)
    c.execute("SELECT * FROM scraping_sources ORDER BY name")
    rows = c.fetchall()
    conn.close()
    return [_row(r) for r in rows]


def update_source_status(source_id, last_scraped=None, last_error=None):
    conn = get_db()
    c = _cur(conn)
    if last_error:
        c.execute(
            "UPDATE scraping_sources SET last_scraped=%s, last_error=%s WHERE id=%s",
            (last_scraped, last_error, source_id)
        )
    else:
        c.execute(
            "UPDATE scraping_sources SET last_scraped=%s, last_error=NULL WHERE id=%s",
            (last_scraped, source_id)
        )
    conn.commit()
    conn.close()


# ─── NAVIGARE CASCADA ─────────────────────────────────────────────────────────

def _parse_species(species):
    """Desparte numele speciei in gen, specie completa, varietate."""
    parts = (species or '').split()
    genus    = parts[0] if parts else '—'
    sp_full  = ' '.join(parts[:2]) if len(parts) >= 2 else (parts[0] if parts else '—')
    variety  = ' '.join(parts[2:]) if len(parts) > 2 else '(specie)'
    return genus, sp_full, variety


def get_cascade_tree():
    """
    Returneaza arborele botanic pentru navigare in cascada:
    { category: { genus: { species_full: { variety: [{ canonical_key, level1_key, sp_key, height_bucket, price_count }] } } } }
    """
    conn = get_db()
    c = _cur(conn)
    c.execute("""
        SELECT
            i.category, i.species,
            i.canonical_key, i.level1_key,
            i.height_bucket,
            COUNT(pv.id) AS price_count
        FROM items i
        LEFT JOIN prices_voluntary pv ON pv.item_id = i.id
        GROUP BY i.category, i.species, i.canonical_key, i.level1_key, i.height_bucket
        ORDER BY i.category, i.species, i.height_bucket
    """)
    rows = c.fetchall()
    conn.close()

    tree = {}
    for r in rows:
        cat             = r['category'] or 'Necunoscut'
        genus, sp, var  = _parse_species(r['species'])
        hb              = r['height_bucket'] or 'generic'
        sp_key          = r['level1_key'].split('|')[0] if r['level1_key'] else ''

        tree.setdefault(cat, {})
        tree[cat].setdefault(genus, {})
        tree[cat][genus].setdefault(sp, {})
        tree[cat][genus][sp].setdefault(var, [])
        tree[cat][genus][sp][var].append({
            'canonical_key': r['canonical_key'],
            'level1_key':    r['level1_key'],
            'sp_key':        sp_key,
            'height_bucket': hb,
            'price_count':   r['price_count'],
        })

    return tree


# ─── MATRICE PRETURI ──────────────────────────────────────────────────────────

def get_price_matrix(species_key):
    """
    Pentru o specie/varietate (species_key = level1_key sau canonical_key prefix),
    returneaza preturile grupate pe dimensiuni cu trend fata de saptamana precedenta.
    """
    conn = get_db()
    c = _cur(conn)
    week, year = current_week()
    prev_week  = week - 1 if week > 1 else 52
    prev_year  = year if week > 1 else year - 1

    # Preturi curente (saptamana aceasta + precedenta)
    c.execute("""
        SELECT
            i.height_bucket,
            i.canonical_key,
            i.display_name,
            AVG(CASE WHEN pv.year=%s AND pv.week_number=%s THEN pv.price END) AS avg_cur,
            AVG(CASE WHEN pv.year=%s AND pv.week_number=%s THEN pv.price END) AS avg_prev,
            AVG(pv.price)   AS avg_all,
            MIN(pv.price)   AS min_price,
            MAX(pv.price)   AS max_price,
            COUNT(pv.id)    AS total_count,
            COUNT(CASE WHEN pv.year=%s AND pv.week_number=%s THEN 1 END) AS week_count
        FROM items i
        JOIN prices_voluntary pv ON pv.item_id = i.id
        WHERE i.level1_key LIKE %s OR i.canonical_key LIKE %s
        GROUP BY i.height_bucket, i.canonical_key, i.display_name
        ORDER BY i.height_bucket NULLS LAST
    """, (year, week, prev_year, prev_week, year, week,
          species_key + '%', species_key + '%'))

    rows = c.fetchall()
    conn.close()

    result = []
    for r in rows:
        avg_cur  = r['avg_cur']
        avg_prev = r['avg_prev']
        trend    = None
        if avg_cur and avg_prev:
            trend = round(avg_cur - avg_prev, 2)

        result.append({
            'height_bucket':  r['height_bucket'] or 'generic',
            'canonical_key':  r['canonical_key'],
            'display_name':   r['display_name'],
            'avg_price':      round(r['avg_all'], 2) if r['avg_all'] else None,
            'avg_cur':        round(avg_cur, 2) if avg_cur else None,
            'min_price':      round(r['min_price'], 2) if r['min_price'] else None,
            'max_price':      round(r['max_price'], 2) if r['max_price'] else None,
            'trend':          trend,
            'total_count':    r['total_count'],
            'week_count':     r['week_count'],
        })
    return result
