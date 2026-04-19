"""
parser.py — Agent AI pentru parsare descrieri de plante/materiale.
Acelasi flux pentru input manual, scraping si upload fisiere.
"""

import os
import json

# ─── BUCKETS FIXE ─────────────────────────────────────────────────────────────

HEIGHT_BUCKETS = [
    (0, 50), (50, 100), (100, 150), (150, 200),
    (200, 250), (250, 300), (300, 400), (400, 500)
]
DIAMETER_BUCKETS = [
    (0, 10), (10, 20), (20, 30), (30, 40),
    (40, 60), (60, 80), (80, 100)
]
CIRCUMFERENCE_BUCKETS = [
    (0, 8), (8, 10), (10, 12), (12, 14), (14, 16),
    (16, 18), (18, 20), (20, 25), (25, 30),
    (30, 40), (40, 50), (50, 60)
]

# Categorii cu TVA redus (plante) vs standard (materiale/servicii)
VAT_RATES = {
    'Conifere':         0.11,
    'Arbori':           0.11,
    'Arbusti':          0.11,
    'Plante_taratoare': 0.11,
    'Gazon':            0.11,
    'Materiale':        0.21,
    'Piatra':           0.21,
    'Manopera':         0.21,
    'Necunoscut':       0.11,
}

VALID_CATEGORIES = list(VAT_RATES.keys())


def assign_bucket(value, buckets):
    """Atribuie o valoare bucket-ului standard corespunzator."""
    if value is None:
        return None
    for low, high in buckets:
        if low <= value < high:
            return f"{low}-{high}cm"
    return f"{buckets[-1][1]}cm+"


def deduct_vat(price, category, vat_included=None):
    """
    Deduce TVA din pret daca nu e specificat altfel.
    Plante: 11%, Materiale: 21%.
    vat_included=True  → deduce TVA
    vat_included=False → pretul e deja fara TVA
    vat_included=None  → presupunem ca include TVA
    """
    if vat_included is False:
        return round(price, 2)
    rate = VAT_RATES.get(category, 0.11)
    return round(price / (1 + rate), 2)


def trimmed_mean(prices):
    """
    Medie aritmetica trunchiata scalata:
    1-3   → medie simpla
    4-9   → eliminam 1 min + 1 max
    10-19 → eliminam 2 min + 2 max
    20-29 → eliminam 4 min + 4 max
    30+   → continuam logic (+2 de fiecare parte la fiecare 10 preturi)
    """
    if not prices:
        return None
    n = len(prices)
    if n <= 3:
        return round(sum(prices) / n, 2)

    if n < 10:
        trim = 1
    else:
        trim = 2 * (n // 10)

    sorted_p = sorted(prices)
    trimmed  = sorted_p[trim: n - trim]
    if not trimmed:
        return round(sum(prices) / n, 2)
    return round(sum(trimmed) / len(trimmed), 2)


_ITEM_SCHEMA = """{
  "species": "numele normalizat (ex: Thuja Occidentalis, Gazonare, Substrat turba)",
  "category": "Conifere / Arbori / Arbusti / Plante_taratoare / Gazon / Materiale / Piatra / Manopera / Necunoscut",
  "unit": "buc / mp / ml / tona / ora / zi / proiect",
  "height_min_cm": intreg sau null,
  "height_max_cm": intreg sau null,
  "root_type": "CLT" sau "balot" sau "radacina_nuda" sau null,
  "clt_size": "5L / 10L / 25L / 45L / etc" sau null,
  "diameter_min_cm": intreg sau null,
  "diameter_max_cm": intreg sau null,
  "circumference_min_cm": intreg sau null,
  "circumference_max_cm": intreg sau null,
  "vat_included": true / false / null
}"""

_RULES = """Reguli categorii:
- Conifere: thuja, picea, pinus, juniperus, chamaecyparis, taxus, abies, cedrus
- Arbori: arbori foiosi ornamentali, stejar, mesteacan, artar, frasin, tei, paltin, betula, acer, liriodendron, liquidambar, aesculus, prunus, koelreuteria, albizia, cercis, sophora, amelanchier, carpinus, ulmus, platanus, pyrus, quercus, robinia
- Arbusti: arbusti ornamentali, trandafir, forsythia, spiraea, ligustrum, buxus, berberis, lagerstroemia, cornus, deutzia, euonymus, lonicera, weigela, photinia, magnolia, viburnum, cotinus, sambucus, tamarix
- Plante_taratoare: ivy, vinca, cotoneaster orizontal, hedera, juniperus horizontalis
- Gazon: gazon rulou, gazon samanta, gazon sport
- Materiale: substrat, turba, cocos, ingrasamant, geotextil, folie iaz, mulci, scoarta
- Piatra: piatra cubica, pietris, nisip, granit, marmura, pavaj, bordura
- Manopera: gazonare, plantare, tuns, amenajare, design, irigatii, cosit, tarif ora
- Necunoscut: altceva

Reguli: m→cm (1.5m=150cm) | C5/C10/C25=CLT | balot/b&b=balot | rn=radacina_nuda | circ=circumferinta trunchi | fara TVA/+TVA→false | cu TVA→true | altfel→null"""


def parse_with_claude(text):
    """
    Parseaza descriere libera folosind Claude Haiku API.
    Returneaza (parsed_dict, error_string).
    """
    results = parse_batch_with_claude([text])
    if results and results[0] is not None:
        return results[0], None
    return None, "Parsare esuata"


def parse_batch_with_claude(texts):
    """
    Parseaza o lista de descrieri intr-un singur apel API.
    Returneaza lista de parsed_dict (None pentru esecuri individuale).
    BATCH_SIZE items per apel → reduce dramatic numarul de apeluri API.
    """
    if not texts:
        return []
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY'))

        numbered = '\n'.join(f'{i+1}. "{t}"' for i, t in enumerate(texts))
        prompt = f"""Esti expert in horticultura si landscaping din Romania.
Analizeaza fiecare descriere si extrage datele structurate.

Descrieri:
{numbered}

Schema pentru fiecare item:
{_ITEM_SCHEMA}

{_RULES}

Returneaza STRICT un array JSON valid cu exact {len(texts)} obiecte, in ordinea descrierilor, fara alte cuvinte:
[
  {{ obiect pentru descrierea 1 }},
  {{ obiect pentru descrierea 2 }},
  ...
]"""

        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300 * len(texts),
            messages=[{"role": "user", "content": prompt}]
        )
        raw = msg.content[0].text.strip()
        if raw.startswith('```'):
            raw = raw.split('```')[1]
            if raw.startswith('json'):
                raw = raw[4:]
        results = json.loads(raw.strip())
        if isinstance(results, list):
            # Pad/trim la lungimea corecta
            while len(results) < len(texts):
                results.append(None)
            return results[:len(texts)]
        return [None] * len(texts)
    except Exception as e:
        return [None] * len(texts)


def build_item_keys(parsed):
    """
    Din parsed dict, calculeaza:
    - height_bucket, diameter_bucket, circumference_bucket
    - canonical_key (pentru deduplicare exacta)
    - display_name
    - level1_key (specie + inaltime — agregare N1)
    - level2_key (specie + inaltime + root_type — agregare N2)
    """
    h_val = parsed.get('height_min_cm') or parsed.get('height_max_cm')
    d_val = parsed.get('diameter_min_cm') or parsed.get('diameter_max_cm')
    c_val = parsed.get('circumference_min_cm') or parsed.get('circumference_max_cm')

    height_bucket   = assign_bucket(h_val, HEIGHT_BUCKETS)        if h_val else None
    diameter_bucket = assign_bucket(d_val, DIAMETER_BUCKETS)      if d_val else None
    circ_bucket     = assign_bucket(c_val, CIRCUMFERENCE_BUCKETS) if c_val else None

    species   = (parsed.get('species') or 'Necunoscut').strip()
    root_type = parsed.get('root_type')
    clt_size  = parsed.get('clt_size')
    sp_key    = species.lower().replace(' ', '_')

    canonical_key = '|'.join([sp_key, height_bucket or '', root_type or '', clt_size or ''])
    level1_key    = '|'.join([sp_key, height_bucket or ''])
    level2_key    = '|'.join([sp_key, height_bucket or '', root_type or ''])

    parts = [species]
    if height_bucket:
        parts.append(height_bucket)
    if root_type:
        parts.append(f"{root_type} {clt_size}" if clt_size else root_type)

    return {
        'species':          species,
        'category':         parsed.get('category') or 'Necunoscut',
        'unit':             parsed.get('unit') or 'buc',
        'height_bucket':    height_bucket,
        'diameter_bucket':  diameter_bucket,
        'circ_bucket':      circ_bucket,
        'root_type':        root_type,
        'clt_size':         clt_size,
        'canonical_key':    canonical_key,
        'level1_key':       level1_key,
        'level2_key':       level2_key,
        'display_name':     ' '.join(parts),
        'vat_included':     parsed.get('vat_included'),
    }


def parse_item(text):
    """
    Entry point principal. Parseaza text liber si returneaza
    dict complet cu toate cheile de indexare, sau None + eroare.
    """
    parsed, err = parse_with_claude(text)
    if err or not parsed:
        return None, err or "Parsare esuata"
    keys = build_item_keys(parsed)
    return keys, None
