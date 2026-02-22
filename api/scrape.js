// ══════════════════════════════════════════════════
// api/scrape.js
// Fetches HandyBulk daily rates page, parses all
// TC rate bullet points, stores them in Supabase.
//
// Called by Vercel Cron at 08:00 UTC daily.
// Also callable manually for testing.
// ══════════════════════════════════════════════════

const SUPABASE_URL    = process.env.SUPABASE_URL;
const SUPABASE_KEY    = process.env.SUPABASE_SERVICE_KEY; // service role key (write access)
const CRON_SECRET     = process.env.CRON_SECRET;          // protects this endpoint

const SOURCE_URL = 'https://www.handybulk.com/ship-charter-rates/';

// ── Region text → our region code ─────────────────
// Maps HandyBulk's free-text location names to our
// standardised region codes used in the calculator.
const REGION_MAP = {
  // Europe
  'continent':              'N.EUROPE',
  'n.europe':               'N.EUROPE',
  'north europe':           'N.EUROPE',
  'northern europe':        'N.EUROPE',
  'arag':                   'N.EUROPE',   // Amsterdam-Rotterdam-Antwerp-Ghent
  'uk continent':           'N.EUROPE',
  'germany':                'N.EUROPE',
  'uk':                     'N.EUROPE',
  'netherlands':            'N.EUROPE',
  'belgium':                'N.EUROPE',
  'france':                 'N.EUROPE',
  // Mediterranean
  'spain':                  'W.MED',
  'portugal':               'W.MED',
  'morocco':                'W.MED',
  'algeria':                'W.MED',
  'w.med':                  'W.MED',
  'west med':               'W.MED',
  'west mediterranean':     'W.MED',
  'east mediterranean':     'E.MED',
  'e.med':                  'E.MED',
  'emed':                   'E.MED',
  'east med':               'E.MED',
  'egypt med':              'E.MED',
  'egypt':                  'E.MED',
  'turkey':                 'E.MED',
  'turkiye':                'E.MED',
  'turkiye med':            'E.MED',
  'greece':                 'E.MED',
  'italy':                  'W.MED',
  // Black Sea
  'black sea':              'BLACK SEA',
  'ukraine':                'BLACK SEA',
  'romania':                'BLACK SEA',
  // Americas
  'us gulf':                'US GULF',
  'usg':                    'US GULF',
  'us east coast':          'US EAST COAST',
  'usec':                   'US EAST COAST',
  'east coast south america':'E.S.AMERICA',
  'ecsa':                   'E.S.AMERICA',
  'brazil':                 'E.S.AMERICA',
  'argentina':              'E.S.AMERICA',
  'uruguay':                'E.S.AMERICA',
  'sw passage':             'E.S.AMERICA',  // SW Passage = S. America
  'north coast south america':'N.S.AMERICA',
  'ncsa':                   'N.S.AMERICA',
  'colombia':               'N.S.AMERICA',
  'venezuela':              'N.S.AMERICA',
  'dominican republic':     'CARIBBEAN',
  'caribbean':              'CARIBBEAN',
  'mexico east coast':      'MEXICO',
  'mexico':                 'MEXICO',
  'peru':                   'W.S.AMERICA',
  'ecuador':                'W.S.AMERICA',
  'west coast south america':'W.S.AMERICA',
  'wcsa':                   'W.S.AMERICA',
  // Africa
  'west africa':            'W.AFRICA',
  'wafr':                   'W.AFRICA',
  'waf':                    'W.AFRICA',
  'w.africa':               'W.AFRICA',
  'nigeria':                'W.AFRICA',
  'gabon':                  'W.AFRICA',
  'ghana':                  'W.AFRICA',
  'south africa':           'S.AFRICA',
  'saf':                    'S.AFRICA',
  's.africa':               'S.AFRICA',
  'east africa':            'E.AFRICA',
  'kenya':                  'E.AFRICA',
  'mozambique':             'E.AFRICA',
  // Middle East / Red Sea
  'middle east':            'MIDDLE EAST',
  'uae':                    'MIDDLE EAST',
  'qatar':                  'MIDDLE EAST',
  'oman':                   'MIDDLE EAST',
  'saudi arabia':           'MIDDLE EAST',
  'iraq':                   'MIDDLE EAST',
  'iran':                   'MIDDLE EAST',
  'red sea':                'RED SEA',
  // Indian Subcontinent
  'west coast india':       'W.INDIA',
  'wci':                    'W.INDIA',
  'w.india':                'W.INDIA',
  'india':                  'W.INDIA',
  'pakistan':               'W.INDIA',
  'east coast india':       'E.INDIA',
  'eci':                    'E.INDIA',
  'e.india':                'E.INDIA',
  'bangladesh':             'E.INDIA',
  'south india':            'S.INDIA',
  's.india':                'S.INDIA',
  'sri lanka':              'S.INDIA',
  // Asia Pacific
  'china':                  'CHINA',
  'south china':            'CHINA',
  'north china':            'CHINA',
  'hong kong':              'CHINA',
  'taiwan':                 'N.ASIA',
  'japan':                  'N.ASIA',
  'japan-korea':            'N.ASIA',
  'south korea':            'N.ASIA',
  'korea':                  'N.ASIA',
  'north pacific':          'N.ASIA',
  'nopac':                  'N.ASIA',
  'n.asia':                 'N.ASIA',
  'far east':               'CHINA',      // generic Far East → China
  'indonesia':              'SE.ASIA',
  'malaysia':               'SE.ASIA',
  'thailand':               'SE.ASIA',
  'vietnam':                'SE.ASIA',
  'cambodia':               'SE.ASIA',
  'philippines':            'SE.ASIA',
  'south east asia':        'SE.ASIA',
  'southeast asia':         'SE.ASIA',
  'se asia':                'SE.ASIA',
  'sea':                    'SE.ASIA',
  'australia':              'AUSTRALIA',
};

// ── Map a raw location text to region code ─────────
function mapRegion(text) {
  if (!text) return null;
  const t = text.toLowerCase()
    .replace(/\s*\(.*?\)/g, '')   // strip parenthetical e.g. "(USG)" "(ECSA)"
    .trim();

  // Direct match first
  if (REGION_MAP[t]) return REGION_MAP[t];

  // Partial match — look for any key that appears in the text
  for (const [key, region] of Object.entries(REGION_MAP)) {
    if (t.includes(key)) return region;
  }

  return null;
}

// ── Map HandyBulk vessel name to our vessel type ───
function mapVessel(text) {
  const t = text.toLowerCase();
  if (t.includes('capesize') || t.includes('cape')) return 'CAPESIZE';
  if (t.includes('panamax'))                         return 'PANAMAX';
  if (t.includes('ultramax'))                        return 'ULTRAMAX';
  if (t.includes('supramax'))                        return 'SUPRAMAX';
  if (t.includes('handy'))                           return 'HANDY';
  return null;
}

// ── Parse bullet line ──────────────────────────────
// Input:  "• Ultramax open Continent to China fixed around $17,500"
// Output: { vesselType, originText, destinationText, rate }
// Also handles "via" routes:
//   "Supramax open West Africa (WAFR) via ECSA to China fixed around $20,500"
function parseLine(line) {
  // Strip bullet, trim
  const clean = line.replace(/^[•·\-\*]\s*/, '').trim();

  // Must end with "fixed around $NUMBER"
  const rateMatch = clean.match(/fixed\s+around\s+\$([0-9,]+)/i);
  if (!rateMatch) return null;

  const rate = parseInt(rateMatch[1].replace(/,/g, ''), 10);
  if (!rate || rate < 1000 || rate > 200000) return null; // sanity check

  // Everything before "fixed around"
  const beforeFixed = clean.substring(0, clean.indexOf('fixed around')).trim();

  // Extract vessel type (first word)
  const firstWord = beforeFixed.split(/\s+/)[0];
  const vesselType = mapVessel(firstWord);
  if (!vesselType) return null;

  // Remove "VESSEL open" prefix
  const afterOpen = beforeFixed.replace(/^\S+\s+open\s+/i, '').trim();

  // Split on " to " — last occurrence is always destination
  const toIdx = afterOpen.lastIndexOf(' to ');
  if (toIdx === -1) return null;

  const originVia = afterOpen.substring(0, toIdx).trim();
  const destinationText = afterOpen.substring(toIdx + 4).trim().replace(/\s*$/, '');

  // Origin is everything before " via " (or the whole string if no "via")
  const viaIdx = originVia.lastIndexOf(' via ');
  const originText = (viaIdx !== -1)
    ? originVia.substring(0, viaIdx).trim()
    : originVia;

  const originRegion = mapRegion(originText);
  const destinationRegion = mapRegion(destinationText);

  // Skip if we can't map either region
  if (!originRegion || !destinationRegion) return null;

  return {
    vesselType,
    originText,
    destinationText,
    originRegion,
    destinationRegion,
    rate,
    rawLine: clean,
  };
}

// ── Parse the full HTML page ───────────────────────
function parseRates(html) {
  const rates = [];
  const seen  = new Set(); // dedup within one day's scrape

  // Extract all bullet lines
  const lines = html.split('\n');
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed.startsWith('•') && !trimmed.startsWith('·')) continue;
    if (!trimmed.toLowerCase().includes('fixed around')) continue;

    const parsed = parseLine(trimmed);
    if (!parsed) continue;

    // Dedup key: same vessel+origin+dest shouldn't appear twice in one day
    const key = `${parsed.vesselType}|${parsed.originRegion}|${parsed.destinationRegion}`;
    if (seen.has(key)) continue;
    seen.add(key);

    rates.push(parsed);
  }

  return rates;
}

// ── Insert rows into Supabase ──────────────────────
async function insertToSupabase(rows) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/scraped_rates`, {
    method: 'POST',
    headers: {
      'apikey':        SUPABASE_KEY,
      'Authorization': `Bearer ${SUPABASE_KEY}`,
      'Content-Type':  'application/json',
      'Prefer':        'return=minimal',
    },
    body: JSON.stringify(rows),
  });

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Supabase insert failed: ${res.status} ${err}`);
  }
}

// ── Main scrape function ───────────────────────────
async function scrape() {
  // 1. Fetch HandyBulk
  const res = await fetch(SOURCE_URL, {
    headers: { 'User-Agent': 'DryFreight-Bot/1.0 (data@dryfreight.com)' },
  });
  if (!res.ok) throw new Error(`Failed to fetch HandyBulk: ${res.status}`);
  const html = await res.text();

  // 2. Parse rates
  const parsed = parseRates(html);
  if (parsed.length === 0) throw new Error('No rates found — page structure may have changed');

  // 3. Add today's date
  const today = new Date().toISOString().split('T')[0];
  const rows  = parsed.map(r => ({
    scraped_date:        today,
    vessel_type:         r.vesselType,
    origin_text:         r.originText,
    destination_text:    r.destinationText,
    origin_region:       r.originRegion,
    destination_region:  r.destinationRegion,
    rate:                r.rate,
    raw_line:            r.rawLine,
  }));

  // 4. Insert to Supabase
  await insertToSupabase(rows);

  return { date: today, inserted: rows.length, rates: rows };
}

// ── Vercel handler ─────────────────────────────────
export default async function handler(req, res) {
  // Allow GET from cron (Vercel sends Authorization header automatically)
  // Also allow manual POST with secret for testing
  const auth = req.headers.authorization || '';
  const isVercelCron = req.headers['x-vercel-cron'] === '1';
  const isManual     = CRON_SECRET && auth === `Bearer ${CRON_SECRET}`;
  const isQuery      = CRON_SECRET && req.query.secret === CRON_SECRET;

  if (!isVercelCron && !isManual && !isQuery) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    const result = await scrape();
    console.log(`[scrape] ${result.date}: inserted ${result.inserted} rates`);
    res.json({ success: true, ...result });
  } catch (err) {
    console.error('[scrape] error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
}
