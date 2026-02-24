// ══════════════════════════════════════════════════
// api/rates.js
// Returns the best available TC rate for every
// vessel/origin/destination combination we have.
//
// Applies the freshness framework:
//   Tier 1 (0-3 days):  confidence 95
//   Tier 2 (4-14 days): confidence 75
//   Tier 3 (15-45 days):confidence 50
//   Hardcoded fallback: confidence 30 (not from this API)
// ══════════════════════════════════════════════════

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_ANON_KEY; // public read-only key

const TIER1_DAYS = 3;
const TIER2_DAYS = 14;
const TIER3_DAYS = 45;

function getTierAndConfidence(daysOld) {
  if (daysOld <= TIER1_DAYS) return { tier: 1, confidence: 95 };
  if (daysOld <= TIER2_DAYS) return { tier: 2, confidence: 75 };
  if (daysOld <= TIER3_DAYS) return { tier: 3, confidence: 50 };
  return { tier: 4, confidence: 30 };
}

module.exports = async function handler(req, res) {
  // CORS — allow the calculator to call this from any origin
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');
  res.setHeader('Cache-Control', 's-maxage=3600, stale-while-revalidate'); // cache 1h on CDN

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - TIER3_DAYS);
    const cutoffStr = cutoff.toISOString().split('T')[0];
    const today     = new Date().toISOString().split('T')[0];

    // Fetch TC rates and bunker prices in parallel
    const [tcRes, bunkerRes] = await Promise.all([
      // TC rates — last 45 days
      fetch(`${SUPABASE_URL}/rest/v1/scraped_rates?select=vessel_type,origin_region,destination_region,origin_text,destination_text,rate,scraped_date,raw_line&scraped_date=gte.${cutoffStr}&order=scraped_date.desc&limit=5000`, {
        headers: { 'apikey': SUPABASE_KEY, 'Authorization': `Bearer ${SUPABASE_KEY}` },
      }),
      // Bunker prices — last 7 days per hub
      fetch(`${SUPABASE_URL}/rest/v1/bunker_prices?select=hub,vlsfo,mgo,scraped_date&order=scraped_date.desc&limit=50`, {
        headers: { 'apikey': SUPABASE_KEY, 'Authorization': `Bearer ${SUPABASE_KEY}` },
      }),
    ]);

    if (!tcRes.ok) throw new Error(`Supabase TC query failed: ${tcRes.status}`);
    const rows = await tcRes.json();

    // Bunker prices — non-fatal
    let bunkerByHub = {};
    if (bunkerRes.ok) {
      const bunkerRows = await bunkerRes.json();
      // Keep only the most recent row per hub
      for (const row of bunkerRows) {
        if (!bunkerByHub[row.hub]) {
          const rowMs   = new Date(row.scraped_date).getTime();
          const todayMs = new Date(today).getTime();
          const daysOld = Math.round((todayMs - rowMs) / 86400000);
          bunkerByHub[row.hub] = {
            hub:         row.hub,
            vlsfo:       row.vlsfo,
            mgo:         row.mgo,
            scrapedDate: row.scraped_date,
            daysOld,
          };
        }
      }
    }

    // Dedup TC rates: keep most recent per (vessel, origin, destination)
    const best = new Map();
    for (const row of rows) {
      const key = `${row.vessel_type}|${row.origin_region}|${row.destination_region}`;
      if (!best.has(key)) best.set(key, row);
    }

    const todayMs = new Date(today).getTime();
    const result  = [];

    for (const row of best.values()) {
      const rowMs   = new Date(row.scraped_date).getTime();
      const daysOld = Math.round((todayMs - rowMs) / 86400000);
      const { tier, confidence } = getTierAndConfidence(daysOld);

      result.push({
        vesselType:         row.vessel_type,
        originRegion:       row.origin_region,
        destinationRegion:  row.destination_region,
        originText:         row.origin_text,
        destinationText:    row.destination_text,
        rate:               row.rate,
        scrapedDate:        row.scraped_date,
        daysOld,
        tier,
        confidence,
        rawLine:            row.raw_line,
      });
    }

    result.sort((a, b) =>
      a.vesselType.localeCompare(b.vesselType) ||
      a.originRegion.localeCompare(b.originRegion)
    );

    res.json({
      success:   true,
      count:     result.length,
      fetchedAt: new Date().toISOString(),
      rates:     result,
      bunker:    bunkerByHub,
    });

  } catch (err) {
    console.error('[rates] error:', err.message);
    res.status(500).json({ success: false, error: err.message, rates: [], bunker: {} });
  }
}
