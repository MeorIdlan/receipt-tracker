/**
 * Receipts Poller – scans a Drive folder for new files and posts to your Ingress endpoint.
 * Uses overlap polling + a small seen-ID cache for safety; backend should still be idempotent.
 * Use in Apps Script
 */

const PROP = PropertiesService.getScriptProperties();

function setup() {
  // Optional: set properties here instead of the UI.
  // PROP.setProperties({
  //   FOLDER_ID: 'YOUR_FOLDER_ID',
  //   INGRESS_URL: 'https://YOUR_INGRESS_URL',
  //   API_KEY: 'YOUR_LONG_RANDOM_KEY',
  //   LOOKBACK_MINUTES: '5'
  // }, true);
}

/** Entry point for the time-driven trigger */
function pollFolder() {
  const cfg = loadConfig();
  const sinceIso = computeSinceIso(cfg.lookbackMinutes);
  const seen = loadSeenIds();

  const query = [
    `'${cfg.folderId}' in parents`,
    "trashed = false",
    `createdTime >= '${sinceIso}'`
  ].join(" and ");

  let pageToken = null;
  let count = 0;

  do {
    const res = Drive.Files.list({
      q: query,
      orderBy: "createdTime",
      pageToken: pageToken,
      includeItemsFromAllDrives: true,
      supportsAllDrives: true,
      fields: "files(id,name,mimeType,createdTime),nextPageToken"
    });

    (res.files || []).forEach(function (f) {
      if (seen.has(f.id)) return; // skip if we just sent it recently
      const payload = {
        fileId: f.id,
        name: f.name,
        mimeType: f.mimeType,
        createdTime: f.createdTime, // RFC3339 (UTC)
        folderId: cfg.folderId
      };
      const ok = postToIngress(cfg.endpoint, cfg.apiKey, payload);
      if (ok) {
        seen.add(f.id);
        count++;
      }
    });

    pageToken = res.nextPageToken || null;
  } while (pageToken);

  persistSeenIds(seen);
  console.log(`pollFolder: posted ${count} new files since ${sinceIso}`);
}

function loadConfig() {
  const folderId = PROP.getProperty('FOLDER_ID');
  const endpoint = PROP.getProperty('INGRESS_URL');
  const apiKey = PROP.getProperty('API_KEY');
  const lookback = parseInt(PROP.getProperty('LOOKBACK_MINUTES') || '5', 10);

  if (!folderId || !endpoint || !apiKey) {
    throw new Error("Missing required Script Properties: FOLDER_ID, INGRESS_URL, API_KEY");
  }
  return { folderId, endpoint, apiKey, lookbackMinutes: lookback };
}

/** overlap polling window to avoid misses; backend dedupes */
function computeSinceIso(lookbackMinutes) {
  const now = new Date();
  const since = new Date(now.getTime() - lookbackMinutes * 60 * 1000);
  // Optionally, store LAST_RUN_ISO if you want a moving watermark:
  PROP.setProperty('LAST_RUN_ISO', now.toISOString());
  return since.toISOString();
}

/** Lightweight seen-ID cache (ring buffer) to reduce duplicate posts between runs */
function loadSeenIds() {
  const raw = PROP.getProperty('SEEN_IDS_JSON') || '[]';
  const arr = JSON.parse(raw);
  // keep only the most recent 500 ids
  return new Set(arr.slice(-500));
}

function persistSeenIds(seenSet) {
  const arr = Array.from(seenSet);
  const trimmed = arr.slice(-500);
  PROP.setProperty('SEEN_IDS_JSON', JSON.stringify(trimmed));
}

/** POST to your Ingress Cloud Function with retries */
function postToIngress(url, apiKey, obj) {
  const options = {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(obj),
    headers: { 'X-API-Key': apiKey },
    muteHttpExceptions: true
  };
  const attempts = 3;
  for (let i = 1; i <= attempts; i++) {
    try {
      const resp = UrlFetchApp.fetch(url, options);
      const code = resp.getResponseCode();
      if (code >= 200 && code < 300) return true;
      console.warn(`Ingress response ${code}: ${resp.getContentText()}`);
      // 4xx likely won't succeed on retry, but we try once more
    } catch (e) {
      console.warn(`postToIngress attempt ${i} error: ${e}`);
    }
    Utilities.sleep(500 * i); // simple backoff
  }
  return false;
}
