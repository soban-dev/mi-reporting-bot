"""
Mi Reporting Bot — per-ad-unit insights sync

Fetches GAM report data (revenue, impressions, clicks, CTR, eCPM, CPM) for a
single GAM network (GAM_NETWORK_CODE), matches each ad unit to a publisher
website using the ad unit path stored in the OLD DB's `websites` table, and
upserts the results into the NEW DB's `ad_unit_daily_stats` table.

Why the match:
  The publisher setup uses exactly ONE ad unit per website. The ad unit's name
  is the website name (set manually), and `websites.ad_units[]` stores the
  `ad_unit_path` (e.g. "/Testing") for that unit. Only report rows whose ad
  unit path exists in the OLD DB are saved.

Usage:
  python sync.py                    # run in loop
  python sync.py --once             # single sync, no loop
  python sync.py --interval 15      # custom interval in minutes

Requires .env file with:
  OLD_SUPABASE_URL, OLD_SUPABASE_SERVICE_KEY,
  SUPABASE_URL, SUPABASE_SERVICE_KEY,
  GAM_CLIENT_EMAIL, GAM_PRIVATE_KEY,
  GAM_NETWORK_CODE                  # child network to report on (e.g. 22862221459)
"""

import os
import re
import sys
import csv
import json
import time
import gzip
import io
import logging
import signal
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests
from dotenv import load_dotenv
from supabase import create_client, Client as SupabaseClient
from google.oauth2 import service_account
from google.auth.transport import requests as gauth_requests

load_dotenv()

# ── Configuration ───────────────────────────────────────────────

OLD_SUPABASE_URL = os.getenv("OLD_SUPABASE_URL")
OLD_SUPABASE_SERVICE_KEY = os.getenv("OLD_SUPABASE_SERVICE_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
GAM_CLIENT_EMAIL = os.getenv("GAM_CLIENT_EMAIL")
GAM_PRIVATE_KEY = os.getenv("GAM_PRIVATE_KEY")
GAM_NETWORK_CODE = os.getenv("GAM_NETWORK_CODE", "").strip()

APPROVED_STATUS = os.getenv("APPROVED_STATUS", "Approved")

SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL", "30"))
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "30"))
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "90"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
WRITE_RETRIES = int(os.getenv("WRITE_RETRIES", "3"))
REPORT_POLL_MAX = int(os.getenv("REPORT_POLL_MAX", "300"))
REPORT_DOWNLOAD_TIMEOUT = int(os.getenv("REPORT_DOWNLOAD_TIMEOUT", "300"))
LOG_DIR = os.getenv("LOG_DIR", "logs")
GAM_VERSION = "v202602"

# ── Logging Setup ───────────────────────────────────────────────

os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, f"mi_report_{datetime.now().strftime('%Y%m%d')}.log")

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ],
)
log = logging.getLogger("mi_reporting")

shutdown_flag = threading.Event()


def handle_signal(signum, frame):
    log.warning("Received signal %s — shutting down gracefully...", signum)
    shutdown_flag.set()


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


# ═════════════════════════════════════════════════════════════════
#  GAM HELPERS
# ═════════════════════════════════════════════════════════════════


def sanitize_private_key(raw: str) -> str:
    key = raw.strip()
    key = key.replace("\\\\n", "\n")
    key = key.replace("\\n", "\n")
    key = key.replace("\r\n", "\n").replace("\r", "\n")
    if not key.endswith("\n"):
        key += "\n"
    return key


def get_access_token(client_email: str, private_key_pem: str) -> str:
    key = sanitize_private_key(private_key_pem)
    creds = service_account.Credentials.from_service_account_info(
        {
            "client_email": client_email.strip(),
            "private_key": key,
            "token_uri": "https://oauth2.googleapis.com/token",
        },
        scopes=["https://www.googleapis.com/auth/dfp"],
    )
    creds.refresh(gauth_requests.Request())
    if not creds.token:
        raise RuntimeError("Failed to obtain GAM access token")
    return creds.token


def build_envelope(network_code: str, body: str) -> str:
    return f'''<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope
  xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
  xmlns:xsd="http://www.w3.org/2001/XMLSchema"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <soapenv:Header>
    <ns1:RequestHeader
      soapenv:actor="http://schemas.xmlsoap.org/soap/actor/next"
      soapenv:mustUnderstand="0"
      xmlns:ns1="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
      <ns1:networkCode>{network_code}</ns1:networkCode>
      <ns1:applicationName>AdGlobeX</ns1:applicationName>
    </ns1:RequestHeader>
  </soapenv:Header>
  <soapenv:Body>{body}</soapenv:Body>
</soapenv:Envelope>'''


_http_local = threading.local()
_supabase_local = threading.local()


def _supabase_client() -> SupabaseClient:
    client = getattr(_supabase_local, "client", None)
    if client is None:
        client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        _supabase_local.client = client
    return client


def _old_db_client() -> SupabaseClient:
    client = getattr(_supabase_local, "old_client", None)
    if client is None:
        client = create_client(OLD_SUPABASE_URL, OLD_SUPABASE_SERVICE_KEY)
        _supabase_local.old_client = client
    return client


def _http_session() -> requests.Session:
    session = getattr(_http_local, "session", None)
    if session is None:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=8, pool_maxsize=32, max_retries=0
        )
        session.mount("https://", adapter)
        _http_local.session = session
    return session


def soap_call(network_code: str, service: str, body: str, token: str) -> str:
    url = f"https://ads.google.com/apis/ads/publisher/{GAM_VERSION}/{service}"
    envelope = build_envelope(network_code, body)
    try:
        resp = _http_session().post(
            url,
            data=envelope.encode("utf-8"),
            headers={
                "Content-Type": "text/xml;charset=UTF-8",
                "SOAPAction": '""',
                "Authorization": f"Bearer {token}",
            },
            timeout=60,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"GAM SOAP {service} HTTP {resp.status_code}: {resp.text[:500]}")
        return resp.text
    except requests.RequestException as e:
        raise RuntimeError(f"GAM SOAP {service} connection error: {e}")
    except Exception as e:
        raise RuntimeError(f"GAM SOAP {service} error: {e}")


def soap_call_with_retry(network_code: str, service: str, body: str, token: str, attempts: int = 3) -> str:
    delay = 2.0
    for i in range(attempts):
        try:
            return soap_call(network_code, service, body, token)
        except RuntimeError as e:
            msg = str(e)
            if i >= attempts - 1:
                raise
            lowered = msg.lower()
            if (
                "server_error" in lowered
                or "connection" in lowered
                or "disconnect" in lowered
                or "terminat" in lowered
                or "aborted" in lowered
                or "reset" in lowered
                or "timed out" in lowered
                or "timeout" in lowered
                or "http 500" in lowered
                or "http 502" in lowered
                or "http 503" in lowered
                or "http 429" in lowered
            ):
                time.sleep(delay)
                delay *= 2
                continue
            raise


def extract_text(xml: str, tag: str) -> Optional[str]:
    m = re.search(
        rf"<(?:[a-z0-9_]+:)?{tag}[^>]*>([\s\S]*?)</(?:[a-z0-9_]+:)?{tag}>",
        xml,
        re.IGNORECASE,
    )
    return m.group(1).strip() if m else None


def date_to_gam_xml(date_str: str, tag_name: str) -> str:
    parts = date_str.split("-")
    return f"<{tag_name}><year>{parts[0]}</year><month>{int(parts[1])}</month><day>{int(parts[2])}</day></{tag_name}>"


def get_date_days_ago(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")


def compute_sync_range() -> tuple[str, str]:
    """Return (start_date, end_date) for the report.

    Incremental by default: start from the day after the newest row already in
    `ad_unit_daily_stats`, minus a 1-day buffer so late/backfilled GAM data is
    picked up. Falls back to LOOKBACK_DAYS when the table is empty or the query
    fails, and never goes further back than LOOKBACK_DAYS.
    """
    today = datetime.now(timezone.utc)
    end_date = today.strftime("%Y-%m-%d")
    max_lookback = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    try:
        resp = _supabase_client().table("ad_unit_daily_stats").select("date").order("date", desc=True).limit(1).execute()
        newest = (resp.data or [{}])[0].get("date")
        if newest:
            newest_dt = datetime.strptime(newest[:10], "%Y-%m-%d").date()
            start_date = (newest_dt - timedelta(days=1)).strftime("%Y-%m-%d")  # 1-day buffer for late data
            if start_date < max_lookback:
                start_date = max_lookback
            return start_date, end_date
    except Exception as e:
        log.warning("Could not read latest sync date, falling back to lookback: %s", e)

    return max_lookback, end_date


def build_report_query(start_date: str, end_date: str, ad_unit_ids: Optional[list[str]] = None) -> str:
    filter_xml = ""
    if ad_unit_ids:
        ids = ",".join(uid for uid in ad_unit_ids)
        filter_xml = f"<statement><query>WHERE AD_UNIT_ID IN ({ids})</query></statement>"
    return f'''
    <runReportJob xmlns="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
      <reportJob>
        <reportQuery>
           <dimensions>DATE</dimensions>
           <dimensions>AD_UNIT</dimensions>
           <dimensions>AD_UNIT_ID</dimensions>
           <dimensions>SITE_NAME</dimensions>
           <dimensions>COUNTRY</dimensions>
           <dimensions>COUNTRY_CODE</dimensions>
           <dimensions>DEVICE_CATEGORY_NAME</dimensions>
           <dimensions>MOBILE_APP_NAME</dimensions>
          <columns>AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE</columns>
          <columns>AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS</columns>
          <columns>AD_EXCHANGE_LINE_ITEM_LEVEL_CLICKS</columns>
          <columns>AD_EXCHANGE_LINE_ITEM_LEVEL_CTR</columns>
          <columns>AD_EXCHANGE_LINE_ITEM_LEVEL_AVERAGE_ECPM</columns>
          {date_to_gam_xml(start_date, "startDate")}
          {date_to_gam_xml(end_date, "endDate")}
          <dateRangeType>CUSTOM_DATE</dateRangeType>
          {filter_xml}
          <reportCurrency>USD</reportCurrency>
          <timeZoneType>PUBLISHER</timeZoneType>
        </reportQuery>
      </reportJob>
    </runReportJob>'''


def submit_report_job(network_code: str, start_date: str, end_date: str, token: str, ad_unit_ids: Optional[list[str]] = None) -> str:
    run_resp = soap_call_with_retry(network_code, "ReportService", build_report_query(start_date, end_date, ad_unit_ids), token)
    job_id = extract_text(run_resp, "id")
    if not job_id:
        raise RuntimeError("Could not extract report job ID")
    return job_id


def get_report_job_status(network_code: str, job_id: str, token: str) -> str:
    status_body = f'''
    <getReportJobStatus xmlns="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
      <reportJobId>{job_id}</reportJobId>
    </getReportJobStatus>'''
    status_resp = soap_call_with_retry(network_code, "ReportService", status_body, token)
    rval = extract_text(status_resp, "rval")
    return rval if rval else "IN_PROGRESS"


def download_report_rows(network_code: str, job_id: str, token: str) -> list[dict]:
    url_body = f'''
    <getReportDownloadURL xmlns="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
      <reportJobId>{job_id}</reportJobId>
      <exportFormat>CSV_DUMP</exportFormat>
    </getReportDownloadURL>'''
    url_resp = soap_call_with_retry(network_code, "ReportService", url_body, token)
    download_url = extract_text(url_resp, "rval")
    if not download_url:
        raise RuntimeError("Could not get report download URL")
    download_url = download_url.replace("&amp;", "&")

    delay = 2.0
    raw = None
    for i in range(3):
        try:
            dl_resp = _http_session().get(download_url, timeout=REPORT_DOWNLOAD_TIMEOUT)
            dl_resp.raise_for_status()
            raw = dl_resp.content
            break
        except Exception as e:
            if i >= 2:
                raise RuntimeError(f"GAM report download failed: {e}")
            time.sleep(delay)
            delay *= 2

    try:
        csv_text = gzip.decompress(raw).decode("utf-8")
    except (gzip.BadGzipFile, OSError):
        csv_text = raw.decode("utf-8")
    return parse_report_csv(csv_text)


# ═════════════════════════════════════════════════════════════════
#  GAM AD UNIT INVENTORY
# ═════════════════════════════════════════════════════════════════

try:
    import xml.etree.ElementTree as ET
except ImportError:  # pragma: no cover
    ET = None


def _strip_ns(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def parse_ad_units_page(xml_text: str) -> list[tuple[str, str]]:
    """Parse a getAdUnitsByStatement response into [(id, name)] pairs.
    Uses a real XML parser so parentPath blocks don't corrupt name extraction."""
    if ET is None:
        raise RuntimeError("xml.etree.ElementTree unavailable")
    root = ET.fromstring(xml_text)
    out: list[tuple[str, str]] = []
    for el in root.iter():
        if _strip_ns(el.tag) != "results":
            continue
        uid_el = el.find("{*}id")
        name_el = el.find("{*}name")
        if uid_el is not None and uid_el.text and name_el is not None and name_el.text:
            out.append((uid_el.text.strip(), name_el.text.strip()))
    return out


def fetch_ad_units(network_code: str, token: str) -> dict[str, list[str]]:
    """Fetch all ad units for the network via InventoryService.getAdUnitsByStatement
    and return {normalized name -> [ad unit ids]}. Paginates on totalResultSetSize.
    Values are lists because ad unit names can repeat across parent paths. Used to
    filter the report to only the ad units we care about (scales to thousands)."""
    result: dict[str, list[str]] = {}
    offset = 0
    page_size = 500
    total = None
    while True:
        body = f'''
        <getAdUnitsByStatement xmlns="https://www.google.com/apis/ads/publisher/{GAM_VERSION}">
          <filterStatement>
            <query>LIMIT {page_size} OFFSET {offset}</query>
          </filterStatement>
        </getAdUnitsByStatement>'''
        resp = soap_call_with_retry(network_code, "InventoryService", body, token)
        try:
            pairs = parse_ad_units_page(resp)
        except Exception as e:
            log.error("Failed to parse ad unit inventory page: %s", e)
            break
        for uid, name in pairs:
            key = normalize_path(name)
            if key:
                result.setdefault(key, []).append(uid)
        if total is None:
            m = re.search(r"<totalResultSetSize>(\d+)</totalResultSetSize>", resp)
            if m:
                total = int(m.group(1))
        offset += page_size
        if total is not None and offset >= total:
            break
        if len(pairs) < page_size:
            break
    return result


# ═════════════════════════════════════════════════════════════════
#  REPORT CSV PARSING
# ═════════════════════════════════════════════════════════════════


def parse_report_csv(csv_text: str) -> list[dict]:
    reader = csv.reader(io.StringIO(csv_text))
    try:
        header_row = next(reader)
    except StopIteration:
        return []
    headers = [h.replace('"', "").strip().lower() for h in header_row]

    def idx(keywords: list[str]) -> int:
        for i, h in enumerate(headers):
            for kw in keywords:
                if kw in h:
                    return i
        return -1

    date_idx = idx(["date"])
    ad_unit_id_idx = idx(["ad_unit_id", "ad unit id"])
    ad_unit_idx = -1
    for i, h in enumerate(headers):
        has_unit = ("ad_unit" in h) or ("ad unit" in h)
        has_id = ("ad_unit_id" in h) or ("ad unit id" in h)
        if has_unit and not has_id:
            ad_unit_idx = i
            break
    revenue_idx = idx(["revenue"])
    ecpm_idx = idx(["ecpm"])
    imp_idx = idx(["impression"])
    clk_idx = idx(["click"])
    ctr_idx = idx(["ctr"])
    country_code_idx = -1
    for i, h in enumerate(headers):
        if ("country code" in h) or ("country_code" in h):
            country_code_idx = i
            break
    country_name_idx = -1
    for i, h in enumerate(headers):
        has_country = ("country" in h) or ("country_name" in h)
        has_code = ("country code" in h) or ("country_code" in h)
        if has_country and not has_code:
            country_name_idx = i
            break
    device_category_idx = -1
    for i, h in enumerate(headers):
        if "device_category" in h or "device category" in h or "devicecategory" in h:
            device_category_idx = i
            break
    app_idx = -1
    for i, h in enumerate(headers):
        hh = h.strip()
        if (
            hh == "app"
            or "app_name" in hh
            or "app name" in hh
            or "mobile_app_name" in hh
            or "mobile app name" in hh
            or "app / site" in hh
        ):
            app_idx = i
            break

    site_idx = -1
    for i, h in enumerate(headers):
        hh = h.strip()
        if hh == "site" or hh == "site name" or hh == "site_name" or hh.endswith(".site_name") or hh.endswith("site_name"):
            site_idx = i
            break

    if ad_unit_id_idx < 0:
        ad_unit_id_idx = ad_unit_idx

    rows: list[dict] = []
    for cols in reader:
        if not cols:
            continue
        try:
            revenue = (float(cols[revenue_idx]) if revenue_idx >= 0 else 0) / 1_000_000
            ecpm = (float(cols[ecpm_idx]) if ecpm_idx >= 0 else 0) / 1_000_000
            impressions = int(float(cols[imp_idx])) if imp_idx >= 0 else 0
            clicks = int(float(cols[clk_idx])) if clk_idx >= 0 else 0
            ctr = (float(cols[ctr_idx]) if ctr_idx >= 0 else 0) / 100.0
        except (ValueError, IndexError):
            continue

        cpm = round((revenue / impressions) * 1000, 6) if impressions else 0

        rows.append({
            "date": cols[date_idx] if date_idx >= 0 else "",
            "ad_unit": cols[ad_unit_idx] if ad_unit_idx >= 0 else "",
            "ad_unit_id": cols[ad_unit_id_idx] if ad_unit_id_idx >= 0 else "",
            "website_name": cols[site_idx] if site_idx >= 0 else "",
            "country_code": cols[country_code_idx] if country_code_idx >= 0 else "",
            "country": cols[country_name_idx] if country_name_idx >= 0 else "",
            "device_category": cols[device_category_idx] if device_category_idx >= 0 else "",
            "app": cols[app_idx] if app_idx >= 0 else "",
            "revenue": round(revenue, 6),
            "ecpm": round(ecpm, 6),
            "impressions": impressions,
            "clicks": clicks,
            "ctr": round(ctr, 6),
            "cpm": cpm,
        })
    return rows


# ═════════════════════════════════════════════════════════════════
#  OLD DB — websites + ad unit path matching
# ═════════════════════════════════════════════════════════════════


def normalize_path(p: str) -> str:
    s = (p or "").strip().lower()
    s = s.replace("\\", "/")
    s = s.strip("/")
    return s


def website_host(website: str) -> str:
    """Best-effort hostname of a stored website URL (e.g. https://site.com/ -> site.com)."""
    s = (website or "").strip().lower()
    s = re.sub(r"^[a-z][a-z0-9+.-]*://", "", s)
    s = s.split("/")[0].split("?")[0].strip()
    s = re.sub(r"^www\.", "", s)
    return s.strip()


def fetch_websites() -> list[dict]:
    """Paginate all APPROVED rows from the OLD DB `websites` table. Each website maps
    to the ad unit paths stored in its `ad_units` JSON (ad_unit_path fields). Rows
    whose status is not "Approved" are skipped entirely."""
    client = _old_db_client()
    rows: list[dict] = []
    offset = 0
    page_size = 1000
    while True:
        resp = (
            client.table("websites")
            .select("website, ad_units, adx_network_code")
            .eq("status", APPROVED_STATUS)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        chunk = resp.data or []
        rows.extend(chunk)
        if len(chunk) < page_size:
            break
        offset += page_size

    websites = []
    for r in rows:
        network_code = (r.get("adx_network_code") or "").strip()
        paths: list[str] = []
        raw = r.get("ad_units")
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    paths = [
                        normalize_path(u.get("ad_unit_path", ""))
                        for u in parsed
                        if isinstance(u, dict) and u.get("ad_unit_path")
                    ]
            except json.JSONDecodeError:
                paths = []
        websites.append({
            "website": (r.get("website") or "").strip(),
            "host": website_host(r.get("website") or ""),
            "network_code": network_code,
            "paths": paths,
        })
    return websites


def build_path_lookup(websites: list[dict]) -> dict[str, dict]:
    """Map report ad-unit identifiers -> { website, network_code }.

    A report row's AD_UNIT value (the ad unit NAME, which the publisher sets to
    the website name) is matched against several aliases so we only persist rows
    we can tie back to a known website:
      * normalized ad unit path from websites.ad_units[].ad_unit_path
      * the website hostname (site.com)
      * the last path segment (e.g. /Testing -> testing)
      * the exact ad unit path with leading slash
    """
    lookup: dict[str, dict] = {}
    for w in websites:
        for p in w["paths"]:
            if not p:
                continue
            entry = {"website": w["website"], "host": w["host"], "network_code": w["network_code"]}
            aliases = {p, p.strip("/"), w["host"]}
            last = p.rsplit("/", 1)[-1]
            if last:
                aliases.add(last)
            for a in aliases:
                if a:
                    lookup.setdefault(a, entry)
    return lookup


# ═════════════════════════════════════════════════════════════════
#  TOKEN CACHE
# ═════════════════════════════════════════════════════════════════

_token_lock = threading.Lock()
_cached_token: Optional[str] = None
_token_obtained: float = 0
_TOKEN_TTL = 55 * 60


def get_gam_token_cached() -> str:
    global _cached_token, _token_obtained
    now = time.monotonic()
    with _token_lock:
        if _cached_token and (now - _token_obtained) < _TOKEN_TTL:
            return _cached_token
        log.info("Refreshing GAM access token...")
        token = get_access_token(GAM_CLIENT_EMAIL, GAM_PRIVATE_KEY)
        _cached_token = token
        _token_obtained = now
        return token


# ═════════════════════════════════════════════════════════════════
#  NEW DB — write helpers
# ═════════════════════════════════════════════════════════════════


def upsert_report_rows(rows: list[dict], lookup: dict[str, dict]) -> int:
    """Map each report row to a website via its ad unit path and upsert the data.

    Because the report is run with the COUNTRY dimension, each (date, ad unit)
    appears once per country/site. We:
      * aggregate revenue/impressions/clicks back to (network, website, ad_unit, date)
        and upsert into ad_unit_daily_stats (recomputing ctr/ecpm/cpm);
      * write each country row into ad_unit_country_daily_stats.
    Returns number of country rows written."""
    if not rows:
        return 0

    now_iso = datetime.now(timezone.utc).isoformat()
    daily: dict[str, dict] = {}
    country: dict[str, dict] = {}
    breakdown_rows: dict[tuple[str, str, str, str, str, str], dict] = {}
    unmatched = 0
    for r in rows:
        ad_unit = (r.get("ad_unit") or "").strip()
        key = normalize_path(ad_unit)
        if not key:
            key = (r.get("ad_unit_id") or "").strip()
        site = lookup.get(key) or lookup.get(website_host(ad_unit))
        if not site:
            unmatched += 1
            continue

        ad_unit_id = r.get("ad_unit_id") or ""
        date = r.get("date") or ""
        revenue = r.get("revenue") or 0
        impressions = r.get("impressions") or 0
        clicks = r.get("clicks") or 0
        path = "/" + (key.lstrip("/"))
        website_name = (r.get("website_name") or "").strip() or site["host"]
        device_category = (r.get("device_category") or "N/A").strip() or "N/A"
        app = (r.get("app") or "N/A").strip() or "N/A"
        country_code = r.get("country_code") or ""
        country_name = r.get("country") or ""
        ctr = r.get("ctr") or 0
        ecpm = r.get("ecpm") or 0

        dkey = (ad_unit_id, website_name, date)
        d = daily.get(dkey)
        if d is None:
            d = daily[dkey] = {
                "network_code": GAM_NETWORK_CODE,
                "ad_unit_id": ad_unit_id,
                "ad_unit_path": path,
                "website_name": website_name,
                "date": date,
                "revenue": 0,
                "impressions": 0,
                "clicks": 0,
            }
        d["revenue"] += revenue
        d["impressions"] += impressions
        d["clicks"] += clicks

        ckey = (ad_unit_id, website_name, country_code, date)
        c = country.get(ckey)
        if c is None:
            c = country[ckey] = {
                "network_code": GAM_NETWORK_CODE,
                "ad_unit_id": ad_unit_id,
                "ad_unit_path": path,
                "website_name": website_name,
                "country_code": country_code,
                "country_name": country_name,
                "date": date,
                "revenue": 0,
                "impressions": 0,
                "clicks": 0,
            }
        c["revenue"] += revenue
        c["impressions"] += impressions
        c["clicks"] += clicks

        bkey = (ad_unit_id, website_name, country_code, device_category, app, date)
        b = breakdown_rows.get(bkey)
        if b is None:
            b = breakdown_rows[bkey] = {
                "network_code": GAM_NETWORK_CODE,
                "ad_unit_id": ad_unit_id,
                "ad_unit_path": path,
                "website_name": website_name,
                "country_code": country_code,
                "country_name": country_name,
                "device_category": device_category,
                "app_name": app,
                "date": date,
                "revenue": 0,
                "impressions": 0,
                "clicks": 0,
            }
        b["revenue"] += revenue
        b["impressions"] += impressions
        b["clicks"] += clicks

    if unmatched:
        log.info("%d report row(s) had no matching ad unit path — skipped", unmatched)

    daily_rows = []
    for d in daily.values():
        imp = d["impressions"]
        rev = d["revenue"]
        clk = d["clicks"]
        d["ctr"] = round(clk / imp, 6) if imp else 0
        d["ecpm"] = round((rev / imp) * 1000, 6) if imp else 0
        d["cpm"] = d["ecpm"]
        d["updated_at"] = now_iso
        daily_rows.append(d)

    country_rows = []
    for c in country.values():
        imp = c["impressions"]
        rev = c["revenue"]
        clk = c["clicks"]
        c["ctr"] = round(clk / imp, 6) if imp else 0
        c["ecpm"] = round((rev / imp) * 1000, 6) if imp else 0
        c["cpm"] = c["ecpm"]
        c["updated_at"] = now_iso
        country_rows.append(c)

    breakdown_rows_list = list(breakdown_rows.values())

    if not daily and not country_rows and not breakdown_rows_list:
        return 0

    client = _supabase_client()
    total = 0
    for i in range(0, len(daily_rows), BATCH_SIZE):
        batch = daily_rows[i:i + BATCH_SIZE]
        resp = write_with_retry(lambda: client.table("ad_unit_daily_stats").upsert(
            batch, on_conflict="network_code,ad_unit_id,website_name,date"
        ).execute())
        total += len(resp.data) if resp.data else 0

    for i in range(0, len(country_rows), BATCH_SIZE):
        batch = country_rows[i:i + BATCH_SIZE]
        resp = write_with_retry(lambda: client.table("ad_unit_country_daily_stats").upsert(
            batch, on_conflict="network_code,ad_unit_id,website_name,country_code,date"
        ).execute())
        total += len(resp.data) if resp.data else 0

    for i in range(0, len(breakdown_rows_list), BATCH_SIZE):
        batch = breakdown_rows_list[i:i + BATCH_SIZE]
        resp = write_with_retry(lambda: client.table("ad_unit_breakdown_daily_stats").upsert(
            batch, on_conflict="network_code,ad_unit_id,website_name,country_code,device_category,app_name,date"
        ).execute())
        total += len(resp.data) if resp.data else 0

    return total


def write_with_retry(op, attempts: int = WRITE_RETRIES):
    """Run a Supabase write with exponential backoff on transient errors."""
    delay = 1.0
    last_err = None
    for i in range(attempts):
        try:
            return op()
        except Exception as e:
            last_err = e
            if i >= attempts - 1:
                raise
            log.warning("Supabase write failed (%s), retrying in %.1fs...", e, delay)
            time.sleep(delay)
            delay *= 2
    raise last_err


def cleanup_old_data(cutoff_date: str) -> int:
    deleted = 0
    try:
        resp = write_with_retry(lambda: _supabase_client().table("ad_unit_daily_stats").delete().lt("date", cutoff_date).execute())
        deleted += len(resp.data) if resp.data else 0
    except Exception as e:
        log.warning("Cleanup error (ad_unit_daily_stats): %s", e)
    try:
        resp = write_with_retry(lambda: _supabase_client().table("ad_unit_country_daily_stats").delete().lt("date", cutoff_date).execute())
        deleted += len(resp.data) if resp.data else 0
    except Exception as e:
        log.warning("Cleanup error (ad_unit_country_daily_stats): %s", e)
    try:
        resp = write_with_retry(lambda: _supabase_client().table("ad_unit_breakdown_daily_stats").delete().lt("date", cutoff_date).execute())
        deleted += len(resp.data) if resp.data else 0
    except Exception as e:
        log.warning("Cleanup error (ad_unit_breakdown_daily_stats): %s", e)
    if deleted:
        log.info("Cleaned up %d rows older than %s", deleted, cutoff_date)
    return deleted


# ═════════════════════════════════════════════════════════════════
#  SYNC LOGIC
# ═════════════════════════════════════════════════════════════════


def validate_config():
    missing = []
    if not OLD_SUPABASE_URL:
        missing.append("OLD_SUPABASE_URL")
    if not OLD_SUPABASE_SERVICE_KEY:
        missing.append("OLD_SUPABASE_SERVICE_KEY")
    if not SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not SUPABASE_SERVICE_KEY:
        missing.append("SUPABASE_SERVICE_KEY")
    if not GAM_CLIENT_EMAIL:
        missing.append("GAM_CLIENT_EMAIL")
    if not GAM_PRIVATE_KEY:
        missing.append("GAM_PRIVATE_KEY")
    if missing:
        log.error("Missing required env vars: %s", ", ".join(missing))
        log.error("Copy .env.example to .env and fill in your credentials")
        sys.exit(1)


def run_sync_cycle() -> dict:
    start_time = time.time()
    end_date = get_date_days_ago(0)
    start_date, end_date = compute_sync_range()

    token = get_gam_token_cached()

    # 1. Load website → ad unit path mapping from the OLD DB
    websites = fetch_websites()
    lookup = build_path_lookup(websites)
    log.info("Loaded %d websites with %d known ad unit path(s) from OLD DB", len(websites), len(lookup))

    if not lookup:
        return {"status": "skipped", "reason": "no ad unit paths", "elapsed": time.time() - start_time}

    # 2. Fetch the network's ad unit inventory, keep only the ones matching our sites,
    #    and filter the report to just those (scales to thousands of units).
    inventory = fetch_ad_units(GAM_NETWORK_CODE, token)
    wanted_ids = []
    for alias, site in lookup.items():
        for uid in inventory.get(alias, []):
            if uid not in wanted_ids:
                wanted_ids.append(uid)
    log.info("GAM inventory: %d ad unit name(s); keeping %d IDs matching our sites", len(inventory), len(wanted_ids))

    if not wanted_ids:
        log.warning("No known ad units found in inventory — falling back to full-network report")

    # 3. Fetch + write the report (single network). If the ID filter is rejected,
    #    fall back to an unfiltered report so the sync still completes.
    try:
        job_id = submit_report_job(GAM_NETWORK_CODE, start_date, end_date, token, wanted_ids or None)
    except RuntimeError as e:
        msg = str(e).lower()
        if wanted_ids and any(k in msg for k in ("not a valid value", "statement", "query", "unexecutable")):
            log.warning("Report ID filter rejected, retrying without filter: %s", e)
            job_id = submit_report_job(GAM_NETWORK_CODE, start_date, end_date, token)
        else:
            raise
    status = "IN_PROGRESS"
    for _ in range(REPORT_POLL_MAX):
        time.sleep(2)
        status = get_report_job_status(GAM_NETWORK_CODE, job_id, token)
        if status in ("COMPLETED", "FAILED"):
            break
    if status == "FAILED":
        raise RuntimeError(f"Report job {job_id} failed")
    if status != "COMPLETED":
        raise RuntimeError(f"Report job {job_id} timed out")

    rows = download_report_rows(GAM_NETWORK_CODE, job_id, token)
    log.info("Fetched %d report row(s)", len(rows))

    written = upsert_report_rows(rows, lookup)

    cutoff = get_date_days_ago(RETENTION_DAYS)
    deleted = cleanup_old_data(cutoff)

    elapsed = round(time.time() - start_time, 1)
    stats = {
        "status": "completed",
        "range": f"{start_date}..{end_date}",
        "inventory_units": len(inventory),
        "report_rows": len(rows),
        "rows_written": written,
        "rows_deleted": deleted,
        "elapsed_seconds": elapsed,
    }
    log.info("Mi reporting cycle complete: %s", json.dumps(stats))
    return stats


def main():
    validate_config()
    once = "--once" in sys.argv

    global SYNC_INTERVAL
    for arg in sys.argv:
        if arg.startswith("--interval="):
            SYNC_INTERVAL = int(arg.split("=")[1])

    log.info("=" * 60)
    log.info("Mi Reporting Bot starting")
    log.info("Interval: %d min | Lookback: %d days | Retention: %d days", SYNC_INTERVAL, LOOKBACK_DAYS, RETENTION_DAYS)
    log.info("GAM network: %s | OLD DB: %s | NEW DB: %s", GAM_NETWORK_CODE, OLD_SUPABASE_URL, SUPABASE_URL)
    log.info("=" * 60)

    cycle = 0
    while not shutdown_flag.is_set():
        cycle += 1
        log.info("─── Cycle %d ─────────────────────────────────────", cycle)
        try:
            run_sync_cycle()
        except Exception as e:
            log.exception("Unhandled error in sync cycle: %s", e)

        if once:
            break

        for _ in range((SYNC_INTERVAL * 60) // 5):
            if shutdown_flag.wait(5):
                break

    log.info("Shutdown complete.")


if __name__ == "__main__":
    main()
