import os
import re
import time
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
import feedparser
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from xml.etree.ElementTree import Element, SubElement, tostring

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# -------------------- CONFIG --------------------

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "application/rss+xml,application/atom+xml,application/xml;q=0.9,text/xml;q=0.8,*/*;q=0.7",
    "Accept-Language": "ru,en;q=0.8",
    "Connection": "keep-alive",
}

TIMEOUT = 25

PER_ORG = 3
TOTAL_LIMIT = 200
SLEEP_LIST = 0.25
SLEEP_ARTICLE = 0.2

OUT_XML = "docs/extra.xml"
OUT_TITLE = "Новости учреждений доп. образования Иркутска"
OUT_LINK = "https://eduirk.ru/"

LOCAL_TZ = timezone(timedelta(hours=8))

NEWS_WORDS = ("новост", "событ", "объяв", "меропр", "пресс", "news", "event")

COMMON_FEEDS = (
    "feed/", "feed", "rss.xml", "rss", "rss/", "atom", "atom/",
    "?format=feed&type=rss", "?format=feed&type=atom",
)

# Tor (опционально): включается через env USE_TOR=1
USE_TOR = os.getenv("USE_TOR", "").strip() in ("1", "true", "yes", "on")
TOR_PROXY = "socks5h://127.0.0.1:9050"
TOR_PROXIES = {"http": TOR_PROXY, "https": TOR_PROXY}

# -------------------- PARSE HELPERS --------------------

DATE_RE_1 = re.compile(r"(\d{1,2})\s+([А-Яа-яёЁ]+)\s+(\d{4}),\s*(\d{1,2}):(\d{2})")
DATE_RE_2 = re.compile(r"(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})\s+(\d{1,2}):(\d{2})")

RU_MONTHS = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}


@dataclass
class Source:
    name: str
    home_url: str
    news_url: Optional[str] = None
    feed_url: Optional[str] = None


def first_text(el) -> str:
    return el.get_text(" ", strip=True) if el else ""


def strip_html(s: str) -> str:
    if not s:
        return ""
    return BeautifulSoup(s, "html.parser").get_text(" ", strip=True)


def rss_date_now() -> str:
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


def to_rfc822(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


def to_iso8601(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def norm_host_from_url(url: str) -> str:
    host = (urlparse(url).netloc or "").lower().strip()
    host = host.lstrip("www.")
    try:
        host = host.encode("idna").decode("ascii")
    except Exception:
        pass
    return host


def is_same_site(url: str, home_url: str) -> bool:
    a = norm_host_from_url(url)
    e = norm_host_from_url(home_url)
    if not a or not e:
        return False
    return a == e or a.endswith("." + e)


def _resp_content(resp) -> bytes:
    try:
        return resp.content
    except Exception:
        pass
    try:
        return (resp.text or "").encode("utf-8", errors="ignore")
    except Exception:
        return b""


def _resp_text(resp) -> str:
    try:
        return resp.text
    except Exception:
        try:
            return _resp_content(resp).decode("utf-8", errors="ignore")
        except Exception:
            return ""


# -------------------- FETCH (anti-403) --------------------

def _req_requests(url: str, proxies=None):
    return requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True, proxies=proxies)


def _req_curl_cffi(url: str, proxies=None):
    # curl_cffi умеет impersonate (TLS/HTTP2 fingerprint) [web:912]
    from curl_cffi import requests as creq
    # curl_cffi принимает proxies в формате requests
    return creq.get(
        url,
        headers=HEADERS,
        timeout=TIMEOUT,
        allow_redirects=True,
        impersonate="chrome",
        proxies=proxies,
    )


def _req_cloudscraper(url: str, proxies=None):
    # cloudscraper пытается обходить Cloudflare anti-bot [web:881]
    import cloudscraper
    s = cloudscraper.create_scraper()
    return s.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True, proxies=proxies)


def _looks_like_feed(resp) -> bool:
    status = getattr(resp, "status_code", 0)
    if status >= 400:
        return False

    ctype = ""
    try:
        ctype = (resp.headers.get("content-type") or "").lower()
    except Exception:
        pass

    raw = _resp_content(resp) or b""
    head = raw[:2000].lstrip().lower()

    # 1) Быстрая проверка по Content-Type
    if any(x in ctype for x in ("rss", "atom", "xml")):
        # но всё равно отсекаем явный HTML
        if b"<html" in head or b"<!doctype html" in head:
            return False
        return True

    # 2) Если Content-Type "левый" — проверяем по содержимому
    if head.startswith(b"<?xml") or b"<rss" in head or b"<feed" in head:
        return True

    return False


def _looks_like_html(resp) -> bool:
    raw = _resp_content(resp) or b""
    head = raw[:400].lstrip().lower()
    return b"<html" in head or b"<!doctype html" in head


def fetch_url(url: str, *, org_name: str = "", kind: str = "page"):
    """
    Порядок проб:
      direct: requests -> curl_cffi -> cloudscraper
      tor (если USE_TOR=1): curl_cffi -> cloudscraper -> requests
    Для kind='feed' успехом считается только ответ, похожий на RSS/Atom/XML.
    """
    attempts = []

    # direct
    attempts.append(("requests", None))
    attempts.append(("curl_cffi", None))
    attempts.append(("cloudscraper", None))

    # tor: начинаем с curl_cffi, т.к. у вас он уже показал лучший результат на home (200, большой HTML)
    if USE_TOR:
        attempts.append(("curl_cffi", TOR_PROXIES))
        attempts.append(("cloudscraper", TOR_PROXIES))
        attempts.append(("requests", TOR_PROXIES))

    last_exc = None

    for method, proxies in attempts:
        proxy_tag = "tor" if proxies else "direct"
        try:
            if method == "requests":
                resp = _req_requests(url, proxies=proxies)
            elif method == "curl_cffi":
                resp = _req_curl_cffi(url, proxies=proxies)
            else:
                resp = _req_cloudscraper(url, proxies=proxies)

            status = getattr(resp, "status_code", 0)
            ctype = ""
            try:
                ctype = resp.headers.get("content-type", "")
            except Exception:
                pass

            raw = _resp_content(resp) or b""
            print(f"[{org_name}] fetch {kind} method={method}/{proxy_tag} url={url} status={status} ctype={ctype} len={len(raw)}")

            # 403 — всегда пробуем дальше
            if status == 403:
                continue

            # Для FEED: если пришёл HTML (даже 200) — это блок-страница, пробуем дальше
            if kind == "feed":
                if _looks_like_feed(resp):
                    return resp
                else:
                    # полезно для диагностики: видим, что отдали "мягкий блок"
                    if status < 400 and _looks_like_html(resp):
                        print(f"[{org_name}] feed looks like HTML-block page (status={status}), trying next method...")
                    continue

            # Для HOME/PAGE/ARTICLE: любой не-403 ответ подходит
            return resp

        except Exception as e:
            last_exc = e
            print(f"[{org_name}] fetch {kind} ERROR method={method}/{proxy_tag} url={url} err={e}")

    if last_exc:
        raise last_exc
    raise RuntimeError("fetch_url: all methods failed/blocked")



# -------------------- SCRAPE LOGIC --------------------

def pick_news_page(home_url: str, explicit_news_url: Optional[str], org_name: str) -> str:
    if explicit_news_url:
        return explicit_news_url
    try:
        resp = fetch_url(home_url, org_name=org_name, kind="home")
        soup = BeautifulSoup(_resp_text(resp), "html.parser")
        for a in soup.find_all("a", href=True):
            txt = (a.get_text(" ", strip=True) or "").lower()
            href = (a.get("href") or "").strip()
            if not href:
                continue
            cand = urljoin(home_url, href)
            if not is_same_site(cand, home_url):
                continue
            if any(w in txt for w in NEWS_WORDS) or any(w in href.lower() for w in NEWS_WORDS):
                return cand
    except Exception:
        pass
    return home_url


def detect_feed_urls(page_url: str, home_url: str, org_name: str) -> list[str]:
    found: list[str] = []
    seen = set()

    try:
        resp = fetch_url(page_url, org_name=org_name, kind="page")
        soup = BeautifulSoup(_resp_text(resp), "html.parser")
        for link in soup.find_all("link", href=True):
            rel = " ".join(link.get("rel") or []).lower()
            typ = (link.get("type") or "").lower()
            if "alternate" in rel and ("rss" in typ or "atom" in typ or "xml" in typ):
                u = urljoin(page_url, link["href"])
                if is_same_site(u, home_url) and u not in seen:
                    seen.add(u)
                    found.append(u)
    except Exception:
        pass

    base = page_url.rstrip("/") + "/"
    for tail in COMMON_FEEDS:
        u = urljoin(base, tail)
        if is_same_site(u, home_url) and u not in seen:
            seen.add(u)
            found.append(u)

    return found


def parse_date_from_text(text: str) -> Optional[datetime]:
    if not text:
        return None

    m = DATE_RE_1.search(text)
    if m:
        day = int(m.group(1))
        mon = RU_MONTHS.get(m.group(2).lower())
        year = int(m.group(3))
        hour = int(m.group(4))
        minute = int(m.group(5))
        if mon:
            return datetime(year, mon, day, hour, minute, tzinfo=LOCAL_TZ)

    m = DATE_RE_2.search(text)
    if m:
        day = int(m.group(1))
        mon = int(m.group(2))
        year = int(m.group(3))
        hour = int(m.group(4))
        minute = int(m.group(5))
        if 1 <= mon <= 12:
            return datetime(year, mon, day, hour, minute, tzinfo=LOCAL_TZ)

    return None


def parse_from_feed(feed_url: str, org_name: str, home_url: str) -> list[dict]:
    try:
        resp = fetch_url(feed_url, org_name=org_name, kind="feed")
        status = getattr(resp, "status_code", 0)
        raw = _resp_content(resp) or b""

        if status >= 400:
            return []

        head = raw[:400].lower()
        if b"<html" in head or b"<!doctype html" in head:
            return []

        d = feedparser.parse(raw)
        if getattr(d, "bozo", False):
            print(f"[{org_name}] feed bozo=True url={feed_url} err={getattr(d, 'bozo_exception', None)}")
        print(f"[{org_name}] feed entries={len(getattr(d, 'entries', []) or [])} url={feed_url}")

    except Exception as e:
        print(f"[{org_name}] feed EXCEPTION url={feed_url} err={e}")
        return []

    base = (getattr(d, "feed", {}) or {}).get("link") or feed_url

    items: list[dict] = []
    for e in getattr(d, "entries", []) or []:
        link = (e.get("link") or "").strip()
        if link:
            link = urljoin(base, link)

        if not link or not is_same_site(link, home_url):
            continue

        raw_title = (e.get("title") or "").strip()
        summary = strip_html(e.get("summary") or e.get("description") or "")

        if not raw_title and summary:
            raw_title = summary[:80].rstrip() + ("…" if len(summary) > 80 else "")

        pub_dt = None
        tt = getattr(e, "published_parsed", None) or getattr(e, "updated_parsed", None)
        if tt:
            try:
                pub_dt = datetime(*tt[:6], tzinfo=timezone.utc)
            except Exception:
                pub_dt = None

        items.append({
            "title": f"[{org_name}] {raw_title}" if raw_title else f"[{org_name}] Новость",
            "link": link,
            "description": (summary[:600] + ("…" if len(summary) > 600 else "")) if summary else "Новость",
            "_dt": pub_dt,
        })

        if len(items) >= PER_ORG:
            break

    return items


def make_rss(items: list[dict], out_title: str, out_link: str) -> bytes:
    rss = Element("rss", version="2.0", attrib={"xmlns:dc": "http://purl.org/dc/elements/1.1/"})
    ch = SubElement(rss, "channel")
    SubElement(ch, "title").text = out_title
    SubElement(ch, "link").text = out_link
    SubElement(ch, "description").text = out_title
    SubElement(ch, "lastBuildDate").text = rss_date_now()

    for it in items:
        item = SubElement(ch, "item")
        SubElement(item, "title").text = it.get("title") or "Новость"
        SubElement(item, "link").text = it["link"]
        SubElement(item, "guid").text = it["link"]

        dt = it.get("_dt")
        if isinstance(dt, datetime):
            pub_rfc = to_rfc822(dt)
            pub_iso = to_iso8601(dt)
        else:
            now = datetime.now(timezone.utc)
            pub_rfc = to_rfc822(now)
            pub_iso = to_iso8601(now)

        SubElement(item, "pubDate").text = pub_rfc
        SubElement(item, "dc:date").text = pub_iso
        SubElement(item, "description").text = it.get("description") or ""

    return b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(rss, encoding="utf-8")


def read_sources(path: str) -> list[Source]:
    """
    Формат строки:
      name | home_url | news_url(optional) | feed_url(optional)
    """
    out: list[Source] = []
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split("|")]
            name = parts[0] if len(parts) > 0 else ""
            home = parts[1] if len(parts) > 1 else ""
            news = parts[2] if len(parts) > 2 and parts[2] else None
            feed = parts[3] if len(parts) > 3 and parts[3] else None
            if name and home:
                out.append(Source(name=name, home_url=home, news_url=news, feed_url=feed))
    return out


def main():
    sources = read_sources("sources.txt")

    all_items: list[dict] = []
    rejected_external = 0

    for src in sources:
        home_url = src.home_url
        news_page = pick_news_page(home_url, src.news_url, org_name=src.name)

        got: list[dict] = []
        via = "none"

        feed_urls: list[str] = []
        if src.feed_url:
            feed_urls.append(src.feed_url)

        feed_urls.extend(detect_feed_urls(news_page, home_url, org_name=src.name))
        if home_url != news_page:
            feed_urls.extend(detect_feed_urls(home_url, home_url, org_name=src.name))

        uniq_fu: list[str] = []
        seen_fu = set()
        for fu in feed_urls:
            if fu not in seen_fu:
                seen_fu.add(fu)
                uniq_fu.append(fu)

        print(f"[{src.name}] detected_feeds={len(uniq_fu)} -> {uniq_fu[:6]}")

        for fu in uniq_fu:
            got = parse_from_feed(fu, src.name, home_url)
            if got:
                via = "feed"
                break

        # финальная фильтрация: только тот же сайт
        filtered: list[dict] = []
        for it in got:
            if it.get("link") and is_same_site(it["link"], home_url):
                filtered.append(it)
            else:
                rejected_external += 1

        all_items.extend(filtered)

        print(f"[{src.name}] via={via} items={len(filtered)} home={home_url} news_page={news_page}")
        time.sleep(SLEEP_LIST)

    # дедуп по ссылке
    dedup: list[dict] = []
    seen = set()
    for it in all_items:
        link = it.get("link") or ""
        if link and link not in seen:
            seen.add(link)
            dedup.append(it)

    # сортировка: новые сверху (без даты — вниз)
    min_dt = datetime.min.replace(tzinfo=timezone.utc)

    def key_dt(it):
        dt = it.get("_dt")
        if isinstance(dt, datetime):
            return dt.astimezone(timezone.utc)
        return min_dt

    dedup.sort(key=key_dt, reverse=True)

    rss_bytes = make_rss(dedup[:TOTAL_LIMIT], OUT_TITLE, OUT_LINK)

    with open(OUT_XML, "wb") as f:
        f.write(rss_bytes)

    print(f"Sources: {len(sources)}")
    print(f"Items total: {len(all_items)}; unique: {len(dedup)}")
    print(f"Rejected external links: {rejected_external}")
    print(f"Output: {OUT_XML}")


if __name__ == "__main__":
    main()
