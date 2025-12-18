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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru,en;q=0.8",
    "Connection": "keep-alive",
}
TIMEOUT = 25

PER_ORG = 3
TOTAL_LIMIT = 200
SLEEP_LIST = 0.3
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
    # совместимость: requests.Response, curl_cffi response, cloudscraper response
    try:
        return resp.content  # requests/cloudscraper
    except Exception:
        pass
    try:
        return resp.read()  # на всякий случай
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


def fetch_url(url: str):
    """Многоступенчатая загрузка: requests -> curl_cffi (impersonate) -> cloudscraper."""
    # 1) обычный requests
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        if r.status_code != 403:
            return r
    except Exception as e:
        print(f"[fetch] requests error url={url} err={e}")
        r = None

    # 2) curl_cffi (TLS/JA3 impersonation) [web:886]
    try:
        from curl_cffi import requests as creq
        r2 = creq.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True, impersonate="chrome120")
        if getattr(r2, "status_code", 0) != 403:
            return r2
    except Exception as e:
        print(f"[fetch] curl_cffi error url={url} err={e}")

    # 3) cloudscraper (Cloudflare обход) [web:881]
    try:
        import cloudscraper
        s = cloudscraper.create_scraper()
        r3 = s.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        return r3
    except Exception as e:
        print(f"[fetch] cloudscraper error url={url} err={e}")

    # fallback
    if r is not None:
        return r
    raise RuntimeError("All fetch methods failed")


def pick_news_page(home_url: str, explicit_news_url: Optional[str]) -> str:
    if explicit_news_url:
        return explicit_news_url

    try:
        resp = fetch_url(home_url)
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


def detect_feed_urls(page_url: str, home_url: str) -> list[str]:
    found: list[str] = []
    seen = set()

    try:
        resp = fetch_url(page_url)
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
        try:
            u = urljoin(base, tail)
            if is_same_site(u, home_url) and u not in seen:
                seen.add(u)
                found.append(u)
        except Exception:
            continue

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


def parse_date_from_page(soup: BeautifulSoup, fallback_text: str) -> Optional[datetime]:
    for t in soup.find_all("time"):
        dtv = (t.get("datetime") or "").strip()
        if dtv:
            try:
                return datetime.fromisoformat(dtv.replace("Z", "+00:00"))
            except Exception:
                pass

    meta_keys = [
        ("property", "article:published_time"),
        ("property", "article:modified_time"),
        ("name", "date"),
        ("name", "pubdate"),
        ("itemprop", "datePublished"),
        ("itemprop", "dateCreated"),
        ("itemprop", "dateModified"),
    ]
    for attr, val in meta_keys:
        m = soup.find("meta", attrs={attr: val})
        if m and m.get("content"):
            c = (m["content"] or "").strip()
            try:
                return datetime.fromisoformat(c.replace("Z", "+00:00"))
            except Exception:
                pass

    return parse_date_from_text(fallback_text)


def clean_container(container: BeautifulSoup) -> None:
    drop_selectors = [
        ".breadcrumb", "nav[aria-label='breadcrumb']", "ol.breadcrumb", "ul.breadcrumb", ".gw-breadcrumbs",
        "header", "footer", "nav", "aside",
        ".bvi-panel", ".bvi-body", ".bvi-open", ".special-version", ".visually-impaired",
    ]
    for sel in drop_selectors:
        for el in container.select(sel):
            el.decompose()


def parse_article(article_url: str, org_name: str, home_url: str) -> Optional[dict]:
    resp = fetch_url(article_url)

    # final_url для requests/cloudscraper/curl_cffi может называться по-разному
    final_url = getattr(resp, "url", None) or article_url

    if not is_same_site(final_url, home_url):
        return None

    soup = BeautifulSoup(_resp_text(resp), "html.parser")

    h1_title = first_text(soup.find("h1")) or "Новость"
    title = f"[{org_name}] {h1_title}".strip()

    h1 = soup.find("h1")
    container = h1
    for _ in range(10):
        if not container:
            break
        txt = first_text(container)
        if len(txt) > 250:
            break
        container = container.parent

    if not container:
        container = soup.find("main") or soup.body or soup

    clean_container(container)
    text = first_text(container)

    pos = text.find(h1_title)
    if pos != -1:
        text = text[pos:]
    if text.startswith(h1_title):
        text = text[len(h1_title):].lstrip(" \t\r\n-–—:|")

    pub_dt = parse_date_from_page(soup, text)
    desc = text[:600] + ("…" if len(text) > 600 else "")
    if not desc:
        desc = h1_title or "Новость"

    return {
        "title": title if title else f"[{org_name}] Новость",
        "link": final_url,
        "description": desc,
        "_dt": pub_dt,
    }


def parse_from_feed(feed_url: str, org_name: str, home_url: str) -> list[dict]:
    try:
        resp = fetch_url(feed_url)
        status = getattr(resp, "status_code", 0)
        final = getattr(resp, "url", None) or feed_url
        ctype = ""
        try:
            ctype = (resp.headers.get("content-type") or "")
        except Exception:
            ctype = ""

        raw = _resp_content(resp) or b""
        print(
            f"[{org_name}] fetch feed url={feed_url} status={status} final={final} "
            f"ctype={ctype} len={len(raw)}"
        )

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


def parse_from_html(news_page_url: str, org_name: str, home_url: str) -> list[dict]:
    try:
        resp = fetch_url(news_page_url)
    except Exception:
        return []

    final_url = getattr(resp, "url", None) or news_page_url
    if not is_same_site(final_url, home_url):
        return []

    soup = BeautifulSoup(_resp_text(resp), "html.parser")

    candidates: list[str] = []
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue

        full = urljoin(final_url, href)
        if not is_same_site(full, home_url):
            continue

        h = full.lower()
        if any(w in h for w in NEWS_WORDS) or "article" in h or "post" in h or "/202" in h:
            candidates.append(full)

    uniq: list[str] = []
    seen = set()
    for u in candidates:
        if u not in seen:
            seen.add(u)
            uniq.append(u)

    items: list[dict] = []
    for u in uniq[:PER_ORG * 6]:
        try:
            it = parse_article(u, org_name=org_name, home_url=home_url)
            time.sleep(SLEEP_ARTICLE)
            if not it:
                continue
            items.append(it)
            if len(items) >= PER_ORG:
                break
        except Exception:
            continue

    return items


def make_rss(items: list[dict], out_title: str, out_link: str) -> bytes:
    rss = Element(
        "rss",
        version="2.0",
        attrib={"xmlns:dc": "http://purl.org/dc/elements/1.1/"},
    )
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
        news_page = pick_news_page(home_url, src.news_url)

        got: list[dict] = []
        via = "none"

        feed_urls: list[str] = []
        if src.feed_url:
            feed_urls.append(src.feed_url)

        feed_urls.extend(detect_feed_urls(news_page, home_url))
        if home_url != news_page:
            feed_urls.extend(detect_feed_urls(home_url, home_url))

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

        if not got:
            got = parse_from_html(news_page, src.name, home_url)
            if got:
                via = "html"

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
