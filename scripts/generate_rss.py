import re
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin

import requests
import feedparser
from bs4 import BeautifulSoup
from xml.etree.ElementTree import Element, SubElement, tostring

HEADERS = {"User-Agent": "Mozilla/5.0"}
TIMEOUT = 25

PER_ORG = 3
TOTAL_LIMIT = 200  # с запасом (15*3=45)
SLEEP_LIST = 0.3
SLEEP_ARTICLE = 0.2

OUT_XML = "docs/extra.xml"
OUT_TITLE = "Новости учреждений доп. образования Иркутска"
OUT_LINK = "https://eduirk.ru/"

# Иркутск UTC+8 (используем для распознавания дат из текста)
LOCAL_TZ = timezone(timedelta(hours=8))

# Часто встречается в текстах: "11 декабря 2025, 10:28"
DATE_RE = re.compile(r"(\d{1,2})\s+([А-Яа-яёЁ]+)\s+(\d{4}),\s*(\d{1,2}):(\d{2})")
RU_MONTHS = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}

# Ключевые слова для авто-поиска страницы новостей
NEWS_WORDS = ("новост", "событ", "объяв", "меропр", "пресс", "news", "event")

# Частые RSS endpoints (без гарантий, но часто помогает)
COMMON_FEEDS = (
    "feed/", "feed", "rss.xml", "rss", "rss/", "atom", "atom/",
    "?format=feed&type=rss", "?format=feed&type=atom",
)

def fetch(url: str) -> str:
    return requests.get(url, headers=HEADERS, timeout=TIMEOUT).text

def first_text(el) -> str:
    return el.get_text(" ", strip=True) if el else ""

def rss_date_now() -> str:
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")

def to_rfc822(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")

def strip_html(s: str) -> str:
    if not s:
        return ""
    return BeautifulSoup(s, "html.parser").get_text(" ", strip=True)

def detect_feed_urls(page_url: str) -> list[str]:
    """Ищем RSS/Atom через <link rel=alternate ...>, плюс пробуем common endpoints."""
    found = set()
    try:
        html = fetch(page_url)
        soup = BeautifulSoup(html, "html.parser")
        for link in soup.find_all("link", href=True):
            rel = " ".join(link.get("rel") or []).lower()
            typ = (link.get("type") or "").lower()
            if "alternate" in rel and ("rss" in typ or "atom" in typ or "xml" in typ):
                found.add(urljoin(page_url, link["href"]))
    except Exception:
        pass

    # fallback: common endpoints
    for tail in COMMON_FEEDS:
        try:
            found.add(urljoin(page_url.rstrip("/") + "/", tail))
        except Exception:
            continue

    return list(found)

def pick_news_page(home_url: str, explicit_news_url: str | None) -> str:
    """Если news_url есть — используем его, иначе пытаемся найти ссылку на 'Новости' на главной."""
    if explicit_news_url:
        return explicit_news_url

    try:
        html = fetch(home_url)
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            txt = (a.get_text(" ", strip=True) or "").lower()
            href = (a.get("href") or "").strip()
            if not href:
                continue
            if any(w in txt for w in NEWS_WORDS) or any(w in href.lower() for w in NEWS_WORDS):
                return urljoin(home_url, href)
    except Exception:
        pass

    return home_url  # fallback: новости могут быть прямо на главной

def parse_date_from_text(text: str) -> datetime | None:
    m = DATE_RE.search(text or "")
    if not m:
        return None
    day = int(m.group(1))
    mon = RU_MONTHS.get(m.group(2).lower())
    year = int(m.group(3))
    hour = int(m.group(4))
    minute = int(m.group(5))
    if not mon:
        return None
    return datetime(year, mon, day, hour, minute, tzinfo=LOCAL_TZ)

def parse_article(article_url: str, org_name: str) -> dict:
    html_doc = fetch(article_url)
    soup = BeautifulSoup(html_doc, "html.parser")

    h1_title = first_text(soup.find("h1")) or "Новость"
    title = f"[{org_name}] {h1_title}"

    # Контейнер вокруг h1 (эвристика)
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
        container = soup.find("main") or soup.body

    # Удаляем типовые “не-статья” блоки
    drop_selectors = [
        ".breadcrumb", "nav[aria-label='breadcrumb']", "ol.breadcrumb", "ul.breadcrumb", ".gw-breadcrumbs",
        "header", "footer", "nav", "aside",
        ".bvi-panel", ".bvi-body", ".bvi-open", ".special-version", ".visually-impaired",
    ]
    for sel in drop_selectors:
        for el in container.select(sel):
            el.decompose()

    text = first_text(container)

    # Режем всё до заголовка (если заголовок попал в текст)
    pos = text.find(h1_title)
    if pos != -1:
        text = text[pos:]
    if text.startswith(h1_title):
        text = text[len(h1_title):].lstrip(" \t\r\n-–—:|")

    pub_dt = parse_date_from_text(text)
    desc = text[:600] + ("…" if len(text) > 600 else "")

    return {
        "title": title if title.strip() else f"[{org_name}] Новость",
        "link": article_url,
        "description": desc or h1_title or "Новость",
        "pubDate": to_rfc822(pub_dt) if pub_dt else None,
        "_dt": pub_dt,
    }

def parse_from_feed(feed_url: str, org_name: str) -> list[dict]:
    d = feedparser.parse(feed_url)
    items = []
    for e in d.entries[:PER_ORG]:
        link = e.get("link") or ""
        raw_title = (e.get("title") or "").strip()
        summary = strip_html(e.get("summary") or e.get("description") or "")
        if not raw_title and summary:
            raw_title = summary[:80].rstrip() + ("…" if len(summary) > 80 else "")

        # Дата из feedparser (если есть)
        pub_dt = None
        if getattr(e, "published_parsed", None):
            pub_dt = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)

        items.append({
            "title": f"[{org_name}] {raw_title}" if raw_title else f"[{org_name}] Новость",
            "link": link,
            "description": summary[:600] + ("…" if len(summary) > 600 else "") if summary else "Новость",
            "pubDate": to_rfc822(pub_dt) if pub_dt else None,
            "_dt": pub_dt,
        })
    # убираем пустые ссылки
    return [it for it in items if it["link"]]

def parse_from_html(news_page_url: str, org_name: str) -> list[dict]:
    html_doc = fetch(news_page_url)
    soup = BeautifulSoup(html_doc, "html.parser")

    # Берём кандидаты ссылок на “похоже-на-статью”
    links = []
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(news_page_url, href)
        h = full.lower()
        if any(w in h for w in NEWS_WORDS) or "article" in h or "post" in h:
            links.append(full)

    # уникализируем
    uniq = []
    for u in links:
        if u not in uniq:
            uniq.append(u)

    items = []
    for u in uniq[:PER_ORG * 3]:  # берём с запасом, т.к. часть может быть “не новость”
        try:
            items.append(parse_article(u, org_name=org_name))
            time.sleep(SLEEP_ARTICLE)
            if len(items) >= PER_ORG:
                break
        except Exception:
            continue
    return items

def make_rss(items, out_title, out_link):
    rss = Element("rss", version="2.0")
    ch = SubElement(rss, "channel")
    SubElement(ch, "title").text = out_title
    SubElement(ch, "link").text = out_link
    SubElement(ch, "description").text = out_title
    SubElement(ch, "lastBuildDate").text = rss_date_now()

    for it in items:
        item = SubElement(ch, "item")
        SubElement(item, "title").text = it["title"]
        SubElement(item, "link").text = it["link"]
        SubElement(item, "guid").text = it["link"]
        SubElement(item, "pubDate").text = it.get("pubDate") or rss_date_now()
        SubElement(item, "description").text = it.get("description") or ""

    return b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(rss, encoding="utf-8")

def read_sources(path: str) -> list[tuple[str, str, str | None]]:
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split("|")]
            name = parts[0]
            home = parts[1] if len(parts) > 1 else ""
            news = parts[2] if len(parts) > 2 and parts[2] else None
            if name and home:
                out.append((name, home, news))
    return out

def main():
    sources = read_sources("sources.txt")  # положите сюда ваш data.txt (переименуйте)
    all_items = []

    for name, home_url, news_url in sources:
        news_page = pick_news_page(home_url, news_url)

        # 1) Пытаемся найти RSS/Atom (на news_page и на home)
        feed_urls = []
        feed_urls.extend(detect_feed_urls(news_page))
        if home_url != news_page:
            feed_urls.extend(detect_feed_urls(home_url))

        # пробуем feed-URL’ы по очереди
        got = []
        for fu in feed_urls:
            try:
                got = parse_from_feed(fu, name)
                if got:
                    break
            except Exception:
                continue

        # 2) Если фидов нет/пусто — HTML
        if not got:
            try:
                got = parse_from_html(news_page, name)
            except Exception:
                got = []

        all_items.extend(got)
        time.sleep(SLEEP_LIST)

    # дедуп по ссылке
    seen = set()
    dedup = []
    for it in all_items:
        if it["link"] and it["link"] not in seen:
            seen.add(it["link"])
            dedup.append(it)

    # сортировка: новые сверху (если даты нет — в конец)
    min_dt = datetime.min.replace(tzinfo=timezone.utc)
    dedup.sort(key=lambda it: (it.get("_dt") or min_dt).astimezone(timezone.utc), reverse=True)

    for it in dedup:
        it.pop("_dt", None)

    rss_bytes = make_rss(dedup[:TOTAL_LIMIT], OUT_TITLE, OUT_LINK)

    with open(OUT_XML, "wb") as f:
        f.write(rss_bytes)

if __name__ == "__main__":
    main()
