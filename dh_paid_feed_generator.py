#!/usr/bin/env python3
import re
import datetime
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import PyRSS2Gen
import xml.dom.minidom
from xml.sax.saxutils import escape

# Import mapping functions and data from your mappings file
from dh_mappings import (
    TRANSLATOR_NOVEL_MAP,
    get_novel_url,
    get_featured_image,
    get_translator,
    get_discord_role_id,
    get_nsfw_novels
)

# Limit concurrent fetches
tz = datetime.timezone.utc
semaphore = asyncio.Semaphore(100)

# ---------------- Helper Functions ----------------

def slugify(text: str) -> str:
    """Convert arbitrary text into a URL-friendly slug."""
    text = text.lower()
    # remove punctuation except unicode word characters and spaces
    text = re.sub(r"[^\w\s-]", "", text)
    # collapse whitespace
    text = re.sub(r"[\s]+", "-", text.strip())
    return text


def clean_description(raw_desc):
    soup = BeautifulSoup(raw_desc, "html.parser")
    for div in soup.find_all("div", class_="c-content-readmore"):
        div.decompose()
    cleaned = soup.decode_contents()
    return re.sub(r'\s+', ' ', cleaned).strip()


def split_title(full_title):
    parts = full_title.split(" - ", 1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return full_title.strip(), ""


def extract_pubdate_from_soup(chap):
    span = chap.find("span", class_="chapter-release-date")
    if span and span.find("i"):
        date_str = span.find("i").get_text(strip=True)
        try:
            return datetime.datetime.strptime(date_str, "%B %d, %Y").replace(tzinfo=tz)
        except:
            now = datetime.datetime.now(tz)
            parts = date_str.lower().split()
            if parts and parts[0].isdigit():
                num = int(parts[0]); unit = parts[1]
                if "minute" in unit:
                    return now - datetime.timedelta(minutes=num)
                if "hour"   in unit:
                    return now - datetime.timedelta(hours=num)
                if "day"    in unit:
                    return now - datetime.timedelta(days=num)
                if "week"   in unit:
                    return now - datetime.timedelta(weeks=num)
    return datetime.datetime.now(tz)


def chapter_num(chaptername):
    nums = re.findall(r'\d+(?:\.\d+)?', chaptername)
    if not nums:
        return (0,)
    return tuple(float(n) if '.' in n else int(n) for n in nums)


def normalize_date(dt):
    return dt.replace(microsecond=0)

# ---------------- Async Fetch ----------------

async def fetch_page(session, url):
    async with session.get(url) as resp:
        resp.raise_for_status()
        return await resp.text()

async def novel_has_paid_update_async(session, novel_url):
    try:
        html = await fetch_page(session, novel_url)
    except:
        return False
    soup = BeautifulSoup(html, "html.parser")
    first = soup.find("li", class_="wp-manga-chapter")
    if first and "premium" in first.get("class", []) and "free-chap" not in first.get("class", []):
        pub = extract_pubdate_from_soup(first)
        if pub >= datetime.datetime.now(tz) - datetime.timedelta(days=7):
            return True
    return False

async def scrape_paid_chapters_async(session, novel_url):
    html = await fetch_page(session, novel_url)
    soup = BeautifulSoup(html, "html.parser")
    desc_div = soup.find("div", class_="description-summary")
    main_desc = clean_description(desc_div.decode_contents()) if desc_div else ""

    paid_chapters = []
    now = datetime.datetime.now(tz)

    # Volume‑based listing
    vol_container = soup.select_one("ul.main.version-chap.volumns")
    if vol_container:
        for parent in vol_container.select("li.parent.has-child"):
            vol_full = parent.select_one("a.has-child").get_text(strip=True)
            vol_slug = slugify(vol_full)
            for chap_li in parent.select("ul.sub-chap-list li.wp-manga-chapter"):
                if "free-chap" in chap_li.get("class", []):
                    continue
                pub_dt = extract_pubdate_from_soup(chap_li)
                if pub_dt < now - datetime.timedelta(days=7):
                    continue
                a_tag = chap_li.find("a")
                if not a_tag:
                    continue
                raw_title = a_tag.get_text(" ", strip=True)
                chap_number, nameext = split_title(raw_title)
                # slugify chapter
                chap_slug = slugify(chap_number)
                href = a_tag.get("href", "").strip()
                if href and href != "#":
                    link = href
                else:
                    link = f"{novel_url}{vol_slug}/{chap_slug}/"
                guid = next((c.split("data-chapter-")[1] for c in chap_li.get("class", []) if c.startswith("data-chapter-")), chap_slug)
                coin = chap_li.find("span", class_="coin").get_text(strip=True) if chap_li.find("span", class_="coin") else ""
                paid_chapters.append({
                    "volume": vol_full,
                    "chaptername": chap_number,
                    "nameextend": nameext,
                    "link": link,
                    "description": main_desc,
                    "pubDate": pub_dt,
                    "guid": guid,
                    "coin": coin
                })

    # No‑volume listing
    no_vol = soup.select_one("ul.main.version-chap.no-volumn")
    if no_vol:
        for chap_li in no_vol.select("li.wp-manga-chapter"):
            if "free-chap" in chap_li.get("class", []):
                continue
            pub_dt = extract_pubdate_from_soup(chap_li)
            if pub_dt < now - datetime.timedelta(days=7):
                continue
            a_tag = chap_li.find("a")
            if not a_tag:
                continue
            raw_title = a_tag.get_text(" ", strip=True)
            chap_number, nameext = split_title(raw_title)
            chap_slug = slugify(chap_number)
            href = a_tag.get("href", "").strip()
            if href and href != "#":
                link = href
            else:
                link = f"{novel_url}{chap_slug}/"
            guid = next((c.split("data-chapter-")[1] for c in chap_li.get("class", []) if c.startswith("data-chapter-")), chap_slug)
            coin = chap_li.find("span", class_="coin").get_text(strip=True) if chap_li.find("span", class_="coin") else ""
            paid_chapters.append({
                "volume": "",
                "chaptername": chap_number,
                "nameextend": nameext,
                "link": link,
                "description": main_desc,
                "pubDate": pub_dt,
                "guid": guid,
                "coin": coin
            })

    return paid_chapters, main_desc

# ---------------- RSS Classes ----------------

class MyRSSItem(PyRSS2Gen.RSSItem):
    def __init__(self, *args, volume="", chaptername="", nameextend="", coin="", **kwargs):
        self.volume = volume
        self.chaptername = chaptername
        self.nameextend = nameextend
        self.coin = coin
        super().__init__(*args#,(Note truncated)...
