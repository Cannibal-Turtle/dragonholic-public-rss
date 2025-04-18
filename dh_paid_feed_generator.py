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
semaphore = asyncio.Semaphore(100)

# ---------------- Helper Functions ----------------

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
            return datetime.datetime.strptime(date_str, "%B %d, %Y").replace(tzinfo=datetime.timezone.utc)
        except:
            # relative time
            now = datetime.datetime.now(datetime.timezone.utc)
            parts = date_str.lower().split()
            if parts and parts[0].isdigit():
                num = int(parts[0])
                unit = parts[1]
                if "minute" in unit:
                    return now - datetime.timedelta(minutes=num)
                if "hour" in unit:
                    return now - datetime.timedelta(hours=num)
                if "day" in unit:
                    return now - datetime.timedelta(days=num)
                if "week" in unit:
                    return now - datetime.timedelta(weeks=num)
    return datetime.datetime.now(datetime.timezone.utc)


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
        if pub >= datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7):
            return True
    return False

async def scrape_paid_chapters_async(session, novel_url):
    html = await fetch_page(session, novel_url)
    soup = BeautifulSoup(html, "html.parser")
    # main description
    desc_div = soup.find("div", class_="description-summary")
    main_desc = clean_description(desc_div.decode_contents()) if desc_div else ""

    paid_chapters = []
    now = datetime.datetime.now(datetime.timezone.utc)

    # 1) Volume-based listing
    vol_container = soup.select_one("ul.main.version-chap.volumns")
    if vol_container:
        for parent in vol_container.select("li.parent.has-child"):
            vol_full = parent.select_one("a.has-child").get_text(strip=True)
            m = re.match(r'\s*(\d+(?:\.\d+)?)', vol_full)
            vol_id = m.group(1) if m else ""
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
                # chapter id from title
                numm = re.search(r'(\d+(?:\.\d+)?)', chap_number)
                chap_id = numm.group(1) if numm else ""
                href = a_tag.get("href", "").strip()
                if href and href != "#":
                    link = href
                else:
                    link = f"{novel_url}{vol_id}/{chap_id}/"
                # guid from class
                guid = next((c.split("data-chapter-")[1] for c in chap_li.get("class", []) if c.startswith("data-chapter-")), chap_id)
                coin = chap_li.find("span", class_="coin").get_text(strip=True) if chap_li.find("span", class_="coin") else ""
                paid_chapters.append({
                    "volume": vol_id,
                    "chaptername": chap_number,
                    "nameextend": nameext,
                    "link": link,
                    "description": main_desc,
                    "pubDate": pub_dt,
                    "guid": guid,
                    "coin": coin
                })

    # 2) No-volume listing
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
            numm = re.search(r'(\d+(?:\.\d+)?)', chap_number)
            chap_id = numm.group(1) if numm else ""
            href = a_tag.get("href", "").strip()
            if href and href != "#":
                link = href
            else:
                link = f"{novel_url}chapter-{chap_id}/"
            guid = next((c.split("data-chapter-")[1] for c in chap_li.get("class", []) if c.startswith("data-chapter-")), chap_id)
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
        super().__init__(*args, **kwargs)

    def writexml(self, writer, indent="", addindent="", newl=""):
        writer.write(indent + "  <item>" + newl)
        writer.write(indent + "    <title>%s</title>" % escape(self.title) + newl)
        writer.write(indent + "    <volume>%s</volume>" % escape(self.volume) + newl)
        writer.write(indent + "    <chaptername>%s</chaptername>" % escape(self.chaptername) + newl)
        formatted = f"***{self.nameextend}***" if self.nameextend.strip() else ""
        writer.write(indent + "    <nameextend>%s</nameextend>" % escape(formatted) + newl)
        writer.write(indent + "    <link>%s</link>" % escape(self.link) + newl)
        writer.write(indent + "    <description><![CDATA[%s]]></description>" % self.description + newl)
        cat = "NSFW" if self.title in get_nsfw_novels() else "SFW"
        writer.write(indent + "    <category>%s</category>" % escape(cat) + newl)
        trans = get_translator(self.title) or ""
        writer.write(indent + "    <translator>%s</translator>" % escape(trans) + newl)
        role = get_discord_role_id(trans)
        if cat == "NSFW":
            role += " <@&1304077473998442506>"
        writer.write(indent + "    <discord_role_id><![CDATA[%s]]></discord_role_id>" % role + newl)
        writer.write(indent + '    <featuredImage url="%s"/>' % escape(get_featured_image(self.title)) + newl)
        if self.coin:
            writer.write(indent + "    <coin>%s</coin>" % escape(self.coin) + newl)
        writer.write(indent + "    <pubDate>%s</pubDate>" % self.pubDate.strftime("%a, %d %b %Y %H:%M:%S +0000") + newl)
        writer.write(indent + "    <guid isPermaLink=\"false\">%s</guid>" % escape(self.guid.guid) + newl)
        writer.write(indent + "  </item>" + newl)

class CustomRSS2(PyRSS2Gen.RSS2):
    def writexml(self, writer, indent="", addindent="", newl=""):
        writer.write('<?xml version="1.0" encoding="utf-8"?>' + newl)
        writer.write(
            '<rss xmlns:content="http://purl.org/rss/1.0/modules/content/" '                'xmlns:wfw="http://wellformedweb.org/CommentAPI/" '                'xmlns:dc="http://purl.org/dc/elements/1.1/" '                'xmlns:atom="http://www.w3.org/2005/Atom" '                'xmlns:sy="http://purl.org/rss/1.0/modules/syndication/" '                'xmlns:slash="http://purl.org/rss/1.0/modules/slash/" '                'xmlns:webfeeds="http://www.webfeeds.org/rss/1.0" '                'xmlns:georss="http://www.georss.org/georss" '                'xmlns:geo="http://www.w3.org/2003/01/geo/wgs84_pos#" version="2.0">' + newl
        )
        writer.write(indent + "<channel>" + newl)
        for tag in ["title", "link", "description", "language", "lastBuildDate", "docs", "generator", "ttl"]:
            val = getattr(self, tag, None)
            if val:
                if tag == "lastBuildDate":
                    val = val.strftime("%a, %d %b %Y %H:%M:%S +0000")
                writer.write(indent + addindent + f"<{tag}>{escape(str(val))}</{tag}>" + newl)
        for item in self.items:
            item.writexml(writer, indent + addindent, addindent, newl)
        writer.write(indent + "</channel>" + newl)
        writer.write("</rss>" + newl)

# ---------------- Main ----------------
async def process_novel(session, novel_title):
    async with semaphore:
        novel_url = get_novel_url(novel_title)
        if not await novel_has_paid_update_async(session, novel_url):
            return []
        chapters, desc = await scrape_paid_chapters_async(session, novel_url)
        items = []
        for c in chapters:
            pd = c["pubDate"]
            if pd.tzinfo is None:
                pd = pd.replace(tzinfo=datetime.timezone.utc)
            item = MyRSSItem(
                title=novel_title,
                volume=c["volume"],
                chaptername=c["chaptername"],
                nameextend=c["nameextend"],
                link=c["link"],
                description=c["description"],
                guid=PyRSS2Gen.Guid(c["guid"], isPermaLink=False),
                pubDate=pd,
                coin=c.get("coin", "")
            )
            items.append(item)
        return items

async def main_async():
    async with aiohttp.ClientSession() as session:
        tasks = [process_novel(session, nt) for nt in TRANSLATOR_NOVEL_MAP.values() for nt in nt]
        all_items = []
        for result in await asyncio.gather(*tasks):
            all_items.extend(result)

    all_items.sort(key=lambda it: (normalize_date(it.pubDate), chapter_num(it.chaptername)), reverse=True)

    feed = CustomRSS2(
        title="Dragonholic Paid Chapters",
        link="https://dragonholic.com",
        description="Aggregated RSS feed for paid chapters across mapped novels.",
        lastBuildDate=datetime.datetime.now(datetime.timezone.utc),
        items=all_items
    )
    output_file = "dh_paid_feed.xml"
    # write out using your CustomRSS2.writexml (which includes volume, chaptername, translator, etc.)
    with open(output_file, "w", encoding="utf-8") as f:
        feed.writexml(f, indent="  ", addindent="  ", newl="\n")

    # (optional) re–pretty–print it
    with open(output_file, "r", encoding="utf-8") as f:
        dom = xml.dom.minidom.parseString(f.read())
    pretty = "\n".join(line for line in dom.toprettyxml(indent="  ").splitlines() if line.strip())
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(pretty)
    print(f"Feed generated with {len(all_items)} items.")

if __name__ == "__main__":
    asyncio.run(main_async())
