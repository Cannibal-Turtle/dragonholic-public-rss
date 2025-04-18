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

def slugify(text):
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    return re.sub(r"[\s]+", "-", text)


def clean_description(raw_desc):
    soup = BeautifulSoup(raw_desc, "html.parser")
    for div in soup.find_all("div", class_="c-content-readmore"):
        div.decompose()
    cleaned = soup.decode_contents()
    return re.sub(r'\s+', ' ', cleaned).strip()


def extract_pubdate_from_soup(chap):
    span = chap.find("span", class_="chapter-release-date")
    if span and span.find("i"):
        date_str = span.find("i").get_text(strip=True)
        try:
            return datetime.datetime.strptime(date_str, "%B %d, %Y").replace(tzinfo=datetime.timezone.utc)
        except:
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


def normalize_date(dt):
    return dt.replace(microsecond=0)

# ---------------- Async Fetch ----------------

async def fetch_page(session, url):
    async with session.get(url) as resp:
        resp.raise_for_status()
        return await resp.text()

async def scrape_paid_chapters_async(session, novel_url):
    html = await fetch_page(session, novel_url)
    soup = BeautifulSoup(html, 'html.parser')

    # main description
    desc = soup.find('div', class_='description-summary')
    main_desc = clean_description(desc.decode_contents()) if desc else ''

    paid = []
    now = datetime.datetime.now(datetime.timezone.utc)

    # 1) Volume-based
    vol_list = soup.select('ul.main.version-chap.volumns > li.parent.has-child')
    for parent in vol_list:
        vol_text = parent.select_one('a.has-child').get_text(strip=True)
        vol_slug = slugify(vol_text)
        for chap_li in parent.select('ul.sub-chap-list li.wp-manga-chapter'):
            if 'free-chap' in chap_li.get('class', []): continue
            pub = extract_pubdate_from_soup(chap_li)
            if pub < now - datetime.timedelta(days=7): continue

            a = chap_li.find('a')
            raw = a.get_text(' ', strip=True)
            # split raw into number + title
            num, rest = raw.split(' ', 1) if ' ' in raw else (raw, '')
            chap_slug = slugify(raw)
            href = a.get('href', '').strip()
            link = href if href and href != '#' else f"{novel_url}{vol_slug}/{chap_slug}/"

            guid = next((c.split('data-chapter-')[1] for c in chap_li.get('class', []) if c.startswith('data-chapter-')), chap_slug)
            coin = chap_li.find('span', class_='coin')
            coin = coin.get_text(strip=True) if coin else ''

            paid.append({
                'volume': vol_text,
                'chaptername': num.strip(),
                'nameextend': rest.strip(),
                'link': link,
                'description': main_desc,
                'pubDate': pub,
                'guid': guid,
                'coin': coin
            })

    # 2) No-volume
    for chap_li in soup.select('ul.main.version-chap.no-volumn li.wp-manga-chapter'):
        if 'free-chap' in chap_li.get('class', []): continue
        pub = extract_pubdate_from_soup(chap_li)
        if pub < now - datetime.timedelta(days=7): continue

        a = chap_li.find('a')
        raw = a.get_text(' ', strip=True)
        num, rest = raw.split(' ', 1) if ' ' in raw else (raw, '')
        chap_slug = slugify(raw)
        href = a.get('href', '').strip()
        link = href if href and href != '#' else f"{novel_url}{chap_slug}/"

        guid = next((c.split('data-chapter-')[1] for c in chap_li.get('class', []) if c.startswith('data-chapter-')), chap_slug)
        coin = chap_li.find('span', class_='coin')
        coin = coin.get_text(strip=True) if coin else ''

        paid.append({
            'volume': '',
            'chaptername': num.strip(),
            'nameextend': rest.strip(),
            'link': link,
            'description': main_desc,
            'pubDate': pub,
            'guid': guid,
            'coin': coin
        })

    return paid

# ---------------- RSS Classes ----------------

class MyRSSItem(PyRSS2Gen.RSSItem):
    def __init__(self, *args, volume='', chaptername='', nameextend='', coin='', **kwargs):
        self.volume = volume
        self.chaptername = chaptername
        self.nameextend = nameextend
        self.coin = coin
        super().__init__(*args, **kwargs)

    def writexml(self, writer, indent='', addindent='', newl=''):
        writer.write(indent + '<item>' + newl)
        writer.write(indent + addindent + f'<title>{escape(self.title)}</title>' + newl)
        writer.write(indent + addindent + f'<volume>{escape(self.volume)}</volume>' + newl)
        writer.write(indent + addindent + f'<chaptername>{escape(self.chaptername)}</chaptername>' + newl)
        ext = f'***{self.nameextend}***' if self.nameextend else ''
        writer.write(indent + addindent + f'<nameextend>{escape(ext)}</nameextend>' + newl)
        writer.write(indent + addindent + f'<link>{escape(self.link)}</link>' + newl)
        writer.write(indent + addindent + f'<description><![CDATA[{self.description}]]></description>' + newl)
        cat = 'NSFW' if self.title in get_nsfw_novels() else 'SFW'
        writer.write(indent + addindent + f'<category>{cat}</category>' + newl)
        trans = get_translator(self.title) or ''
        writer.write(indent + addindent + f'<translator>{escape(trans)}</translator>' + newl)
        role = get_discord_role_id(trans)
        if cat == 'NSFW': role += ' <@&1304077473998442506>'
        writer.write(indent + addindent + f'<discord_role_id><![CDATA[{role}]]></discord_role_id>' + newl)
        writer.write(indent + addindent + f'<featuredImage url="{escape(get_featured_image(self.title))}"/>' + newl)
        if self.coin:
            writer.write(indent + addindent + f'<coin>{escape(self.coin)}</coin>' + newl)
        writer.write(indent + addindent + f'<pubDate>{self.pubDate.strftime("%a, %d %b %Y %H:%M:%S +0000")}</pubDate>' + newl)
        writer.write(indent + addindent + f'<guid isPermaLink="false">{escape(self.guid.guid)}</guid>' + newl)
        writer.write(indent + '</item>' + newl)

class CustomRSS2(PyRSS2Gen.RSS2):
    def writexml(self, writer, indent='', addindent='', newl=''):
        writer.write('<?xml version="1.0" encoding="utf-8"?>' + newl)
        writer.write('<rss version="2.0" ' +
                     'xmlns:content="http://purl.org/rss/1.0/modules/content/" ' +
                     'xmlns:wfw="http://wellformedweb.org/CommentAPI/" ' +
                     'xmlns:dc="http://purl.org/dc/elements/1.1/" ' +
                     'xmlns:atom="http://www.w3.org/2005/Atom" ' +
                     'xmlns:sy="http://purl.org/rss/1.0/modules/syndication/" ' +
                     'xmlns:slash="http://purl.org/rss/1.0/modules/slash/" ' +
                     'xmlns:webfeeds="http://www.webfeeds.org/rss/1.0" ' +
                     'xmlns:georss="http://www.georss.org/georss" ' +
                     'xmlns:geo="http://www.w3.org/2003/01/geo/wgs84_pos#"' +
                     '>' + newl)
        writer.write(indent + '<channel>' + newl)
        props = ['title','link','description','language','lastBuildDate','docs','generator','ttl']
        for p in props:
            val = getattr(self, p, None)
            if val:
                if p=='lastBuildDate':
                    val = val.strftime("%a, %d %b %Y %H:%M:%S +0000")
                writer.write(indent+addindent+f'<{p}>{escape(str(val))}</{p}>' + newl)
        for i in self.items:
            i.writexml(writer, indent+addindent, addindent, newl)
        writer.write(indent + '</channel>' + newl)
        writer.write('</rss>')

# ---------------- Main ----------------
async def main_async():
    items = []
    async with aiohttp.ClientSession() as sess:
        tasks = [scrape_paid_chapters_async(sess, get_novel_url(n)) for t in TRANSLATOR_NOVEL_MAP.values() for n in t]
        results = await asyncio.gather(*tasks)
        for chaps in results:
            for c in chaps:
                pd = c['pubDate']
                if pd.tzinfo is None:
                    pd = pd.replace(tzinfo=datetime.timezone.utc)
                items.append(
                    MyRSSItem(
                        title=[k for k,v in TRANSLATOR_NOVEL_MAP.items() if any(n==k for n_list in [v] for n in n_list)][0],
                        volume=c['volume'],
                        chaptername=c['chaptername'],
                        nameextend=c['nameextend'],
                        link=c['link'],
                        description=c['description'],
                        guid=PyRSS2Gen.Guid(c['guid'], isPermaLink=False),
                        pubDate=pd,
                        coin=c['coin']
                    )
                )
    items.sort(key=lambda x:(normalize_date(x.pubDate),), reverse=True)
    feed = CustomRSS2(
        title="Dragonholic Paid Chapters",
        link="https://dragonholic.com",
        description="Aggregated RSS feed for paid chapters.",
        lastBuildDate=datetime.datetime.now(datetime.timezone.utc),
        items=items
    )
    with open('dh_paid_feed.xml','w',encoding='utf-8') as f:
        feed.writexml(f,indent='  ',addindent='  ',newl='\n')
    # pretty print
    txt = xml.dom.minidom.parseString(open('dh_paid_feed.xml','r',encoding='utf-8').read())
    with open('dh_paid_feed.xml','w',encoding='utf-8') as f:
        f.write('\n'.join([l for l in txt.toprettyxml(indent='  ').splitlines() if l.strip()]))
    print(f"Feed generated with {len(items)} items.")

if __name__=='__main__':
    asyncio.run(main_async())
