#!/usr/bin/env python3
import os, re, json, hashlib, asyncio, datetime
from urllib.parse import urljoin
import httpx, feedparser, tldextract
from bs4 import BeautifulSoup
from rapidfuzz import fuzz
from PIL import Image
from io import BytesIO
from dateutil import parser as dtp

SITE=os.environ.get("DINAVIS_SITE","/media/%s/T7/dinavis/site"%os.environ.get("USER","user"))
DATA_DIR=os.path.join(SITE,"assets","data"); IMG_DIR=os.path.join(SITE,"assets","images")
PROOF_DIR=os.path.join(SITE,"assets","proof"); CACHE_DIR=os.path.join(SITE,".cache","http")
OUT_JSON=os.path.join(DATA_DIR,"articles.json")
SRC_JSON=os.path.join(SITE,"tools","dinavis-pipeline","sources.json")
KW_JSON=os.path.join(SITE,"tools","dinavis-pipeline","categories_keywords.json")
GEO_JSON=os.path.join(SITE,"tools","dinavis-pipeline","geography_no.json")
MAP_JSON=os.path.join(SITE,"tools","dinavis-pipeline","domain_mapping.json")
for d in (DATA_DIR,IMG_DIR,PROOF_DIR,CACHE_DIR): os.makedirs(d,exist_ok=True)

def sha1(s:str)->str: return hashlib.sha1(s.encode("utf-8","ignore")).hexdigest()
def norm(t): return re.sub(r"[^a-z0-9]+"," ",(t or "").lower()).strip()
def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat()

def load(path,default): 
  try: return json.load(open(path,"r",encoding="utf-8"))
  except: return default
def save(path,data):
  os.makedirs(os.path.dirname(path),exist_ok=True)
  tmp=path+".tmp"; json.dump(data,open(tmp,"w",encoding="utf-8"),ensure_ascii=False,indent=2); os.replace(tmp,path)

def domain(u):
  ex=tldextract.extract(u); return ".".join([p for p in [ex.domain,ex.suffix] if p])

def abs_url(base,u):
  try: return urljoin(base,u)
  except: return u

def extract_meta(html_text, base_url):
  soup=BeautifulSoup(html_text,"lxml"); meta={}
  def pick(*qs):
    for q in qs:
      tag=soup.find("meta",q)
      if tag and tag.get("content"): return abs_url(base_url,tag["content"])
  img=pick({"property":"og:image"},{"name":"twitter:image"},{"property":"og:image:url"})
  if not img:
    for s in soup.find_all("script",{"type":"application/ld+json"}):
      try:
        data=json.loads(s.string or "{}"); im=data.get("image")
        if isinstance(im,str): img=abs_url(base_url,im); break
        if isinstance(im,list) and im: img=abs_url(base_url,im[0]); break
      except: pass
  meta["image"]=img
  t=soup.find("meta",{"property":"og:title"}) or soup.find("meta",{"name":"twitter:title"})
  meta["title"]=(t and t.get("content")) or (soup.title.text.strip() if soup.title else None)
  d=soup.find("meta",{"property":"og:description"}) or soup.find("meta",{"name":"description"})
  meta["description"]=d.get("content").strip() if d and d.get("content") else None
  L=soup.find("link",{"rel":"license"})
  meta["license_url"]=abs_url(base_url,L["href"]) if L and L.get("href") else None
  alt=soup.find("meta",{"property":"og:image:alt"}) or soup.find("meta",{"name":"twitter:image:alt"})
  meta["image_alt"]=alt.get("content").strip() if alt and alt.get("content") else None
  return meta

def save_image_variants(blob, base):
  out={}
  try: im=Image.open(BytesIO(blob))
  except: return out
  for w in (320,640,1024):
    imc=im.copy(); imc.thumbnail((w,10000))
    p=os.path.join(IMG_DIR,f"{base}-{w}.webp")
    try: imc.save(p,"WEBP",quality=80,method=6); out[str(w)]=os.path.relpath(p,start=os.path.join(SITE,"assets"))
    except: pass
  return out

def guess_categories(text, kw):
  t=text.lower(); res=set()
  for cat,words in kw.items():
    for w in words:
      if re.search(r"\b"+re.escape(w.lower())+r"\b",t): res.add(cat); break
  return sorted(res)

def guess_geo(text, geo, dmap, url):
  t=text.lower(); hit=[]
  for k in geo.get("kommunar",[]): 
    if re.search(r"\b"+re.escape(k.lower())+r"\b",t): hit.append({"type":"kommune","name":k})
  for f in geo.get("fylke",[]):
    if re.search(r"\b"+re.escape(f.lower())+r"\b",t): hit.append({"type":"fylke","name":f})
  if hit: return hit
  reg=dmap.get(domain(url)); 
  return [{"type":"region","name":reg}] if reg else []

def dedup_key(title,link): return sha1(norm(title)+"|"+domain(link))
def is_dup(title,link,existing):
  k=dedup_key(title,link)
  for a in existing:
    if a.get("dedup")==k: return True
    try:
      if fuzz.token_set_ratio(norm(title),norm(a.get("title","")) )>=90: return True
    except: pass
  return False

async def fetch(client,url): 
  try:
    r=await client.get(url,timeout=20.0,follow_redirects=True)
    return r.status_code, r.text, dict(r.headers)
  except: return None, None, None

async def fetch_bin(client,url):
  try:
    r=await client.get(url,timeout=20.0,follow_redirects=True)
    if r.status_code==200 and r.content: return r.content
  except: pass
  return None

async def process_rss(client,src,kw,geo,dmap,existing):
  out=[]
  try:
    r=await client.get(src["url"],timeout=20.0)
    feed=feedparser.parse(r.content)
  except: return out
  for e in feed.entries[:50]:
    link=e.get("link"); title=e.get("title") or ""
    if not link or not title: continue
    if is_dup(title,link,existing+out): continue
    pub=(e.get("published") or e.get("updated")); 
    try: pub_iso=dtp.parse(pub).astimezone(datetime.timezone.utc).isoformat() if pub else None
    except: pub_iso=None
    code,html,headers=await fetch(client,link)
    img_rel={}; img_alt=None; lic_url=None; meta={}
    if html:
      meta=extract_meta(html,link); lic_url=meta.get("license_url")
      if meta.get("image"):
        blob=await fetch_bin(client,meta["image"])
        if blob: img_rel=save_image_variants(blob, sha1(meta["image"])[:12])
      img_alt=meta.get("image_alt") or meta.get("title") or title
      # proof
      day=datetime.datetime.utcnow().strftime("%Y%m%d")
      pdir=os.path.join(PROOF_DIR,day); os.makedirs(pdir,exist_ok=True)
      open(os.path.join(pdir,sha1(link)+".html"),"w",encoding="utf-8").write(html)
      json.dump(headers,open(os.path.join(pdir,sha1(link)+".headers.json"),"w",encoding="utf-8"),ensure_ascii=False,indent=2)
    ingress = meta.get("description") or (e.get("summary") or "")
    full = f"{title} {ingress}"
    cats=guess_categories(full,kw)
    g=guess_geo(full,geo,dmap,link) or [{"type":"region","name":src.get("default_region","Nasjonalt")}]
    out.append({
      "id":sha1(link)[:16],
      "dedup":dedup_key(title,link),
      "title":title.strip(),
      "ingress":ingress.strip()[:220],
      "url":link,
      "domain":domain(link),
      "published_at":pub_iso or now(),
      "fetched_at":now(),
      "categories":cats,
      "geo":g,
      "license_hint":src.get("license_hint"),
      "license_url":lic_url,
      "image_variants":img_rel,
      "image_alt":img_alt,
      "source_name":src.get("name")
    })
  return out

async def main():
  sources=load(SRC_JSON,[]); kw=load(KW_JSON,{}); geo=load(GEO_JSON,{}); dmap=load(MAP_JSON,{})
  existing=load(OUT_JSON,[])
  async with httpx.AsyncClient(headers={"User-Agent":"DINAVIS/1.0"},http2=True) as client:
    tasks=[process_rss(client,s,kw,geo,dmap,existing) for s in sources if s.get("type")=="rss"]
    results=await asyncio.gather(*tasks)
  new=[it for sub in results for it in sub]
  combined=existing[:]; seen={a.get("dedup") for a in existing}
  for it in new:
    if it["dedup"] in seen: continue
    combined.append(it); seen.add(it["dedup"])
  def key(a):
    try: return dtp.parse(a.get("published_at") or a.get("fetched_at") or "1970").timestamp()
    except: return 0
  combined.sort(key=key, reverse=True)
  save(OUT_JSON,combined)
  print(f"✅ Oppdatert {OUT_JSON} med {len(new)} nye saker (total: {len(combined)}).")
if __name__=="__main__": asyncio.run(main())
