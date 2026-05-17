import json
import time
import socket
import threading
import re
import ssl
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen, Request
from urllib.parse import urlparse, parse_qs
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


# ═══════════════════════════════════════════
#  COLORS
# ═══════════════════════════════════════════
class C:
    R='\033[91m'; G='\033[92m'; Y='\033[93m'
    B='\033[94m'; M='\033[95m'; CY='\033[96m'
    W='\033[97m'; BOLD='\033[1m'; END='\033[0m'


# ═══════════════════════════════════════════
#  GEO CACHE (prevents repeated slow lookups)
# ═══════════════════════════════════════════
geo_cache = {}
geo_lock = threading.Lock()
geo_semaphore = threading.Semaphore(5)  # rate limit: 5 concurrent geo lookups


def geo_lookup(ip):
    """Cached geo lookup — returns instantly if already looked up"""
    with geo_lock:
        if ip in geo_cache:
            return geo_cache[ip]

    result = {
        "country":"Unknown","country_code":"??","state":"Unknown",
        "city":"Unknown","isp":"Unknown","org":"Unknown",
        "lat":0,"lon":0,"timezone":"Unknown","as_number":"Unknown",
        "hosting":False,"proxy_detected":False
    }

    with geo_semaphore:
        try:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            url = f"http://ip-api.com/json/{ip}?fields=66846719"
            req = Request(url, headers={"User-Agent":"Mozilla/5.0"})
            res = urlopen(req, timeout=4, context=ctx)
            d = json.loads(res.read().decode())
            if d.get("status") == "success":
                result.update({
                    "country":d.get("country","Unknown"),
                    "country_code":d.get("countryCode","??"),
                    "state":d.get("regionName","Unknown"),
                    "city":d.get("city","Unknown"),
                    "isp":d.get("isp","Unknown"),
                    "org":d.get("org","Unknown"),
                    "lat":d.get("lat",0),
                    "lon":d.get("lon",0),
                    "timezone":d.get("timezone","Unknown"),
                    "as_number":d.get("as","Unknown"),
                    "hosting":d.get("hosting",False),
                    "proxy_detected":d.get("proxy",False),
                })
        except Exception:
            pass

    with geo_lock:
        geo_cache[ip] = result
    # ip-api.com rate limit: max 45 req/min → small delay
    time.sleep(0.15)
    return result


# ═══════════════════════════════════════════
#  DATA STORE
# ═══════════════════════════════════════════
class Store:
    def __init__(self):
        self.proxies = []
        self.validated = {}
        self.mode = "normal"
        self.lock = threading.Lock()
        self.is_scraping = False
        self.is_validating = False
        self.scraped_count = 0
        self.valid_count = 0

    def reset(self):
        with self.lock:
            self.proxies = []
            self.validated = {}
            self.scraped_count = 0
            self.valid_count = 0

store = Store()


# ═══════════════════════════════════════════
#  SCRAPER
# ═══════════════════════════════════════════
def scrape_proxies():
    store.is_scraping = True
    found = []
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    hdrs = {"User-Agent":"Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36"}

    print(f"\n{C.CY}{'─'*55}{C.END}")
    print(f"{C.Y}{C.BOLD}[*] SOURCE 1 → ProxyScrape API (Latest Only){C.END}")
    print(f"{C.CY}{'─'*55}{C.END}")

    urls = [
        "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
    ]

    for url in urls:
        try:
            req = Request(url, headers=hdrs)
            res = urlopen(req, timeout=20, context=ctx)
            raw = res.read().decode("utf-8", errors="ignore")
            cnt = 0
            for line in raw.strip().split("\n"):
                line = line.strip()
                if ":" in line:
                    pts = line.split(":")
                    if len(pts)==2:
                        ip,port = pts[0].strip(),pts[1].strip()
                        addr = f"{ip}:{port}"
                        if (re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$",ip)
                            and port.isdigit() and 1<=int(port)<=65535
                            and addr not in found):
                            found.append(addr)
                            cnt += 1
            print(f"{C.G}    [✓] ProxyScrape: {cnt} proxies{C.END}")
        except Exception as e:
            print(f"{C.R}    [!] Error: {str(e)[:60]}{C.END}")

    # Backup source
    try:
        burl = "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt"
        req = Request(burl, headers=hdrs)
        res = urlopen(req, timeout=20, context=ctx)
        raw = res.read().decode("utf-8", errors="ignore")
        cnt = 0
        for line in raw.strip().split("\n")[:150]:
            line = line.strip()
            if ":" in line:
                pts = line.split(":")
                if len(pts)==2:
                    ip,port = pts[0].strip(),pts[1].strip()
                    addr = f"{ip}:{port}"
                    if (re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$",ip)
                        and port.isdigit() and addr not in found):
                        found.append(addr)
                        cnt += 1
        print(f"{C.G}    [✓] Backup: +{cnt} proxies{C.END}")
    except Exception:
        pass

    found = found[:250]
    with store.lock:
        store.proxies = found
        store.scraped_count = len(found)

    print(f"{C.G}{C.BOLD}\n[✓] Total scraped: {len(found)} proxies{C.END}")
    store.is_scraping = False


# ═══════════════════════════════════════════
#  FAST VALIDATOR (reduced timeouts)
# ═══════════════════════════════════════════
RESIDENTIAL_KW = [
    'telecom','broadband','cable','fiber','mobile','wireless',
    'cellular','dsl','communications','airtel','jio','vodafone',
    'bsnl','att','verizon','comcast','spectrum','cox','charter',
    'internet','isp','network','provider'
]


def fast_tcp_check(ip, port, timeout=4):
    """Fast TCP connect check with speed measurement"""
    try:
        t0 = time.time()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        result = s.connect_ex((ip, port))
        speed = round((time.time()-t0)*1000)
        s.close()
        return result == 0, speed
    except Exception:
        return False, 9999


def fast_anon_check(ip, port):
    """Quick anonymity check — 3 second timeout max"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect((ip, port))
        s.send(b"GET http://httpbin.org/headers HTTP/1.1\r\nHost: httpbin.org\r\nConnection: close\r\n\r\n")
        resp = b""
        try:
            while len(resp) < 4096:
                chunk = s.recv(2048)
                if not chunk:
                    break
                resp += chunk
        except socket.timeout:
            pass
        s.close()

        rt = resp.decode("utf-8", errors="ignore").lower()
        if not rt:
            return "unknown"
        if "x-forwarded-for" not in rt and "via" not in rt:
            return "elite"
        elif "x-forwarded-for" not in rt:
            return "anonymous"
        return "transparent"
    except Exception:
        return "unknown"


def fast_ssl_check(ip, port):
    """Quick SSL tunnel check — 2 second timeout"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        s.connect((ip, port))
        s.send(b"CONNECT www.google.com:443 HTTP/1.1\r\nHost: www.google.com:443\r\n\r\n")
        resp = s.recv(256)
        s.close()
        return b"200" in resp
    except Exception:
        return False


def validate_proxy(proxy_str, idx):
    """Validate a single proxy — all checks in parallel where possible"""
    parts = proxy_str.split(":")
    if len(parts) != 2:
        return None
    ip, port_str = parts[0], parts[1]
    port = int(port_str)

    p = {
        "id":idx, "address":proxy_str, "ip":ip, "port":port,
        "alive":False, "speed_ms":9999, "anonymity":"unknown",
        "protocol":"http", "score":0, "score_label":"Poor",
        "country":"Unknown", "country_code":"??", "state":"Unknown",
        "city":"Unknown", "isp":"Unknown", "org":"Unknown",
        "lat":0, "lon":0, "timezone":"Unknown", "as_number":"Unknown",
        "hosting":False, "proxy_detected":False, "ssl_support":False,
        "type":"datacenter",
        "last_checked":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    # ── Step 1: Fast TCP check (fail fast) ──
    alive, speed = fast_tcp_check(ip, port, timeout=4)
    if not alive:
        return p
    p["alive"] = True
    p["speed_ms"] = speed

    # ── Step 2: Run geo + anon + ssl in parallel threads ──
    geo_result = [None]
    anon_result = ["unknown"]
    ssl_result = [False]

    def do_geo():
        geo_result[0] = geo_lookup(ip)

    def do_anon():
        anon_result[0] = fast_anon_check(ip, port)

    def do_ssl():
        ssl_result[0] = fast_ssl_check(ip, port)

    t1 = threading.Thread(target=do_geo, daemon=True)
    t2 = threading.Thread(target=do_anon, daemon=True)
    t3 = threading.Thread(target=do_ssl, daemon=True)
    t1.start(); t2.start(); t3.start()

    # Wait with timeout — don't block forever
    t1.join(timeout=5)
    t2.join(timeout=4)
    t3.join(timeout=3)

    # ── Apply results ──
    geo = geo_result[0] or {
        "country":"Unknown","country_code":"??","state":"Unknown",
        "city":"Unknown","isp":"Unknown","org":"Unknown",
        "lat":0,"lon":0,"timezone":"Unknown","as_number":"Unknown",
        "hosting":False,"proxy_detected":False
    }
    p.update({k: geo[k] for k in geo})
    p["anonymity"] = anon_result[0]
    p["ssl_support"] = ssl_result[0]

    # ── Type detection ──
    combo = (geo.get("isp","") + " " + geo.get("org","")).lower()
    if any(kw in combo for kw in RESIDENTIAL_KW) and not geo.get("hosting",False):
        p["type"] = "residential"

    # ── Score ──
    score = 0
    if p["speed_ms"]<500:     score+=3
    elif p["speed_ms"]<1500:  score+=2
    elif p["speed_ms"]<3000:  score+=1
    if p["anonymity"]=="elite":       score+=3
    elif p["anonymity"]=="anonymous": score+=2
    elif p["anonymity"]=="transparent": score+=1
    if p["ssl_support"]: score+=1
    if p["type"]=="residential": score+=1
    if p["alive"]: score+=1
    if not p["hosting"]: score+=1
    p["score"] = min(score,10)
    p["score_label"] = "Elite" if score>=8 else "Good" if score>=6 else "Average" if score>=4 else "Poor"

    # ── Print ──
    col = C.G if score>=6 else C.Y if score>=4 else C.R
    print(f"  {col}[✓] {proxy_str:25s} | {p['score_label']:7s} {p['country_code']:2s} — {p['score']}/10 | {p['speed_ms']:4d}ms | {p['anonymity']}{C.END}")
    return p


def validate_all():
    store.is_validating = True
    total = len(store.proxies)
    print(f"\n{C.M}{C.BOLD}[*] Validating {total} proxies (workers=30)...{C.END}")
    print(f"{C.CY}{'─'*60}{C.END}")

    with ThreadPoolExecutor(max_workers=30) as ex:
        futures = {ex.submit(validate_proxy, px, i): px for i, px in enumerate(store.proxies)}
        for fut in as_completed(futures):
            try:
                res = fut.result()
                if res and res["alive"]:
                    with store.lock:
                        store.validated[res["address"]] = res
                        store.valid_count = len(store.validated)
            except Exception:
                pass

    if store.mode == "residential":
        with store.lock:
            store.validated = {k:v for k,v in store.validated.items() if v["type"]=="residential"}
            store.valid_count = len(store.validated)
        print(f"\n{C.G}{C.BOLD}[✓] Residential: {store.valid_count}{C.END}")
    else:
        print(f"\n{C.G}{C.BOLD}[✓] Valid: {store.valid_count}/{total}{C.END}")

    store.is_validating = False
    print(f"{C.CY}[*] Done! → http://localhost:5000{C.END}\n")


def run_pipeline():
    scrape_proxies()
    validate_all()


# ═══════════════════════════════════════════
#  FULL HTML (embedded frontend)
# ═══════════════════════════════════════════

FULL_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>🌐 Proxy Scraper</title>
<style>
:root{--bg0:#07070f;--bg1:#0f0f1a;--bg2:#15152a;--bg3:#1e1e35;--bg4:#262645;--ac:#00d4ff;--acg:#00ff88;--acy:#ffd700;--acr:#ff4757;--acp:#a855f7;--t1:#e4e4f0;--t2:#a1a1c0;--t3:#6b6b90;--bd:#1e1e38;--bd2:#2e2e50}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Segoe UI',-apple-system,sans-serif;background:var(--bg0);color:var(--t1);min-height:100vh}
a{text-decoration:none;color:inherit}
::-webkit-scrollbar{width:6px}::-webkit-scrollbar-track{background:var(--bg1)}::-webkit-scrollbar-thumb{background:var(--bd2);border-radius:3px}

.hdr{background:linear-gradient(135deg,var(--bg1),#0d0d22);border-bottom:1px solid var(--bd);padding:14px 0;position:sticky;top:0;z-index:999}
.hdr-in{max-width:1400px;margin:0 auto;padding:0 20px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px}
.logo{display:flex;align-items:center;gap:10px}
.logo-icon{font-size:28px;animation:spin 12s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.logo h1{font-size:20px;font-weight:800;background:linear-gradient(135deg,var(--ac),var(--acp));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.logo-sub{font-size:10px;color:var(--t3);margin-top:2px}
.hdr-stats{display:flex;gap:20px}
.st{text-align:center}.st-v{font-size:20px;font-weight:800;color:var(--ac)}.st-v.g{color:var(--acg)}.st-v.p{color:var(--acp)}
.st-l{font-size:9px;color:var(--t3);text-transform:uppercase;letter-spacing:1px}
.hdr-btns{display:flex;align-items:center;gap:10px}
.badge{padding:5px 12px;border-radius:20px;font-size:11px;font-weight:600;display:flex;align-items:center;gap:5px}
.badge.busy{background:rgba(255,215,0,.12);color:var(--acy);border:1px solid rgba(255,215,0,.3)}
.badge.ready{background:rgba(0,255,136,.12);color:var(--acg);border:1px solid rgba(0,255,136,.3)}
.dot{width:7px;height:7px;border-radius:50%;animation:pulse 1.4s infinite}
.dot.y{background:var(--acy)}.dot.g{background:var(--acg)}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.4;transform:scale(.7)}}
.btn{padding:8px 16px;border:none;border-radius:8px;cursor:pointer;font-size:12px;font-weight:700;transition:all .25s;display:flex;align-items:center;gap:6px}
.btn-ac{background:linear-gradient(135deg,var(--ac),#009abf);color:#000}
.btn-ac:hover{transform:translateY(-2px);box-shadow:0 4px 20px rgba(0,212,255,.35)}
.btn-ac:disabled{opacity:.45;cursor:not-allowed;transform:none}

.prog-w{max-width:1400px;margin:10px auto 0;padding:0 20px}
.prog-o{background:var(--bg2);border:1px solid var(--bd);border-radius:8px;height:6px;overflow:hidden}
.prog-i{height:100%;background:linear-gradient(90deg,var(--ac),var(--acp));transition:width .5s;border-radius:8px}
.prog-t{font-size:10px;color:var(--t3);margin-top:4px;text-align:center}

.filters{max-width:1400px;margin:20px auto 0;padding:0 20px;display:flex;gap:12px;flex-wrap:wrap;align-items:center}
.fg{display:flex;align-items:center;gap:6px}
.fg label{font-size:11px;color:var(--t3);white-space:nowrap}
select{background:var(--bg2);border:1px solid var(--bd);color:var(--t1);padding:6px 10px;border-radius:6px;font-size:11px;cursor:pointer;outline:none}
select:focus{border-color:var(--ac)}
.cnt{font-size:11px;color:var(--t3);margin-left:8px}

.grid{max-width:1400px;margin:16px auto;padding:0 20px 40px;display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:12px}

.card{background:var(--bg2);border:1px solid var(--bd);border-radius:12px;padding:16px;cursor:pointer;transition:all .25s;position:relative;overflow:hidden}
.card::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;background:linear-gradient(90deg,var(--ac),var(--acp));opacity:0;transition:.3s}
.card:hover{background:var(--bg3);border-color:var(--bd2);transform:translateY(-3px);box-shadow:0 8px 30px rgba(0,0,0,.4)}
.card:hover::before{opacity:1}
.c-top{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px}
.c-addr{font-family:'Courier New',monospace;font-size:14px;font-weight:700;color:var(--ac)}
.c-sc{text-align:center}.sc-n{font-size:24px;font-weight:900;line-height:1}.sc-l{font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:1px}
.sc-e .sc-n,.sc-e .sc-l{color:var(--acg)}.sc-g .sc-n,.sc-g .sc-l{color:var(--ac)}.sc-a .sc-n,.sc-a .sc-l{color:var(--acy)}.sc-p .sc-n,.sc-p .sc-l{color:var(--acr)}
.c-loc{display:flex;align-items:center;gap:8px;margin-bottom:8px}
.fl{font-size:20px}.loc-c{font-size:12px;font-weight:600}.loc-s{font-size:10px;color:var(--t2)}
.tags{display:flex;gap:4px;flex-wrap:wrap;margin-bottom:8px}
.tag{padding:3px 8px;border-radius:10px;font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.4px}
.t-elite{background:rgba(0,255,136,.1);color:var(--acg);border:1px solid rgba(0,255,136,.25)}
.t-anon{background:rgba(0,212,255,.1);color:var(--ac);border:1px solid rgba(0,212,255,.25)}
.t-trans{background:rgba(255,215,0,.1);color:var(--acy);border:1px solid rgba(255,215,0,.25)}
.t-unk{background:rgba(161,161,170,.1);color:var(--t2);border:1px solid rgba(161,161,170,.2)}
.t-res{background:rgba(168,85,247,.1);color:var(--acp);border:1px solid rgba(168,85,247,.25)}
.t-dc{background:rgba(161,161,170,.1);color:var(--t3);border:1px solid rgba(161,161,170,.2)}
.t-ssl{background:rgba(0,255,136,.1);color:var(--acg);border:1px solid rgba(0,255,136,.25)}
.sp-r{display:flex;align-items:center;gap:8px}
.sp-bw{flex:1;height:4px;background:var(--bd);border-radius:3px;overflow:hidden}
.sp-b{height:100%;border-radius:3px}
.sp-f{background:var(--acg)}.sp-m{background:var(--acy)}.sp-s{background:var(--acr)}
.sp-t{font-size:10px;color:var(--t2);min-width:42px;text-align:right}

.loader{display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:50vh;gap:14px}
.spinner{width:42px;height:42px;border:3px solid var(--bd2);border-top:3px solid var(--ac);border-radius:50%;animation:spin .7s linear infinite}
.l-t{font-size:14px;color:var(--t2)}.l-s{font-size:11px;color:var(--t3)}
.empty{text-align:center;padding:60px 20px;color:var(--t3)}.empty-i{font-size:42px;margin-bottom:12px}.empty h3{font-size:16px;color:var(--t2);margin-bottom:6px}.empty p{font-size:12px;line-height:1.6}

/* Detail */
.det{max-width:920px;margin:0 auto;padding:24px 20px 60px;display:none}
.back{display:inline-flex;align-items:center;gap:6px;color:var(--ac);font-size:12px;cursor:pointer;background:var(--bg2);border:1px solid var(--bd);border-radius:6px;padding:7px 12px;margin-bottom:18px;transition:.2s}
.back:hover{border-color:var(--ac);background:var(--bg3)}
.d-hdr{background:var(--bg2);border:1px solid var(--bd);border-radius:14px;padding:26px;margin-bottom:18px;position:relative;overflow:hidden}
.d-hdr::before{content:'';position:absolute;top:0;left:0;right:0;height:4px;background:linear-gradient(90deg,var(--ac),var(--acp),var(--acg))}
.d-top{display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:14px;margin-bottom:16px}
.d-addr{font-family:'Courier New',monospace;font-size:24px;font-weight:900;color:var(--ac)}
.d-proto{font-size:10px;color:var(--t3);text-transform:uppercase;letter-spacing:2px;margin-top:3px}
.d-sc{text-align:center;padding:16px 26px;background:rgba(0,0,0,.25);border-radius:10px;border:1px solid var(--bd)}
.d-sc-n{font-size:48px;font-weight:900;line-height:1}
.d-sc-l{font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:2px;margin-top:3px}
.d-sc-o{font-size:10px;color:var(--t3);margin-top:3px}
.d-tags{display:flex;gap:6px;flex-wrap:wrap;margin-top:10px}
.d-tags .tag{font-size:11px;padding:4px 11px}
.ig{display:grid;grid-template-columns:repeat(auto-fit,minmax(380px,1fr));gap:14px}
.is{background:var(--bg2);border:1px solid var(--bd);border-radius:12px;padding:20px}
.is h3{font-size:13px;font-weight:700;color:var(--ac);margin-bottom:12px;display:flex;align-items:center;gap:6px}
.ir{display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid var(--bd)}
.ir:last-child{border-bottom:none}
.ir-l{font-size:11px;color:var(--t2)}
.ir-v{font-size:12px;font-weight:600;text-align:right;max-width:60%;word-break:break-all}

@media(max-width:680px){.hdr-in{flex-direction:column;text-align:center}.hdr-stats{justify-content:center}.grid{grid-template-columns:1fr}.ig{grid-template-columns:1fr}.d-top{flex-direction:column;align-items:center;text-align:center}.d-addr{font-size:16px}}
</style>
</head>
<body>

<!-- ═══ HEADER ═══ -->
<header class="hdr"><div class="hdr-in">
  <div class="logo"><span class="logo-icon">🌐</span><div><h1>Proxy Scraper &amp; Validator</h1><div class="logo-sub" id="ml">Loading...</div></div></div>
  <div class="hdr-stats">
    <div class="st"><div class="st-v" id="ss">0</div><div class="st-l">Scraped</div></div>
    <div class="st"><div class="st-v g" id="sv">0</div><div class="st-l">Valid</div></div>
    <div class="st"><div class="st-v p" id="sc">0</div><div class="st-l">Countries</div></div>
  </div>
  <div class="hdr-btns">
    <div class="badge busy" id="sb"><span class="dot y" id="sd"></span><span id="st">Connecting...</span></div>
    <button class="btn btn-ac" id="rb" onclick="rescrape()" disabled>🔄 Re-Scrape</button>
  </div>
</div></header>

<div class="prog-w" id="pw" style="display:none">
  <div class="prog-o"><div class="prog-i" id="pb" style="width:0%"></div></div>
  <div class="prog-t" id="pt">Working...</div>
</div>

<!-- ═══ INDEX VIEW ═══ -->
<div id="index-view">
  <div class="filters" id="fb" style="display:none">
    <div class="fg"><label>🔒 Anonymity</label><select id="fa" onchange="af()"><option value="">All</option><option value="elite">Elite</option><option value="anonymous">Anonymous</option><option value="transparent">Transparent</option></select></div>
    <div class="fg"><label>🏷 Type</label><select id="ft" onchange="af()"><option value="">All</option><option value="residential">Residential</option><option value="datacenter">Datacenter</option></select></div>
    <div class="fg"><label>🌍 Country</label><select id="fc" onchange="af()"><option value="">All</option></select></div>
    <span class="cnt" id="cl"></span>
  </div>
  <div id="gr"></div>
</div>

<!-- ═══ DETAIL VIEW ═══ -->
<div class="det" id="detail-view">
  <div class="back" onclick="showList()">← Back to List</div>
  <div id="det-content"><div class="loader"><div class="spinner"></div><div class="l-t">Loading...</div></div></div>
</div>

<script>
// ═══ UTILS ═══
function flag(cc){if(!cc||cc.length!==2||cc==='??')return'🌍';const o=127397;return String.fromCodePoint(...cc.toUpperCase().split('').map(c=>c.charCodeAt(0)+o))}
function scCls(s){return s>=8?'sc-e':s>=6?'sc-g':s>=4?'sc-a':'sc-p'}
function scLbl(s){return s>=8?'Elite':s>=6?'Good':s>=4?'Average':'Poor'}
function atCls(a){return{elite:'t-elite',anonymous:'t-anon',transparent:'t-trans'}[a]||'t-unk'}
function spCls(m){return m<500?'sp-f':m<2000?'sp-m':'sp-s'}
function spPct(m){return Math.max(4,100-(m/5000)*100)}
function fm(v){return v&&v!=='Unknown'&&v!=='N/A'?v:'—'}
function row(l,v){return `<div class="ir"><span class="ir-l">${l}</span><span class="ir-v">${v}</span></div>`}
function rowC(l,v,c){return `<div class="ir"><span class="ir-l">${l}</span><span class="ir-v" style="color:${c}">${v}</span></div>`}

let allP=[];
let currentView='list';

// ═══ PROXY DATA IS ALREADY LOADED — INSTANT DETAIL ═══
// All proxy data is stored in allP, clicking shows detail INSTANTLY from memory

function showList(){
  currentView='list';
  document.getElementById('index-view').style.display='block';
  document.getElementById('detail-view').style.display='none';
  document.querySelector('.hdr').style.display='block';
  document.getElementById('pw').style.display=document.getElementById('pw').dataset.show==='1'?'block':'none';
}

function showDetail(id){
  currentView='detail';
  document.getElementById('index-view').style.display='none';
  document.getElementById('detail-view').style.display='block';
  document.querySelector('.hdr').style.display='none';
  document.getElementById('pw').style.display='none';

  // Find proxy from ALREADY LOADED data — INSTANT, no fetch needed
  const p = allP.find(x => x.id == id);
  const dc = document.getElementById('det-content');

  if(!p){
    dc.innerHTML='<div class="empty"><div class="empty-i">❌</div><h3>Proxy Not Found</h3></div>';
    return;
  }

  const scC = p.score>=8?'var(--acg)':p.score>=6?'var(--ac)':p.score>=4?'var(--acy)':'var(--acr)';
  const sl = scLbl(p.score);
  const at = atCls(p.anonymity);
  const spts = p.speed_ms<500?'3/3 ⚡':p.speed_ms<1500?'2/3':p.speed_ms<3000?'1/3':'0/3';
  const apts = p.anonymity==='elite'?'3/3 🔒':p.anonymity==='anonymous'?'2/3':'1/3';
  const aliveTag = p.alive
    ? '<span class="tag" style="background:rgba(0,255,136,.1);color:var(--acg);border:1px solid rgba(0,255,136,.25)">🟢 Alive</span>'
    : '<span class="tag" style="background:rgba(255,71,87,.1);color:var(--acr);border:1px solid rgba(255,71,87,.25)">🔴 Dead</span>';

  dc.innerHTML = `
  <div class="d-hdr">
    <div class="d-top">
      <div>
        <div class="d-addr">${p.address}</div>
        <div class="d-proto">${(p.protocol||'HTTP').toUpperCase()} PROXY • Port ${p.port} • ${p.ssl_support?'SSL ✓':'No SSL'}</div>
        <div class="d-tags">
          <span class="tag ${at}">${p.anonymity||'unknown'}</span>
          <span class="tag ${p.type==='residential'?'t-res':'t-dc'}">${p.type==='residential'?'🏠':'🏢'} ${p.type||'datacenter'}</span>
          ${p.ssl_support?'<span class="tag t-ssl">🔐 SSL</span>':''}
          ${aliveTag}
        </div>
      </div>
      <div class="d-sc">
        <div class="d-sc-n" style="color:${scC}">${p.score}</div>
        <div class="d-sc-l" style="color:${scC}">${sl} ${p.country_code||'??'}</div>
        <div class="d-sc-o">out of 10</div>
      </div>
    </div>
  </div>
  <div class="ig">
    <div class="is"><h3>📡 Connection Details</h3>
      ${row('Proxy Address',p.address)}
      ${row('IP Address',p.ip)}
      ${row('Port',p.port)}
      ${row('Protocol',(p.protocol||'HTTP').toUpperCase())}
      ${rowC('Response Speed',p.speed_ms+'ms',p.speed_ms<500?'var(--acg)':p.speed_ms<2000?'var(--acy)':'var(--acr)')}
      ${row('SSL Support',p.ssl_support?'✅ Yes':'❌ No')}
      ${rowC('Anonymity Level',(p.anonymity||'unknown').charAt(0).toUpperCase()+(p.anonymity||'').slice(1),p.anonymity==='elite'?'var(--acg)':p.anonymity==='anonymous'?'var(--ac)':'var(--acy)')}
      ${row('Last Checked',p.last_checked||'—')}
    </div>
    <div class="is"><h3>${flag(p.country_code)} Location Details</h3>
      ${row('Country',flag(p.country_code)+' '+fm(p.country))}
      ${row('Country Code',p.country_code||'??')}
      ${row('State / Region',fm(p.state))}
      ${row('City',fm(p.city))}
      ${row('Latitude',p.lat||'—')}
      ${row('Longitude',p.lon||'—')}
      ${row('Timezone',fm(p.timezone))}
    </div>
    <div class="is"><h3>🌐 Network Information</h3>
      ${row('ISP',fm(p.isp))}
      ${row('Organization',fm(p.org))}
      ${row('AS Number',fm(p.as_number))}
      ${row('Hosting Provider',p.hosting?'✅ Yes (Datacenter)':'❌ No')}
      ${row('Proxy Detected',p.proxy_detected?'✅ Yes':'❌ No')}
      ${rowC('Proxy Type',p.type==='residential'?'🏠 Residential':'🏢 Datacenter',p.type==='residential'?'var(--acp)':'var(--t2)')}
    </div>
    <div class="is"><h3>⭐ Score Breakdown</h3>
      ${rowC('Overall Score',sl+' '+p.country_code+' — '+p.score+'/10',scC)}
      ${row('Speed Points',spts)}
      ${row('Anonymity Points',apts)}
      ${row('SSL Bonus',p.ssl_support?'1/1 ✓':'0/1')}
      ${row('Residential Bonus',p.type==='residential'?'1/1 🏠':'0/1')}
      ${row('Alive Bonus',p.alive?'1/1 ✓':'0/1')}
      ${row('Non-Hosting Bonus',!p.hosting?'1/1 ✓':'0/1')}
    </div>
  </div>`;
}

// ═══ CARD BUILDER ═══
function mkCard(p){
  const sc=scCls(p.score),sl=scLbl(p.score),sp=spPct(p.speed_ms),spc=spCls(p.speed_ms),at=atCls(p.anonymity);
  return `<div class="card" onclick="showDetail(${p.id})">
    <div class="c-top"><div class="c-addr">${p.address}</div><div class="c-sc ${sc}"><div class="sc-n">${p.score}</div><div class="sc-l">${sl}</div></div></div>
    <div class="c-loc"><span class="fl">${flag(p.country_code)}</span><div><div class="loc-c">${fm(p.country)} (${p.country_code||'??'})</div><div class="loc-s">${fm(p.state)} ${p.city&&p.city!=='Unknown'?'• '+p.city:''}</div></div></div>
    <div class="tags"><span class="tag ${at}">${p.anonymity||'unknown'}</span><span class="tag ${p.type==='residential'?'t-res':'t-dc'}">${p.type||'datacenter'}</span>${p.ssl_support?'<span class="tag t-ssl">SSL ✓</span>':''}</div>
    <div class="sp-r"><div class="sp-bw"><div class="sp-b ${spc}" style="width:${sp}%"></div></div><span class="sp-t">${p.speed_ms}ms</span></div>
  </div>`;
}

function render(proxies){
  const gr=document.getElementById('gr');
  if(!proxies.length){gr.innerHTML='<div class="grid"><div class="empty" style="grid-column:1/-1"><div class="empty-i">🔍</div><h3>No Proxies Yet</h3><p>Scraping & validating... auto-refreshes every 4s.</p></div></div>';return}
  gr.innerHTML='<div class="grid">'+proxies.map(mkCard).join('')+'</div>';
}

function af(){
  const an=document.getElementById('fa').value,ty=document.getElementById('ft').value,cc=document.getElementById('fc').value;
  let l=allP.slice();
  if(an)l=l.filter(p=>p.anonymity===an);
  if(ty)l=l.filter(p=>p.type===ty);
  if(cc)l=l.filter(p=>p.country_code===cc);
  document.getElementById('cl').textContent='Showing '+l.length+' proxies';
  render(l);
}

function updCC(proxies){
  const sel=document.getElementById('fc'),cur=sel.value;
  const codes=[...new Set(proxies.map(p=>p.country_code).filter(Boolean))].sort();
  sel.innerHTML='<option value="">All</option>'+codes.map(c=>`<option value="${c}">${flag(c)} ${c}</option>`).join('');
  if(cur)sel.value=cur;
}

async function fetchData(){
  try{
    const r=await fetch('/api/proxies');const d=await r.json();
    if(!d.success)return;
    document.getElementById('ss').textContent=d.scraped_count||0;
    document.getElementById('sv').textContent=d.valid_count||0;
    document.getElementById('ml').textContent='Mode: '+(d.mode==='residential'?'🏠 Residential':'🌍 Normal')+' • 1 Source';
    const sb=document.getElementById('sb'),sd=document.getElementById('sd'),st_el=document.getElementById('st'),rb=document.getElementById('rb');
    const pw=document.getElementById('pw'),pb=document.getElementById('pb'),pt=document.getElementById('pt');
    if(d.is_scraping){sb.className='badge busy';sd.className='dot y';st_el.textContent='Scraping...';rb.disabled=true;pw.style.display='block';pw.dataset.show='1';pb.style.width='30%';pt.textContent='Scraping from source...'}
    else if(d.is_validating){sb.className='badge busy';sd.className='dot y';st_el.textContent='Validating...';rb.disabled=true;pw.style.display=currentView==='list'?'block':'none';pw.dataset.show='1';const pct=d.scraped_count>0?Math.round((d.valid_count/d.scraped_count)*100):50;pb.style.width=Math.max(30,pct)+'%';pt.textContent='Validated '+d.valid_count+'/'+d.scraped_count}
    else{sb.className='badge ready';sd.className='dot g';st_el.textContent='Ready';rb.disabled=false;pw.style.display='none';pw.dataset.show='0'}
    const ctries=new Set(d.proxies.map(p=>p.country_code).filter(Boolean));
    document.getElementById('sc').textContent=ctries.size;
    allP=d.proxies;
    if(currentView==='list'){document.getElementById('fb').style.display='flex';updCC(allP);af()}
  }catch(e){
    if(currentView==='list'){document.getElementById('gr').innerHTML='<div class="loader"><div class="spinner"></div><div class="l-t">Waiting for backend...</div><div class="l-s">Server on localhost:5000</div></div>'}
  }
}

async function rescrape(){try{await fetch('/api/rescrape');document.getElementById('rb').disabled=true}catch(e){}}

document.getElementById('gr').innerHTML='<div class="loader"><div class="spinner"></div><div class="l-t">Loading proxies...</div><div class="l-s">Connecting...</div></div>';
fetchData();
setInterval(fetchData,4000);
</script>
</body>
</html>"""


# ═══════════════════════════════════════════
#  HTTP SERVER
# ═══════════════════════════════════════════
class Handler(BaseHTTPRequestHandler):
    def log_message(self,*a): pass

    def send_html(self, html):
        d = html.encode()
        self.send_response(200)
        self.send_header("Content-Type","text/html;charset=utf-8")
        self.send_header("Content-Length",len(d))
        self.end_headers()
        self.wfile.write(d)

    def send_json(self, obj, code=200):
        d = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Access-Control-Allow-Origin","*")
        self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",len(d))
        self.end_headers()
        self.wfile.write(d)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin","*")
        self.send_header("Access-Control-Allow-Methods","GET,OPTIONS")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        qs = parse_qs(parsed.query)

        # ── HTML: everything served from one page ──
        if not path.startswith("/api/"):
            self.send_html(FULL_HTML)
            return

        # ── API: /api/proxies ──
        if path == "/api/proxies":
            lst = sorted(store.validated.values(), key=lambda x: x.get("score",0), reverse=True)
            for f,k in [("type","type"),("anonymity","anonymity"),("country","country_code")]:
                fv = qs.get(f,[None])[0]
                if fv: lst = [p for p in lst if p.get(k,"").lower()==fv.lower()]
            self.send_json({"success":True,"total":len(lst),"mode":store.mode,"is_scraping":store.is_scraping,"is_validating":store.is_validating,"scraped_count":store.scraped_count,"valid_count":store.valid_count,"proxies":lst})
            return

        # ── API: /api/proxy/<id> ──
        if path.startswith("/api/proxy/"):
            pid = path.split("/api/proxy/")[-1]
            found = None
            for v in store.validated.values():
                if str(v.get("id"))==pid or v.get("address")==pid:
                    found=v; break
            self.send_json({"success":bool(found),"proxy":found} if found else {"success":False,"error":"Not found"}, 200 if found else 404)
            return

        # ── API: /api/status ──
        if path == "/api/status":
            self.send_json({"success":True,"mode":store.mode,"is_scraping":store.is_scraping,"is_validating":store.is_validating,"scraped_count":store.scraped_count,"valid_count":store.valid_count})
            return

        # ── API: /api/rescrape ──
        if path == "/api/rescrape":
            if not store.is_scraping and not store.is_validating:
                store.reset()
                threading.Thread(target=run_pipeline, daemon=True).start()
                self.send_json({"success":True,"message":"Started"})
            else:
                self.send_json({"success":False,"message":"Already running"})
            return

        self.send_json({"success":False,"error":"Not found"}, 404)


# ═══════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════
def banner():
    print(f"""{C.CY}{C.BOLD}
   ---                       _____           _ 
 |  _ \ _ __ _____  ___   _  |_   _|__   ___ | |
 | |_) | '__/ _ \ \/ / | | |   | |/ _ \ / _ \| |
 |  __/| | | (_) >  <| |_| |   | | (_) | (_) | |
 |_|   |_|  \___/_/\_\\__, |   |_|\___/ \___/|_|
                      |___/
{C.END}""")


def main():
    banner()

    print(f"{C.Y}{C.BOLD}Scrape All Proxies Or Only Residential?{C.END}")
    print(f"  {C.CY}[1]{C.END}  Normal  (All Proxies)")
    print(f"  {C.CY}[2]{C.END}  Residential Only\n")

    while True:
        ch = input(f"{C.G}Your choice (1 / 2): {C.END}").strip()
        if ch in ("1","2"): break
        print(f"{C.R}  Enter 1 or 2.{C.END}")

    store.mode = "residential" if ch=="2" else "normal"
    label = "🏠 Residential Only" if store.mode=="residential" else "🌍 Normal (All)"

    print(f"\n{C.G}[✓] Mode             : {label}{C.END}")
    print(f"{C.B}[✓] How Many Sources : 1{C.END}")
    print(f"{C.B}[✓] Source           : ProxyScrape API (latest){C.END}")
    print(f"{C.B}[✓] Running On       : http://localhost:5000{C.END}\n")

    threading.Thread(target=run_pipeline, daemon=True).start()

    try:
        srv = HTTPServer(("0.0.0.0", 5000), Handler)
        print(f"{C.G}{C.BOLD}╔══════════════════════════════════════════╗")
        print(f"║  🚀  Open → http://localhost:5000        ║")
        print(f"╚══════════════════════════════════════════╝{C.END}")
        print(f"{C.CY}  Click any proxy → INSTANT detail page")
        print(f"  Auto-refreshes every 4 seconds")
        print(f"  Press Ctrl+C to stop\n{C.END}")
        srv.serve_forever()
    except KeyboardInterrupt:
        print(f"\n{C.R}[!] Stopped.{C.END}")
        sys.exit(0)


if __name__ == "__main__":
    main()