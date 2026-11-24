# -*- coding: utf-8 -*-
"""
Credit-Loan-Korea : 신용·대차 CHECK 데이터 수집기
- 공매도 잔고 : KRX 정보데이터시스템 [33001] 개별종목 공매도 잔고 (pykrx 경유, KRX 회원 로그인)
- 주가·상장주식수 : KRX [12001] 전종목 시세 (pykrx 경유)
- 대차 잔고 : 공공데이터포털 '금융위원회_주식대차거래정보' getStckLnbDetail
- 필요 환경변수 (GitHub Secrets): KRX_ID, KRX_PW, DATA_GO_KR_KEY
- 산출물: data/daily/YYYYMMDD.json (일별 원자료)
          data/latest.json (사이트용 집계), data/hist/NN.json (종목별 60일 추이)
"""
import os
import sys
import json
import time
import datetime as dt
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
DAILY_DIR = DATA / "daily"
HIST_DIR = DATA / "hist"
OUT_FILE = DATA / "latest.json"
META_FILE = DATA / "meta.json"
LEND_SAMPLE = DATA / "lend_sample.json"

KST = dt.timezone(dt.timedelta(hours=9))
N_KEEP = 61                 # 보관 거래일 수 (60일 변화율 계산용)
PERIODS = [1, 5, 20, 60]    # 기간 변화율 (거래일)
LOOKBACK_CAL = 110          # 최대 탐색 달력일
MAX_FETCH = int(os.getenv("MAX_FETCH", "80"))   # 1회 실행 시 신규 조회 일수 한도
SLEEP = float(os.getenv("KRX_SLEEP", "1.2"))    # KRX 차단 방지 지연(초)
READY_HOUR = 18             # 당일 시세는 18시(KST) 이후 조회
RETRY_DAYS = 10             # 최근 N일 안의 미수신 항목은 매 실행마다 재시도
MAX_TRIES = 8
N_BUCKET = 64               # 종목별 추이 파일 묶음 수

LEND_URL = "https://apis.data.go.kr/1160100/service/GetCMStckLnbInfoService/getStckLnbDetail"
DGK = unquote(os.getenv("DATA_GO_KR_KEY", "").strip())


# ───────────────────────── 공통 ─────────────────────────
def now_kst():
    return dt.datetime.now(KST)


def load_json(p, default):
    try:
        return json.loads(Path(p).read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(p, obj):
    Path(p).parent.mkdir(parents=True, exist_ok=True)
    Path(p).write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def num(x):
    s = str(x).replace(",", "").strip()
    if s in ("", "-", "None", "nan"):
        return 0
    try:
        return int(float(s))
    except ValueError:
        return 0


def norm_code(v):
    s = str(v).strip().upper()
    if len(s) == 12 and s.startswith("KR"):
        s = s[3:9]
    if len(s) == 7 and s.startswith("A"):
        s = s[1:]
    return s.zfill(6) if s.isdigit() else s


# ───────────────────────── KRX ─────────────────────────
def fetch_px(d):
    """전종목 시세 → {code: [name, mkt(K/Q), close, list_shrs, mktcap]} / 휴장이면 None"""
    from pykrx.website.krx.market.core import 전종목시세
    df = 전종목시세().fetch(d, "ALL")
    time.sleep(SLEEP)
    if df is None or df.empty:
        return None
    out = {}
    for _, r in df.iterrows():
        mk = str(r.get("MKT_NM", ""))
        if mk.startswith("KOSPI"):
            m = "K"
        elif mk.startswith("KOSDAQ"):
            m = "Q"
        else:
            continue  # KONEX 제외
        out[norm_code(r["ISU_SRT_CD"])] = [
            str(r["ISU_ABBRV"]).strip(), m,
            num(r.get("TDD_CLSPRC")), num(r.get("LIST_SHRS")), num(r.get("MKTCAP")),
        ]
    if not out or sum(v[2] for v in out.values()) == 0:
        return None
    return out


def fetch_short(d):
    """개별종목 공매도 잔고(전종목) → {code: [qty, amt]} / 미공표면 None"""
    from pykrx.website.krx.market.core import 전종목_공매도_잔고
    out = {}
    for idx in (1, 2):
        df = 전종목_공매도_잔고().fetch(d, idx)
        time.sleep(SLEEP)
        if df is None or df.empty:
            return None
        for _, r in df.iterrows():
            out[norm_code(r["ISU_CD"])] = [num(r.get("BAL_QTY")), num(r.get("BAL_AMT"))]
    if not out or sum(v[0] for v in out.values()) == 0:
        return None
    return out


# ───────────────────────── 공공데이터포털 (대차) ─────────────────────────
def _pick(keys, must, avoid=()):
    for k in keys:
        lk = k.lower()
        if any(m in lk for m in must) and not any(a in lk for a in avoid):
            return k
    return None


def map_lend_keys(item):
    keys = list(item.keys())
    km = {
        "code": _pick(keys, ["srtncd", "itmscd", "stckcd"]) or _pick(keys, ["cd"], ["mrkt", "isin", "clsf"]) or _pick(keys, ["isin"]),
        "name": _pick(keys, ["itmsnm"]) or _pick(keys, ["nm"], ["mrkt", "clsf"]),
        "qty": _pick(keys, ["blnc", "bal", "rmnd", "remn"], ["amt"]),
        "amt": _pick(keys, ["blncamt", "balamt", "rmndamt", "remnamt"]) or _pick(keys, ["amt"], ["cntr", "rdmp", "rpy", "trd"]),
    }
    missing = [k for k in ("code", "qty") if not km[k]]
    if missing:
        raise RuntimeError(f"대차 응답 필드 판별 실패 {missing}: 받은 필드 {keys}")
    return km


def fetch_lend(d):
    """주식 대차거래내역 → {code: [qty, amt, name]} / 미공표면 None"""
    import requests
    if not DGK:
        return None
    out, page, km, total = {}, 1, None, None
    while True:
        params = {"serviceKey": DGK, "pageNo": page, "numOfRows": 1000, "resultType": "json", "basDt": d}
        r = None
        for attempt in range(3):  # 공공데이터포털 응답 지연 대비 재시도
            try:
                r = requests.get(LEND_URL, params=params, timeout=90)
                break
            except requests.exceptions.RequestException as e:
                print(f"[retry] lend {d} p{page} ({attempt + 1}/3): {e}")
                time.sleep(5 * (attempt + 1))
        if r is None:
            raise RuntimeError(f"대차 API 응답 없음 {d} p{page}")
        try:
            js = r.json()
        except ValueError:
            raise RuntimeError(f"대차 API 응답 오류(HTTP {r.status_code}): {r.text[:300]}")
        resp = js.get("response", js)
        head = resp.get("header", {})
        if str(head.get("resultCode", "00")) not in ("00", "0"):
            raise RuntimeError(f"대차 API 오류: {head}")
        body = resp.get("body", {})
        total = num(body.get("totalCount", 0))
        items = body.get("items") or {}
        items = items.get("item", []) if isinstance(items, dict) else items
        if isinstance(items, dict):
            items = [items]
        if not items:
            break
        if km is None:
            km = map_lend_keys(items[0])
            save_json(LEND_SAMPLE, {"date": d, "keys_used": km, "sample": items[0]})
        for it in items:
            bd = str(it.get("basDt", d)).replace("-", "")
            if bd and bd != d:
                continue
            c = norm_code(it.get(km["code"]))
            q = num(it.get(km["qty"]))
            a = num(it.get(km["amt"])) if km["amt"] else 0
            nm = str(it.get(km["name"], "")).strip() if km["name"] else ""
            if c in out:  # 같은 종목이 여러 줄이면 합산
                out[c][0] += q
                out[c][1] += a
            else:
                out[c] = [q, a, nm]
        if page * 1000 >= total:
            break
        page += 1
        time.sleep(0.3)
    if not out or sum(v[0] for v in out.values()) == 0:
        return None
    return out


# ───────────────────────── 수집 ─────────────────────────
def collect_days():
    meta = load_json(META_FILE, {"holidays": []})
    holidays = set(meta.get("holidays", []))
    now = now_kst()
    today = now.date()
    dates, fetched, errors = [], 0, []

    for back in range(LOOKBACK_CAL):
        day = today - dt.timedelta(days=back)
        if day.weekday() >= 5:
            continue
        d = day.strftime("%Y%m%d")
        if d in holidays:
            continue
        if day == today and now.hour < READY_HOUR:
            continue
        f = DAILY_DIR / f"{d}.json"
        rec = load_json(f, None)
        age = (today - day).days
        changed = False

        if rec is None:
            if fetched >= MAX_FETCH:
                continue
            fetched += 1
            print(f"[px] {d}")
            try:
                px = fetch_px(d)
            except Exception as e:
                print(f"[error] px {d}: {e}")
                errors.append(f"px {d}")
                continue
            if px is None:
                if day < today:
                    holidays.add(d)
                    print(f"[holiday] {d}")
                continue
            rec = {"date": d, "px": px, "short": None, "lend": None, "tries": {"short": 0, "lend": 0}}
            changed = True

        for kind, fn in (("short", fetch_short), ("lend", fetch_lend)):
            if rec.get(kind) is not None:
                continue
            if kind == "lend" and not DGK:
                continue
            tries = rec.setdefault("tries", {}).get(kind, 0)
            if tries >= MAX_TRIES or (tries >= 3 and age > RETRY_DAYS):
                continue
            print(f"[{kind}] {d}")
            try:
                data = fn(d)
            except Exception as e:
                print(f"[error] {kind} {d}: {e}")
                errors.append(f"{kind} {d}: {e}")
                data = None
            rec["tries"][kind] = tries + 1
            if data is not None:
                rec[kind] = data
            changed = True

        if changed:
            rec["fetched_at"] = now.isoformat(timespec="minutes")
            save_json(f, rec)
        dates.append(d)
        if len(dates) >= N_KEEP:
            break

    meta["holidays"] = sorted(holidays)[-300:]
    meta["errors"] = errors[-20:]
    meta["lend_key"] = bool(DGK)
    save_json(META_FILE, meta)

    keep = set(dates)
    for p in DAILY_DIR.glob("*.json"):
        if p.stem not in keep:
            p.unlink()
    return sorted(dates)


# ───────────────────────── 집계 ─────────────────────────
def r2(x, n=2):
    return None if x is None else round(x, n)


def bucket(code):
    return sum(ord(c) for c in code) % N_BUCKET


def aggregate(days):
    recs = {d: load_json(DAILY_DIR / f"{d}.json", None) for d in days}
    days = [d for d in days if recs.get(d)]
    out = {
        "generated_at": now_kst().isoformat(timespec="minutes"),
        "days": days,
        "periods": PERIODS,
        "lend_key": bool(DGK),
        "kinds": {},
        "totals": {"mcap": {"K": [], "Q": []}},
    }
    for d in days:
        px = recs[d]["px"]
        for m in ("K", "Q"):
            out["totals"]["mcap"][m].append(sum(v[4] for v in px.values() if v[1] == m))

    names = {}
    for d in days:
        for c, v in recs[d]["px"].items():
            names[c] = (v[0], v[1])

    hist = {}
    for kind in ("lend", "short"):
        kd = [d for d in days if recs[d].get(kind)]
        tot = {"K": [], "Q": []}
        for d in days:
            data = recs[d].get(kind)
            px = recs[d]["px"]
            for m in ("K", "Q"):
                if data is None:
                    tot[m].append(None)
                else:
                    tot[m].append(sum(v[1] for c, v in data.items() if px.get(c, ["", ""])[1] == m))
        out["totals"][kind] = tot
        if not kd:
            out["kinds"][kind] = None
            continue

        asof = kd[-1]
        px0 = recs[asof]["px"]
        codes = set()
        for d in kd[-(max(PERIODS) + 1):]:
            codes.update(recs[d][kind].keys())

        rows = []
        for c in codes:
            p0 = px0.get(c)
            if p0 is None:
                continue  # 상장폐지·KONEX 등
            cur = recs[asof][kind].get(c, [0, 0])
            q0 = cur[0]
            close0, list0 = p0[2], p0[3]
            ratio0 = q0 / list0 * 100 if list0 else 0
            amt0 = cur[1] if cur[1] else q0 * close0
            row = [c, p0[0], p0[1], q0, amt0, r2(ratio0, 4)]
            any_move = q0 > 0
            for k in PERIODS:
                if len(kd) > k:
                    base = kd[-1 - k]
                    qb = recs[base][kind].get(c, [0, 0])[0]
                    pb = recs[base]["px"].get(c)
                    listb = pb[3] if pb else list0
                    ratiob = qb / listb * 100 if listb else 0
                    dq = q0 - qb
                    pct = (dq / qb * 100) if qb > 0 else None
                    pxp = (close0 / pb[2] - 1) * 100 if pb and pb[2] else None
                    row += [dq, r2(pct), r2(ratio0 - ratiob, 4), r2(pxp)]
                    if dq != 0:
                        any_move = True
                else:
                    row += [None, None, None, None]
            streak = 0
            seq = [recs[d][kind].get(c, [0])[0] for d in kd[-21:]]
            for i in range(len(seq) - 1, 0, -1):
                diff = seq[i] - seq[i - 1]
                if diff > 0 and streak >= 0:
                    streak += 1
                elif diff < 0 and streak <= 0:
                    streak -= 1
                else:
                    break
            row.append(streak)
            if any_move:
                rows.append(row)
        out["kinds"][kind] = {"asof": asof, "n_days": len(kd), "rows": rows}

        idx = {d: i for i, d in enumerate(days)}
        for c in codes:
            if c not in px0:
                continue
            h = hist.setdefault(c, {})
            arr = [None] * len(days)
            for d in kd:
                arr[idx[d]] = recs[d][kind].get(c, [0])[0]
            h[kind[0]] = arr

    for c, h in hist.items():
        h["c"] = [recs[d]["px"].get(c, [0, 0, None])[2] for d in days]
        h["n"] = names.get(c, ("", ""))[0]

    buckets = {}
    for c, h in hist.items():
        buckets.setdefault(bucket(c), {})[c] = h
    HIST_DIR.mkdir(parents=True, exist_ok=True)
    written = set()
    for b, obj in buckets.items():
        fn = HIST_DIR / f"{b:02d}.json"
        save_json(fn, {"days": days, "codes": obj})
        written.add(fn.name)
    for p in HIST_DIR.glob("*.json"):
        if p.name not in written:
            p.unlink()

    out["n_bucket"] = N_BUCKET
    save_json(OUT_FILE, out)
    for kind in ("lend", "short"):
        k = out["kinds"].get(kind)
        print(f"[done] {kind}: " + (f"asof {k['asof']}, {len(k['rows'])} rows, {k['n_days']} days" if k else "no data"))


def main():
    if not DGK:
        print("[warn] DATA_GO_KR_KEY 미등록 → 대차 데이터는 건너뜁니다.")
    days = collect_days()
    if not days:
        print("[error] 수집된 거래일이 없습니다.")
        sys.exit(1)
    aggregate(days)


if __name__ == "__main__":
    main()
