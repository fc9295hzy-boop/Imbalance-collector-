"""
Collecteur Order Book Imbalance -- BTC (Hyperliquid)
=====================================================
Concu pour tourner en continu sur Railway (meme principe que
FundingReversion : boucle infinie + persistance SQLite sur Volume).

CE QU'IL FAIT
  Toutes les COLLECT_INTERVAL_SECONDS, interroge l2Book (carnet d'ordres
  public, gratuit, sans cle API) et enregistre :
    - le mid-price du moment
    - le desequilibre bid/ask (imbalance) a plusieurs profondeurs
      (5, 10, 20 niveaux de prix)

  L'imbalance est calcule ainsi :
    imbalance = (volume_bids - volume_asks) / (volume_bids + volume_asks)
  Valeur entre -1 (mur de vente ecrasant) et +1 (mur d'achat ecrasant).

POURQUOI COLLECTER D'ABORD, ANALYSER APRES
  Contrairement aux bougies et au funding, Hyperliquid NE FOURNIT PAS
  d'historique gratuit du carnet d'ordres (verifie -- seul un snapshot
  instantane est expose via l'API publique, l'archive S3 officielle est
  payante). Il faut donc accumuler nous-memes les donnees en continu
  AVANT de pouvoir tester si l'imbalance a un pouvoir predictif —
  exactement comme le cold-start de FundingReversion, mais cote donnees
  plutot que cote seuils.

  Le script imbalance_analysis.py (a lancer separement, une fois assez
  de donnees accumulees) fera le test de validite : correlation entre
  imbalance et rendement futur. C'est LUI qui dira si l'idee merite un
  backtest complet, pas ce collecteur.

DEPLOIEMENT RAILWAY
  Memes fichiers que FundingReversion : ce script + requirements.txt
  (requests uniquement, pas besoin de pandas/numpy ici) + Volume monte
  sur /data pour la persistance SQLite.
"""

import os
import sqlite3
import time
from datetime import datetime, timezone

import requests

HL_INFO_URL = "https://api.hyperliquid.xyz/info"
COIN = "BTC"

COLLECT_INTERVAL_SECONDS = 15  # frequence de collecte
DEPTHS = (5, 10, 20)            # profondeurs de carnet analysees (niveaux de prix)
DB_PATH = os.environ.get("DB_PATH", "/data/orderbook_imbalance.db")

MAX_CONSECUTIVE_ERRORS_BEFORE_BACKOFF = 5


def log(msg):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} | {msg}", flush=True)


def init_db(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            time_ms INTEGER NOT NULL,
            mid_price REAL NOT NULL,
            best_bid REAL,
            best_ask REAL,
            imbalance_5 REAL,
            imbalance_10 REAL,
            imbalance_20 REAL,
            bid_vol_20 REAL,
            ask_vol_20 REAL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_time ON snapshots(time_ms)")
    conn.commit()
    return conn


def fetch_l2_book():
    """Recupere le carnet d'ordres brut. Retourne None en cas d'echec
    -- ne leve jamais d'exception (coherent avec le reste des bots)."""
    try:
        resp = requests.post(
            HL_INFO_URL,
            json={"type": "l2Book", "coin": COIN},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log(f"[ERROR] fetch_l2_book : {e}")
        return None


def compute_imbalances(book):
    """
    book["levels"] = [bids, asks], chacun une liste de {"px", "sz", "n"}
    triee du meilleur prix vers le pire.
    Retourne un dict avec mid_price, best_bid, best_ask, et l'imbalance
    a chaque profondeur de DEPTHS. None si donnees incoherentes.
    """
    try:
        levels = book.get("levels")
        if not levels or len(levels) != 2:
            return None
        bids, asks = levels[0], levels[1]
        if not bids or not asks:
            return None

        best_bid = float(bids[0]["px"])
        best_ask = float(asks[0]["px"])
        mid_price = (best_bid + best_ask) / 2

        result = {"mid_price": mid_price, "best_bid": best_bid, "best_ask": best_ask}

        for depth in DEPTHS:
            bid_vol = sum(float(l["sz"]) for l in bids[:depth])
            ask_vol = sum(float(l["sz"]) for l in asks[:depth])
            total = bid_vol + ask_vol
            imbalance = (bid_vol - ask_vol) / total if total > 0 else 0.0
            result[f"imbalance_{depth}"] = imbalance
            if depth == 20:
                result["bid_vol_20"] = bid_vol
                result["ask_vol_20"] = ask_vol

        return result
    except (KeyError, ValueError, TypeError, IndexError) as e:
        log(f"[ERROR] compute_imbalances : {e}")
        return None


def save_snapshot(conn, data):
    try:
        conn.execute("""
            INSERT INTO snapshots
            (time_ms, mid_price, best_bid, best_ask,
             imbalance_5, imbalance_10, imbalance_20, bid_vol_20, ask_vol_20)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            int(datetime.now(timezone.utc).timestamp() * 1000),
            data["mid_price"], data["best_bid"], data["best_ask"],
            data["imbalance_5"], data["imbalance_10"], data["imbalance_20"],
            data["bid_vol_20"], data["ask_vol_20"],
        ))
        conn.commit()
        return True
    except Exception as e:
        log(f"[ERROR] save_snapshot : {e}")
        return False


def get_stats(conn):
    try:
        cur = conn.execute("SELECT COUNT(*), MIN(time_ms), MAX(time_ms) FROM snapshots")
        n, tmin, tmax = cur.fetchone()
        if n == 0:
            return n, None, None
        cov_hours = (tmax - tmin) / 1000 / 3600
        return n, cov_hours, tmax
    except Exception as e:
        log(f"[ERROR] get_stats : {e}")
        return 0, None, None


def main():
    log(f"Demarrage collecteur order book imbalance -- {COIN}, "
        f"intervalle {COLLECT_INTERVAL_SECONDS}s, profondeurs {DEPTHS}")
    conn = init_db(DB_PATH)

    n0, cov0, _ = get_stats(conn)
    log(f"Base existante : {n0} snapshots, {cov0:.1f}h de couverture" if n0 else
        "Base vide, premiere collecte.")

    consecutive_errors = 0

    while True:
        book = fetch_l2_book()

        if book is None:
            consecutive_errors += 1
            backoff = min(COLLECT_INTERVAL_SECONDS * (2 ** min(consecutive_errors, 5)), 300)
            log(f"[WARN] Echec #{consecutive_errors}, nouvelle tentative dans {backoff}s")
            time.sleep(backoff)
            continue

        data = compute_imbalances(book)
        if data is None:
            consecutive_errors += 1
            time.sleep(COLLECT_INTERVAL_SECONDS)
            continue

        consecutive_errors = 0
        ok = save_snapshot(conn, data)

        n, cov_hours, _ = get_stats(conn)
        if ok and n % 200 == 0:  # log de progression toutes les ~200 collectes
            log(f"[PROGRESS] {n} snapshots -- {cov_hours:.1f}h de couverture -- "
                f"mid={data['mid_price']:.1f} | "
                f"imb5={data['imbalance_5']:+.3f} imb10={data['imbalance_10']:+.3f} "
                f"imb20={data['imbalance_20']:+.3f}")

        time.sleep(COLLECT_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Arret demande (Ctrl+C).")
    except Exception as e:
        log(f"[FATAL ERROR] {e}")
        raise
