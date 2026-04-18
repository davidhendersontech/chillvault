#!/usr/bin/env python3
"""
bulk_process.py — ChillVault bulk episode processor
Reads from Supabase `episodes` table, skips already-processed, handles failures, resumable.

Usage:
    python bulk_process.py                           # process all unprocessed
    python bulk_process.py --limit 10                # process next 10
    python bulk_process.py --episode-id ep345        # reprocess specific episode
    python bulk_process.py --dry-run                 # show what would be processed
    python bulk_process.py --workers 2               # parallel workers (default: 2)
    python bulk_process.py --from-manifest eps.json  # use local manifest instead of DB
"""

import os
import sys
import json
import time
import signal
import logging
import argparse
import traceback
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event

from dotenv import load_dotenv
from supabase import create_client, Client
from extract_themes import process_episode

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

TRANSCRIPTS_DIR          = Path(os.environ.get("TRANSCRIPTS_DIR", "./transcripts"))
LOG_DIR                  = Path(os.environ.get("LOG_DIR", "./logs"))
RETRY_FAILED_AFTER_HOURS = 24
MAX_RETRIES_PER_EPISODE  = 3

# ── Logging ───────────────────────────────────────────────────────────────────

LOG_DIR.mkdir(parents=True, exist_ok=True)
log_file = LOG_DIR / f"bulk_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ── Graceful shutdown ─────────────────────────────────────────────────────────

shutdown_event = Event()

def _handle_signal(sig, frame):
    log.warning(f"Signal {sig} received — finishing current episodes then exiting...")
    shutdown_event.set()

signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ── Supabase helpers ──────────────────────────────────────────────────────────

def get_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_unprocessed_episodes(
    supabase: Client,
    limit: int | None = None,
    episode_id: str | None = None,
) -> list[dict]:
    if episode_id:
        resp = (
            supabase.table("episodes")
            .select("id, youtube_id, title, transcript_path, processing_status, retry_count, last_processed_at")
            .eq("id", episode_id)
            .execute()
        )
        return resp.data

    pending_q = (
        supabase.table("episodes")
        .select("id, youtube_id, title, transcript_path, processing_status, retry_count, last_processed_at")
        .in_("processing_status", ["pending"])
        .order("id")
    )
    if limit:
        pending_q = pending_q.limit(limit)
    pending = pending_q.execute().data

    remaining = (limit - len(pending)) if limit else None
    if remaining is None or remaining > 0:
        failed_q = (
            supabase.table("episodes")
            .select("id, youtube_id, title, transcript_path, processing_status, retry_count, last_processed_at")
            .eq("processing_status", "failed")
            .lt("retry_count", MAX_RETRIES_PER_EPISODE)
            .order("last_processed_at")
        )
        if remaining:
            failed_q = failed_q.limit(remaining)
        failed = failed_q.execute().data

        cutoff = time.time() - RETRY_FAILED_AFTER_HOURS * 3600
        failed = [
            ep for ep in failed
            if ep.get("last_processed_at") is None
            or datetime.fromisoformat(ep["last_processed_at"]).timestamp() < cutoff
        ]
    else:
        failed = []

    return pending + failed


def mark_episode(supabase: Client, episode_id: str, status: str, retry_increment: bool = False):
    update = {
        "processing_status": status,
        "last_processed_at": datetime.now(timezone.utc).isoformat(),
    }
    supabase.table("episodes").update(update).eq("id", episode_id).execute()
    if retry_increment:
        try:
            supabase.rpc("increment_retry_count", {"ep_id": episode_id}).execute()
        except Exception:
            pass


def upsert_topics(supabase: Client, episode_id: str, themes_data: dict):
    topics  = themes_data.get("topics", [])
    chapters = themes_data.get("chapters", [])
    quotes  = themes_data.get("key_quotes", [])
    guests  = themes_data.get("guests_mentioned", [])

    for topic in topics:
        topic_row = {
            "episode_id":   episode_id,
            "title":        topic["title"],
            "description":  topic.get("description", ""),
            "start_seconds": int(topic.get("start_seconds") or 0),
            "end_seconds":  int(topic["end_seconds"]) if topic.get("end_seconds") else None,
            "confidence":   topic.get("confidence", 0.8),
        }
        resp = supabase.table("topics").insert(topic_row).execute()
        topic_db_id = resp.data[0]["id"]

        for tag_name in topic.get("tags", []):
            tag_resp = supabase.table("tags").upsert(
                {"name": tag_name.lower()}, on_conflict="name"
            ).execute()
            tag_id = tag_resp.data[0]["id"]
            supabase.table("topic_tags").insert(
                {"topic_id": topic_db_id, "tag_id": tag_id}
            ).execute()

    for chap in chapters:
        if not chap.get("title"):
            continue
        supabase.table("chapters").upsert({
            "episode_id":   episode_id,
            "title":        chap["title"],
            "start_seconds": int(chap["start_seconds"]) if chap.get("start_seconds") else 0,
        }, on_conflict="episode_id,start_seconds").execute()

    for q in quotes:
        if not q.get("text"):
            continue
        supabase.table("key_quotes").insert({
            "episode_id":   episode_id,
            "text":         q["text"],
            "speaker":      q.get("speaker"),
            "start_seconds": int(q["start_seconds"]) if q.get("start_seconds") else None,
        }).execute()

    for name in guests:
        if not name:
            continue
        g_resp = supabase.table("guests").upsert(
            {"name": name}, on_conflict="name"
        ).execute()
        guest_id = g_resp.data[0]["id"]
        supabase.table("episode_guests").upsert({
            "episode_id": episode_id,
            "guest_id":   guest_id,
        }, on_conflict="episode_id,guest_id").execute()


# ── Per-episode worker ────────────────────────────────────────────────────────

def process_one(episode: dict) -> tuple[str, str, str | None]:
    ep_id = episode["id"]
    yt_id = episode.get("youtube_id", ep_id)
    title = episode.get("title", ep_id)

    supabase = get_client()

    # Locate transcript
    transcript_path = episode.get("transcript_path")
    if transcript_path:
        tpath = Path(transcript_path)
    else:
        candidates = [
            TRANSCRIPTS_DIR / f"{ep_id}.json",
            TRANSCRIPTS_DIR / f"{yt_id}.json",
            TRANSCRIPTS_DIR / f"{ep_id}.txt",
            TRANSCRIPTS_DIR / f"{yt_id}.txt",
        ]
        tpath = next((p for p in candidates if p.exists()), None)

    if not tpath or not tpath.exists():
        log.warning(f"[{ep_id}] No transcript found — skipping")
        return ep_id, "skipped", "no transcript"

    # themes file is written by extract_themes.py next to the transcript
    # e.g. transcripts/ep345.json → transcripts/ep345_themes.json
    themes_path = tpath.parent / f"{ep_id}_themes.json"

    # Check for cached themes (avoids re-calling GPT)
    themes_data = None
    if themes_path.exists():
        try:
            themes_data = json.loads(themes_path.read_text())
            log.info(f"[{ep_id}] Using cached themes file")
        except Exception:
            themes_data = None

    if themes_data is None:
        log.info(f"[{ep_id}] Extracting themes: {title[:60]}")
        mark_episode(supabase, ep_id, "processing")
        try:
            # process_episode writes to {ep_id}_themes.json and returns None
            process_episode(str(tpath), episode_context=title)
            themes_data = json.loads(themes_path.read_text())
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            log.error(f"[{ep_id}] Extraction failed: {err}")
            mark_episode(supabase, ep_id, "failed", retry_increment=True)
            return ep_id, "failed", err

    # Upsert into DB
    try:
        upsert_topics(supabase, ep_id, themes_data)
        mark_episode(supabase, ep_id, "done")
        log.info(f"[{ep_id}] ✓ Done — {len(themes_data.get('topics', []))} topics")
        return ep_id, "ok", None
    except Exception as e:
        err = f"DB upsert failed: {type(e).__name__}: {e}"
        log.error(f"[{ep_id}] {err}\n{traceback.format_exc()}")
        mark_episode(supabase, ep_id, "failed", retry_increment=True)
        return ep_id, "failed", err


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="ChillVault bulk episode processor")
    parser.add_argument("--limit",         type=int, help="Max episodes to process this run")
    parser.add_argument("--episode-id",    help="Process a specific episode ID")
    parser.add_argument("--workers",       type=int, default=2, help="Parallel workers (default: 2)")
    parser.add_argument("--dry-run",       action="store_true", help="List episodes without processing")
    parser.add_argument("--from-manifest", help="JSON file with list of episode dicts")
    args = parser.parse_args()

    supabase = get_client()

    if args.from_manifest:
        episodes = json.loads(Path(args.from_manifest).read_text())
        # retry manifest is a list of episode IDs or dicts
        if episodes and isinstance(episodes[0], str):
            # list of IDs — fetch full rows
            episodes = [
                supabase.table("episodes")
                .select("id, youtube_id, title, transcript_path, processing_status, retry_count, last_processed_at")
                .eq("id", eid)
                .execute()
                .data[0]
                for eid in episodes
            ]
        log.info(f"Loaded {len(episodes)} episodes from manifest")
    else:
        log.info("Fetching unprocessed episodes from Supabase...")
        episodes = fetch_unprocessed_episodes(supabase, limit=args.limit, episode_id=args.episode_id)
        log.info(f"Found {len(episodes)} episodes to process")

    if not episodes:
        log.info("Nothing to do. All episodes processed!")
        return

    if args.dry_run:
        print(f"\n{'ID':<20} {'STATUS':<12} {'RETRIES':<8} TITLE")
        print("-" * 80)
        for ep in episodes:
            print(f"{ep['id']:<20} {ep.get('processing_status','null'):<12} {ep.get('retry_count',0):<8} {ep.get('title','')[:50]}")
        print(f"\nTotal: {len(episodes)} episodes")
        return

    results    = {"ok": 0, "failed": 0, "skipped": 0}
    failed_ids = []
    start_time = time.time()

    log.info(f"Starting bulk processing: {len(episodes)} episodes, {args.workers} workers")
    log.info(f"Log: {log_file}")

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_one, ep): ep for ep in episodes}

        for future in as_completed(futures):
            if shutdown_event.is_set():
                log.warning("Shutdown requested — cancelling pending futures")
                for f in futures:
                    f.cancel()
                break

            ep_id, status, error = future.result()
            results[status] += 1

            if status == "failed":
                failed_ids.append(ep_id)

            total_done = sum(results.values())
            if total_done % 10 == 0:
                elapsed = time.time() - start_time
                rate = total_done / elapsed * 60
                remaining = len(episodes) - total_done
                eta_min = remaining / rate if rate > 0 else 0
                log.info(
                    f"Progress: {total_done}/{len(episodes)} "
                    f"| ✓{results['ok']} ✗{results['failed']} ⊘{results['skipped']} "
                    f"| {rate:.1f} ep/min | ETA {eta_min:.0f}m"
                )

    elapsed = time.time() - start_time
    log.info(
        f"\n{'='*60}\n"
        f"DONE in {elapsed/60:.1f}m\n"
        f"  ✓ OK:      {results['ok']}\n"
        f"  ✗ Failed:  {results['failed']}\n"
        f"  ⊘ Skipped: {results['skipped']}\n"
        f"{'='*60}"
    )

    if failed_ids:
        fail_log = LOG_DIR / f"failed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        fail_log.write_text("\n".join(failed_ids))
        log.info(f"Failed IDs written to: {fail_log}")

        retry_manifest = LOG_DIR / "retry_manifest.json"
        retry_manifest.write_text(json.dumps(failed_ids))
        log.info(f"Retry with: python bulk_process.py --from-manifest {retry_manifest}")


if __name__ == "__main__":
    main()
