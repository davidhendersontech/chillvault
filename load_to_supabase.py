"""
load_to_supabase.py
Reads an ep{N}_themes.json file and upserts everything into Supabase.

Requires:
  pip install supabase python-dotenv
  .env with SUPABASE_URL and SUPABASE_SERVICE_KEY
"""

import json
import argparse
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

from supabase import create_client

sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])


def upsert_tags(tag_names: list[str]) -> dict[str, int]:
    """Ensure tags exist and return {name: id} map."""
    if not tag_names:
        return {}
    # upsert by name; on_conflict does nothing, returning id either way
    sb.table("tags").upsert(
        [{"name": t} for t in tag_names], on_conflict="name"
    ).execute()
    rows = sb.table("tags").select("id, name").in_("name", tag_names).execute().data
    return {r["name"]: r["id"] for r in rows}


def load_themes(themes_path: str, episode_meta: dict) -> None:
    data       = json.loads(Path(themes_path).read_text())
    episode_id = data["episode_id"]

    # ── 1. Upsert episode row ──────────────────────────────────────────────────
    episode_row = {
        "id":             episode_id,
        "title":          episode_meta.get("title", episode_id),
        "episode_number": episode_meta.get("episode_number"),
        "description":    episode_meta.get("description"),
        "published_at":   episode_meta.get("published_at"),
        "duration_sec":   episode_meta.get("duration_sec"),
        "youtube_id":     episode_meta.get("youtube_id"),
        "audio_url":      episode_meta.get("audio_url"),
        "thumbnail_url":  episode_meta.get("thumbnail_url"),
        "processed_at":   "now()",
    }
    sb.table("episodes").upsert(episode_row, on_conflict="id").execute()
    print(f"✓ Episode {episode_id} upserted")

    # ── 2. Collect all tags up front (one round trip) ─────────────────────────
    all_tag_names = set()
    for topic in data.get("topics", []):
        all_tag_names.update(topic.get("tags", []))
    tag_map = upsert_tags(list(all_tag_names))

    # ── 3. Upsert topics + tags ───────────────────────────────────────────────
    for topic in data.get("topics", []):
        topic_row = {
            "episode_id":         episode_id,
            "title":              topic["title"],
            "description":        topic.get("description"),
            "start_seconds":      topic["start_seconds"],
            "end_seconds":        topic["end_seconds"],
            "confidence":         topic.get("confidence"),
            "chunk_window_start": topic.get("chunk_window_start"),
            "chunk_window_end":   topic.get("chunk_window_end"),
        }
        result = sb.table("topics").insert(topic_row).execute()
        topic_id = result.data[0]["id"]

        tag_names = topic.get("tags", [])
        if tag_names:
            sb.table("topic_tags").insert([
                {"topic_id": topic_id, "tag_id": tag_map[t]}
                for t in tag_names if t in tag_map
            ]).execute()

    print(f"  → {len(data.get('topics', []))} topics inserted")

    # ── 4. Upsert chapters ────────────────────────────────────────────────────
    chapters = data.get("chapters", [])
    if chapters:
        sb.table("chapters").insert([
            {
                "episode_id":   episode_id,
                "title":        c["title"],
                "start_seconds": c.get("start_seconds") or 0,
            }
            for c in chapters if c.get("title")
        ]).execute()
        print(f"  → {len(chapters)} chapters inserted")

    # ── 5. Upsert key quotes ──────────────────────────────────────────────────
    quotes = data.get("key_quotes", [])
    if quotes:
        sb.table("key_quotes").insert([
            {
                "episode_id":   episode_id,
                "text":         q["text"],
                "start_seconds": q["start_seconds"],
            }
            for q in quotes if q.get("text")
        ]).execute()
        print(f"  → {len(quotes)} quotes inserted")

    # ── 6. Guests ─────────────────────────────────────────────────────────────
    guest_names = data.get("guests_mentioned", [])
    if guest_names:
        sb.table("guests").upsert(
            [{"name": g} for g in guest_names], on_conflict="name"
        ).execute()
        guest_rows = sb.table("guests").select("id, name").in_("name", guest_names).execute().data
        sb.table("episode_guests").upsert([
            {"episode_id": episode_id, "guest_id": r["id"]} for r in guest_rows
        ], on_conflict="episode_id,guest_id").execute()
        print(f"  → {len(guest_names)} guests linked")

    print("✅ Load complete\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("themes_json", help="Path to ep345_themes.json")
    parser.add_argument("--title",          default="")
    parser.add_argument("--episode-number", type=int)
    parser.add_argument("--youtube-id",     default="")
    parser.add_argument("--published-at",   default=None)
    parser.add_argument("--duration-sec",   type=int)
    args = parser.parse_args()

    meta = {
        "title":          args.title,
        "episode_number": args.episode_number,
        "youtube_id":     args.youtube_id,
        "published_at":   args.published_at,
        "duration_sec":   args.duration_sec,
    }
    load_themes(args.themes_json, meta)
