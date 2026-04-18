#!/usr/bin/env python3
"""
seed_episodes.py — populate Supabase episodes table from channel_metadata.json
Handles:
  - "Episode NNN: Title"                          → type=episode
  - "Episode NNN - Title"                         → type=episode
  - "Chilluminati Podcast - Episode NNN - Title"  → type=episode
  - "Midweek Mini: Title"                         → type=mini
  - "Midweek Mini - Title"                        → type=mini
  - "MINISODE NNN - Title"                        → type=mini
  - everything else                               → skipped
"""

import json
import re
import os
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

METADATA_FILE = "channel_metadata.json"

# Matches: "Episode 345: Title" or "Episode 345 - Title"
EPISODE_RE = re.compile(r"^Episode\s+(\d+)\s*[-:]\s*(.+)$", re.IGNORECASE)

# Matches: "Chilluminati Podcast - Episode 242 - Title"
CHILL_RE = re.compile(r"^Chilluminati Podcast\s*[-:]\s*Episode\s+(\d+)\s*[-:]\s*(.+)$", re.IGNORECASE)

# Matches: "Midweek Mini: Title" or "Midweek Mini - Title"
MINI_RE = re.compile(r"^Midweek Mini\s*[-:]\s*(.+)$", re.IGNORECASE)

# Matches: "MINISODE 100 - Title"
MINISODE_RE = re.compile(r"^MINISODE\s+(\d+)\s*[-:]\s*(.+)$", re.IGNORECASE)

data = json.loads(Path(METADATA_FILE).read_text())
entries = data.get("entries", [])

print(f"Total videos in channel: {len(entries)}")

to_insert = []
seen_ids  = set()
skipped   = []

for entry in entries:
    title    = entry.get("title", "").strip()
    yt_id    = entry.get("id", "")
    duration = entry.get("duration")
    uploaded = entry.get("upload_date")

    published_at = None
    if uploaded and len(uploaded) == 8:
        published_at = f"{uploaded[:4]}-{uploaded[4:6]}-{uploaded[6:]}T00:00:00+00:00"

    duration_sec = int(duration) if duration else None

    ep_match       = EPISODE_RE.match(title) or CHILL_RE.match(title)
    mini_match     = MINI_RE.match(title)
    minisode_match = MINISODE_RE.match(title)

    if ep_match:
        ep_num = int(ep_match.group(1))
        row_id = f"ep{ep_num}"
        if row_id in seen_ids:
            skipped.append(f"[DUPE] {title}")
            continue
        seen_ids.add(row_id)
        to_insert.append({
            "id":                row_id,
            "youtube_id":        yt_id,
            "title":             title,
            "episode_number":    ep_num,
            "episode_type":      "episode",
            "processing_status": "pending",
            "retry_count":       0,
            "published_at":      published_at,
            "duration_seconds":  duration_sec,
        })

    elif minisode_match:
        ep_num = int(minisode_match.group(1))
        row_id = f"minisode_{ep_num}"
        if row_id in seen_ids:
            skipped.append(f"[DUPE] {title}")
            continue
        seen_ids.add(row_id)
        to_insert.append({
            "id":                row_id,
            "youtube_id":        yt_id,
            "title":             title,
            "episode_number":    ep_num,
            "episode_type":      "mini",
            "processing_status": "pending",
            "retry_count":       0,
            "published_at":      published_at,
            "duration_seconds":  duration_sec,
        })

    elif mini_match:
        row_id = f"mini_{yt_id}"
        if row_id in seen_ids:
            skipped.append(f"[DUPE] {title}")
            continue
        seen_ids.add(row_id)
        to_insert.append({
            "id":                row_id,
            "youtube_id":        yt_id,
            "title":             title,
            "episode_number":    None,
            "episode_type":      "mini",
            "processing_status": "pending",
            "retry_count":       0,
            "published_at":      published_at,
            "duration_seconds":  duration_sec,
        })

    else:
        skipped.append(title)

episodes = [e for e in to_insert if e["episode_type"] == "episode"]
minis    = [e for e in to_insert if e["episode_type"] == "mini"]

print(f"Episodes:  {len(episodes)}")
print(f"Minis:     {len(minis)}")
print(f"Skipped:   {len(skipped)}")

if not to_insert:
    print("Nothing to insert!")
    exit()

print(f"\nInserting {len(to_insert)} rows into episodes table...")

BATCH = 50
for i in range(0, len(to_insert), BATCH):
    batch = to_insert[i:i + BATCH]
    supabase.table("episodes").upsert(batch, on_conflict="id").execute()
    print(f"  Inserted rows {i+1}–{min(i+BATCH, len(to_insert))}")

print("\nDone! Run: python bulk_process.py --dry-run  to verify")
