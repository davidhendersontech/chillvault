"""
extract_themes.py
Sends Whisper transcript chunks to GPT-4o-mini and returns topics/timestamps as JSON.
Input:  ep345.json  (list of {start, end, text} segments from Whisper)
Output: ep345_themes.json
"""

import json
import time
import argparse
from pathlib import Path
from openai import OpenAI

client = OpenAI()  # uses OPENAI_API_KEY env var

# ── Config ─────────────────────────────────────────────────────────────────────
CHUNK_SECONDS   = 300   # 5-minute windows
OVERLAP_SECONDS = 30    # trailing overlap so topics don't get cut at boundaries
MODEL           = "gpt-4o-mini"
MAX_RETRIES     = 3
RETRY_DELAY     = 5     # seconds between retries

SYSTEM_PROMPT = """You are a podcast topic extractor. Given a transcript excerpt with timestamps, identify distinct topics, themes, and notable moments.

Return ONLY valid JSON matching this exact schema — no markdown, no explanation:
{
  "topics": [
    {
      "title": "Short topic title (4–7 words)",
      "description": "1–2 sentence summary of what's discussed",
      "start_seconds": <float>,
      "end_seconds": <float>,
      "tags": ["tag1", "tag2"],
      "confidence": <float 0.0–1.0>
    }
  ],
  "chapter_title": "Suggested chapter title if this excerpt feels like a distinct segment",
  "guests_mentioned": ["name1"],
  "key_quotes": [
    { "text": "verbatim short quote", "start_seconds": <float> }
  ]
}

Rules:
- start_seconds / end_seconds must fall within the provided window timestamps
- tags should be lowercase, single-word or hyphenated (e.g. "conspiracy", "true-crime")
- Omit fields that don't apply (empty arrays are fine, omit chapter_title if not a clear break)
- confidence reflects how clearly defined the topic boundary is"""


# ── Chunking ───────────────────────────────────────────────────────────────────
def chunk_segments(segments: list[dict], chunk_sec: int, overlap_sec: int) -> list[dict]:
    """Group whisper segments into time-window chunks with optional overlap."""
    if not segments:
        return []

    chunks = []
    chunk_start = segments[0]["start"]
    chunk_end   = chunk_start + chunk_sec
    current     = []

    for seg in segments:
        if seg["start"] < chunk_end:
            current.append(seg)
        else:
            if current:
                chunks.append({
                    "window_start": current[0]["start"],
                    "window_end":   current[-1]["end"],
                    "segments":     current,
                })
            # include overlap: keep segments from the last `overlap_sec`
            overlap_cutoff = chunk_end - overlap_sec
            current = [s for s in current if s["start"] >= overlap_cutoff]
            current.append(seg)
            chunk_start = chunk_end
            chunk_end   = chunk_start + chunk_sec

    if current:
        chunks.append({
            "window_start": current[0]["start"],
            "window_end":   current[-1]["end"],
            "segments":     current,
        })

    return chunks


def format_chunk_for_prompt(chunk: dict) -> str:
    """Convert a chunk into a readable transcript block with timestamps."""
    lines = [f"[Window: {chunk['window_start']:.1f}s – {chunk['window_end']:.1f}s]\n"]
    for seg in chunk["segments"]:
        ts = f"[{seg['start']:.1f}s]"
        lines.append(f"{ts} {seg['text'].strip()}")
    return "\n".join(lines)


# ── API call with retry ────────────────────────────────────────────────────────
def extract_chunk_themes(chunk: dict, episode_context: str = "") -> dict | None:
    user_content = ""
    if episode_context:
        user_content += f"Episode context: {episode_context}\n\n"
    user_content += format_chunk_for_prompt(chunk)

    for attempt in range(MAX_RETRIES):
        try:
            response = client.chat.completions.create(
                model=MODEL,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": user_content},
                ],
                temperature=0.2,   # low temp = consistent structure
                max_tokens=1000,
            )
            raw = response.choices[0].message.content
            result = json.loads(raw)
            # stamp the source window on every topic for downstream deduplication
            for topic in result.get("topics", []):
                topic["chunk_window_start"] = chunk["window_start"]
                topic["chunk_window_end"]   = chunk["window_end"]
            return result

        except json.JSONDecodeError as e:
            print(f"  ⚠ JSON parse error on attempt {attempt+1}: {e}")
        except Exception as e:
            print(f"  ⚠ API error on attempt {attempt+1}: {e}")

        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_DELAY * (attempt + 1))  # exponential-ish backoff

    return None


# ── Post-processing ────────────────────────────────────────────────────────────
def merge_chunk_results(chunk_results: list[dict], episode_id: str) -> dict:
    """Flatten per-chunk results into a single episode themes object."""
    all_topics    = []
    all_quotes    = []
    all_guests    = set()
    chapter_cuts  = []

    for cr in chunk_results:
        if cr is None:
            continue
        all_topics.extend(cr.get("topics", []))
        all_quotes.extend(cr.get("key_quotes", []))
        all_guests.update(cr.get("guests_mentioned", []))
        if cr.get("chapter_title"):
            chapter_cuts.append({
                "title":         cr["chapter_title"],
                "start_seconds": cr["topics"][0]["start_seconds"] if cr.get("topics") else None,
            })

    # sort everything chronologically
    all_topics.sort(key=lambda t: t.get("start_seconds", 0))
    all_quotes.sort(key=lambda q: q.get("start_seconds", 0))

    return {
        "episode_id":      episode_id,
        "topics":          all_topics,
        "key_quotes":      all_quotes,
        "guests_mentioned": sorted(all_guests),
        "chapters":        chapter_cuts,
        "total_topics":    len(all_topics),
    }


# ── Main ───────────────────────────────────────────────────────────────────────
def process_episode(input_path: str, episode_context: str = "") -> None:
    input_file  = Path(input_path)
    episode_id  = input_file.stem                          # e.g. "ep345"
    output_file = input_file.with_name(f"{episode_id}_themes.json")

    print(f"📂 Loading {input_file}")
    segments = json.loads(input_file.read_text())

    # support both raw list and {"segments": [...]} shapes
    if isinstance(segments, dict):
        segments = segments.get("segments", segments.get("results", []))

    print(f"   {len(segments)} segments loaded")
    chunks = chunk_segments(segments, CHUNK_SECONDS, OVERLAP_SECONDS)
    print(f"   → {len(chunks)} chunks ({CHUNK_SECONDS}s windows, {OVERLAP_SECONDS}s overlap)")

    results = []
    for i, chunk in enumerate(chunks):
        print(f"  🤖 Chunk {i+1}/{len(chunks)} "
              f"[{chunk['window_start']:.0f}s – {chunk['window_end']:.0f}s] ...", end=" ")
        result = extract_chunk_themes(chunk, episode_context)
        results.append(result)
        topics_found = len(result.get("topics", [])) if result else 0
        print(f"{'✓' if result else '✗'} {topics_found} topics")
        time.sleep(0.5)  # light rate limiting between chunks

    merged = merge_chunk_results(results, episode_id)
    output_file.write_text(json.dumps(merged, indent=2))
    print(f"\n✅ Done — {merged['total_topics']} topics → {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract themes from a Whisper transcript")
    parser.add_argument("input",   help="Path to episode JSON (e.g. ep345.json)")
    parser.add_argument("--context", default="", help="Brief episode context/description")
    args = parser.parse_args()
    process_episode(args.input, args.context)
