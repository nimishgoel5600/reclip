import os
import re
import uuid
import glob
import json
import time
import subprocess
import threading
import logging
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify, send_file, render_template

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MAX_WORKERS = 4  # max concurrent downloads
JOB_TTL = 3600  # seconds before completed jobs are cleaned up
MAX_FILE_SIZE = "2G"  # yt-dlp max file size limit
DOWNLOAD_TIMEOUT = 600  # 10 min per download

app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")
COOKIE_PATH = os.path.join(BASE_DIR, "cookies.txt")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("reclip")

jobs = {}
jobs_lock = threading.Lock()
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)


def _build_cmd():
    """Build base yt-dlp command with cookie support."""
    cmd = [
        "yt-dlp",
        "--no-warnings",
        "--no-check-certificates",
        "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "--extractor-args", "youtube:player_client=android,web",
    ]
    if os.path.isfile(COOKIE_PATH):
        cmd += ["--cookies", COOKIE_PATH]
    return cmd


# ---------------------------------------------------------------------------
# Simple rate limiter (per IP)
# ---------------------------------------------------------------------------
_rate = {}
_rate_lock = threading.Lock()
RATE_WINDOW = 60  # seconds
RATE_LIMIT = 30  # requests per window


def _check_rate(ip):
    now = time.time()
    with _rate_lock:
        stamps = _rate.get(ip, [])
        stamps = [t for t in stamps if now - t < RATE_WINDOW]
        if len(stamps) >= RATE_LIMIT:
            return False
        stamps.append(now)
        _rate[ip] = stamps
        return True


# ---------------------------------------------------------------------------
# Job cleanup thread
# ---------------------------------------------------------------------------
def _cleanup_loop():
    while True:
        time.sleep(300)  # every 5 min
        now = time.time()
        to_remove = []
        with jobs_lock:
            for jid, job in jobs.items():
                if job.get("finished_at") and now - job["finished_at"] > JOB_TTL:
                    to_remove.append(jid)
            for jid in to_remove:
                job = jobs.pop(jid, {})
                fpath = job.get("file")
                if fpath and os.path.isfile(fpath):
                    try:
                        os.remove(fpath)
                    except OSError:
                        pass
                log.info("Cleaned up job %s", jid)

_cleanup_thread = threading.Thread(target=_cleanup_loop, daemon=True)
_cleanup_thread.start()


# ---------------------------------------------------------------------------
# Self-ping to prevent free-tier hosting from sleeping
# ---------------------------------------------------------------------------
def _keep_alive():
    import urllib.request
    app_url = os.environ.get("RENDER_EXTERNAL_URL")
    if not app_url:
        return  # only run on Render
    health_url = f"{app_url}/health"
    log.info("Keep-alive enabled: pinging %s every 5 min", health_url)
    while True:
        time.sleep(290)  # ~5 min
        try:
            urllib.request.urlopen(health_url, timeout=10)
        except Exception:
            pass

_keepalive_thread = threading.Thread(target=_keep_alive, daemon=True)
_keepalive_thread.start()


# ---------------------------------------------------------------------------
# Secure filename helper
# ---------------------------------------------------------------------------
def safe_filename(title, ext):
    if not title:
        return None
    # Remove anything that isn't alphanumeric, space, dash, underscore, or dot
    safe = re.sub(r'[^\w\s\-.]', '', title).strip()
    safe = re.sub(r'\s+', '_', safe)[:80]
    if not safe:
        return None
    return f"{safe}{ext}"


# ---------------------------------------------------------------------------
# Parse yt-dlp progress from stderr
# ---------------------------------------------------------------------------
_PROGRESS_RE = re.compile(r'\[download\]\s+([\d.]+)%')


def run_download(job_id, url, format_choice, format_id, subtitles, playlist):
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return

    if playlist:
        out_template = os.path.join(DOWNLOAD_DIR, f"{job_id}_%(playlist_index)s.%(ext)s")
    else:
        out_template = os.path.join(DOWNLOAD_DIR, f"{job_id}.%(ext)s")

    cmd = _build_cmd() + ["-o", out_template, "--max-filesize", MAX_FILE_SIZE]

    if not playlist:
        cmd.append("--no-playlist")

    # Format selection
    if format_choice == "audio":
        cmd += ["-x", "--audio-format", "mp3"]
    elif format_choice == "audio-wav":
        cmd += ["-x", "--audio-format", "wav"]
    elif format_choice == "audio-flac":
        cmd += ["-x", "--audio-format", "flac"]
    elif format_id:
        cmd += ["-f", f"{format_id}+bestaudio/best", "--merge-output-format", "mp4"]
    else:
        cmd += ["-f", "bestvideo+bestaudio/best", "--merge-output-format", "mp4"]

    # Subtitles
    if subtitles:
        cmd += ["--write-subs", "--write-auto-subs", "--sub-lang", "en", "--embed-subs"]

    # Retry & network resilience
    cmd += ["--retries", "3", "--fragment-retries", "3"]

    cmd.append(url)
    log.info("Job %s started: %s", job_id, " ".join(cmd))

    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        deadline = time.time() + DOWNLOAD_TIMEOUT

        for line in proc.stdout:
            if time.time() > deadline:
                proc.kill()
                with jobs_lock:
                    job["status"] = "error"
                    job["error"] = f"Download timed out ({DOWNLOAD_TIMEOUT // 60} min limit)"
                    job["finished_at"] = time.time()
                return
            m = _PROGRESS_RE.search(line)
            if m:
                with jobs_lock:
                    job["progress"] = float(m.group(1))

        proc.wait()
        if proc.returncode != 0:
            with jobs_lock:
                job["status"] = "error"
                job["error"] = "Download failed — the platform may have blocked the request"
                job["finished_at"] = time.time()
            return

        files = glob.glob(os.path.join(DOWNLOAD_DIR, f"{job_id}*"))
        # Filter out subtitle-only files for the main file list
        media_files = [f for f in files if not f.endswith(('.vtt', '.srt', '.ass'))]
        if not media_files:
            with jobs_lock:
                job["status"] = "error"
                job["error"] = "Download completed but no file was found"
                job["finished_at"] = time.time()
            return

        # Pick the right file
        if format_choice.startswith("audio"):
            ext_pref = {"audio": ".mp3", "audio-wav": ".wav", "audio-flac": ".flac"}.get(format_choice, ".mp3")
            target = [f for f in media_files if f.endswith(ext_pref)]
        else:
            target = [f for f in media_files if f.endswith(".mp4")]
        chosen = target[0] if target else media_files[0]

        # Cleanup extra files (not the chosen one)
        for f in files:
            if f != chosen:
                try:
                    os.remove(f)
                except OSError:
                    pass

        ext = os.path.splitext(chosen)[1]
        title = job.get("title", "")
        filename = safe_filename(title, ext) or os.path.basename(chosen)

        with jobs_lock:
            job["status"] = "done"
            job["file"] = chosen
            job["filename"] = filename
            job["progress"] = 100
            job["finished_at"] = time.time()

        log.info("Job %s completed: %s", job_id, filename)

    except Exception as e:
        log.exception("Job %s failed", job_id)
        with jobs_lock:
            job["status"] = "error"
            job["error"] = str(e)
            job["finished_at"] = time.time()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    has_cookies = os.path.isfile(COOKIE_PATH)
    return render_template("index.html", has_cookies=has_cookies)


@app.route("/health")
def health():
    return jsonify({"status": "ok", "active_jobs": sum(1 for j in jobs.values() if j["status"] == "downloading")})


@app.route("/api/info", methods=["POST"])
def get_info():
    if not _check_rate(request.remote_addr):
        return jsonify({"error": "Too many requests — slow down"}), 429

    data = request.json or {}
    url = data.get("url", "").strip()
    if not url:
        return jsonify({"error": "No URL provided"}), 400

    cmd = _build_cmd() + ["--no-playlist", "-j", url]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            err = result.stderr.strip().split("\n")[-1] if result.stderr.strip() else "Unknown error"
            return jsonify({"error": err}), 400

        info = json.loads(result.stdout)

        # Build quality options — keep best format per resolution
        best_by_height = {}
        for f in info.get("formats", []):
            height = f.get("height")
            if height and f.get("vcodec", "none") != "none":
                tbr = f.get("tbr") or 0
                if height not in best_by_height or tbr > (best_by_height[height].get("tbr") or 0):
                    best_by_height[height] = f

        formats = []
        for height, f in best_by_height.items():
            size_mb = round((f.get("filesize") or f.get("filesize_approx") or 0) / 1048576, 1)
            formats.append({
                "id": f["format_id"],
                "label": f"{height}p",
                "height": height,
                "size": f"{size_mb}MB" if size_mb else "",
            })
        formats.sort(key=lambda x: x["height"], reverse=True)

        return jsonify({
            "title": info.get("title", ""),
            "thumbnail": info.get("thumbnail", ""),
            "duration": info.get("duration"),
            "uploader": info.get("uploader", ""),
            "formats": formats,
            "extractor": info.get("extractor", ""),
            "webpage_url": info.get("webpage_url", url),
        })
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Timed out fetching video info — try again"}), 400
    except json.JSONDecodeError:
        return jsonify({"error": "Could not parse video info"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/download", methods=["POST"])
def start_download():
    if not _check_rate(request.remote_addr):
        return jsonify({"error": "Too many requests — slow down"}), 429

    data = request.json or {}
    url = data.get("url", "").strip()
    format_choice = data.get("format", "video")
    format_id = data.get("format_id")
    title = data.get("title", "")
    subtitles = data.get("subtitles", False)
    playlist = data.get("playlist", False)

    if not url:
        return jsonify({"error": "No URL provided"}), 400

    # Validate format_choice
    if format_choice not in ("video", "audio", "audio-wav", "audio-flac"):
        format_choice = "video"

    job_id = uuid.uuid4().hex[:12]
    with jobs_lock:
        jobs[job_id] = {
            "status": "downloading",
            "url": url,
            "title": title,
            "progress": 0,
            "created_at": time.time(),
            "finished_at": None,
        }

    executor.submit(run_download, job_id, url, format_choice, format_id, subtitles, playlist)
    return jsonify({"job_id": job_id})


@app.route("/api/status/<job_id>")
def check_status(job_id):
    # Validate job_id format (hex string, 12 chars)
    if not re.match(r'^[a-f0-9]{12}$', job_id):
        return jsonify({"error": "Invalid job ID"}), 400

    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify({
        "status": job["status"],
        "error": job.get("error"),
        "filename": job.get("filename"),
        "progress": job.get("progress", 0),
    })


@app.route("/api/file/<job_id>")
def download_file(job_id):
    if not re.match(r'^[a-f0-9]{12}$', job_id):
        return jsonify({"error": "Invalid job ID"}), 400

    with jobs_lock:
        job = jobs.get(job_id)
    if not job or job["status"] != "done":
        return jsonify({"error": "File not ready"}), 404

    filepath = job.get("file")
    if not filepath or not os.path.isfile(filepath):
        return jsonify({"error": "File no longer available"}), 404

    # Ensure the file is within DOWNLOAD_DIR (path traversal protection)
    real_path = os.path.realpath(filepath)
    if not real_path.startswith(os.path.realpath(DOWNLOAD_DIR)):
        return jsonify({"error": "Access denied"}), 403

    return send_file(real_path, as_attachment=True, download_name=job.get("filename", "download"))


@app.route("/api/cookies", methods=["POST"])
def upload_cookies():
    """Upload cookies.txt from the browser for YouTube/other auth."""
    text = request.get_data(as_text=True)
    if not text or len(text) < 10:
        return jsonify({"error": "Empty or invalid cookie data"}), 400
    if len(text) > 500_000:
        return jsonify({"error": "Cookie file too large"}), 400

    # Basic validation: Netscape cookie format starts with comments or domain
    lines = text.strip().split("\n")
    valid = any(l.strip().startswith(("#", ".")) or "\t" in l for l in lines[:5])
    if not valid:
        return jsonify({"error": "Invalid format — must be Netscape cookie format (cookies.txt)"}), 400

    with open(COOKIE_PATH, "w") as f:
        f.write(text)

    log.info("Cookies uploaded (%d bytes)", len(text))
    return jsonify({"ok": True, "message": "Cookies saved — YouTube should work now"})


@app.route("/api/cookies", methods=["DELETE"])
def delete_cookies():
    """Remove uploaded cookies."""
    if os.path.isfile(COOKIE_PATH):
        os.remove(COOKIE_PATH)
    return jsonify({"ok": True})


@app.route("/api/cookies/status")
def cookies_status():
    return jsonify({"has_cookies": os.path.isfile(COOKIE_PATH)})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8899))
    host = os.environ.get("HOST", "127.0.0.1")
    log.info("ReClip starting on %s:%s (workers=%d)", host, port, MAX_WORKERS)
    app.run(host=host, port=port)
