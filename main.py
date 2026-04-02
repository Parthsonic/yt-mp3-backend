from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
from fastapi.responses import FileResponse
from starlette.background import BackgroundTask
import subprocess, uuid, os, re
import time
from concurrent.futures import ThreadPoolExecutor
from yt_dlp import YoutubeDL

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # allow all for now (dev only)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
COMPLETED_JOB_TTL_SECONDS = 2
FAILED_JOB_TTL_SECONDS = 2

jobs = {}
executor = ThreadPoolExecutor()

def safe_delete_file(file_path):
    if file_path and os.path.exists(file_path):
        try:
            os.remove(file_path)
        except OSError:
            pass

def cleanup_completed_job(file_id, file_path):
    safe_delete_file(file_path)
    jobs.pop(file_id, None)

def mark_job_for_expiry(file_id, ttl_seconds):
    if file_id in jobs:
        jobs[file_id]["expires_at"] = time.time() + ttl_seconds

def cleanup_expired_jobs():
    now = time.time()
    expired_job_ids = []

    for file_id, job in jobs.items():
        expires_at = job.get("expires_at")

        if expires_at and expires_at <= now:
            filename = job.get("filename")
            if filename:
                safe_delete_file(os.path.join(DOWNLOAD_DIR, filename))
            expired_job_ids.append(file_id)

    for file_id in expired_job_ids:
        jobs.pop(file_id, None)

def parse_download_percent(download_data):
    percent_str = str(download_data.get("_percent_str", "0%"))
    sanitized = re.sub(r"\x1b\[[0-9;]*m", "", percent_str).replace("%", "").strip()

    try:
        return float(sanitized)
    except ValueError:
        downloaded_bytes = download_data.get("downloaded_bytes") or 0
        total_bytes = download_data.get("total_bytes") or download_data.get("total_bytes_estimate") or 0

        if total_bytes:
            return (downloaded_bytes / total_bytes) * 100

        return 0.0

def parse_time_to_seconds(raw_value):
    if not raw_value:
        return None

    if isinstance(raw_value, (int, float)):
        return float(raw_value)

    text = str(raw_value).strip()

    if not text:
        return None

    if ":" not in text:
        try:
            return float(text)
        except ValueError:
            return None

    parts = text.split(":")

    try:
        total_seconds = 0.0
        for part in parts:
            total_seconds = total_seconds * 60 + float(part)
        return total_seconds
    except ValueError:
        return None

def update_download_progress(file_id, percent):
    mapped_progress = min(max(int(percent * 0.6), 0), 60)
    jobs[file_id]["progress"] = max(jobs[file_id].get("progress", 0), mapped_progress)
    jobs[file_id]["text"] = f"Downloading... {percent:.1f}%"

def run_ffmpeg_with_progress(file_id, input_file, output_file, bitrate, duration_seconds):
    ffmpeg_cmd = [
        "ffmpeg",
        "-loglevel", "error",
        "-i", input_file,
        "-vn",
        "-acodec", "libmp3lame",
        "-ab", f"{bitrate}k",
        "-ar", "44100",
        "-y",
        "-progress", "pipe:1",
        "-nostats",
        output_file
    ]

    process = subprocess.Popen(
        ffmpeg_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )

    stderr_output = []

    if process.stdout:
        for raw_line in process.stdout:
            line = raw_line.strip()
            if not line or "=" not in line:
                continue

            key, value = line.split("=", 1)

            if key == "out_time_ms":
                try:
                    current_seconds = int(value) / 1_000_000
                except ValueError:
                    continue

                if duration_seconds and duration_seconds > 0:
                    conversion_percent = min(current_seconds / duration_seconds, 1.0)
                    jobs[file_id]["progress"] = max(60, min(99, 60 + int(conversion_percent * 39)))
                    jobs[file_id]["text"] = f"Converting... {conversion_percent * 100:.1f}%"
            elif key == "out_time":
                current_seconds = parse_time_to_seconds(value)

                if current_seconds is not None and duration_seconds and duration_seconds > 0:
                    conversion_percent = min(current_seconds / duration_seconds, 1.0)
                    jobs[file_id]["progress"] = max(60, min(99, 60 + int(conversion_percent * 39)))
                    jobs[file_id]["text"] = f"Converting... {conversion_percent * 100:.1f}%"
            elif key == "progress" and value == "end":
                jobs[file_id]["progress"] = 99
                jobs[file_id]["text"] = "Conversion complete"

    if process.stderr:
        stderr_output = process.stderr.read()

    return_code = process.wait()

    if return_code != 0:
        raise RuntimeError(f"FFmpeg failed: {stderr_output}")

def process_convert(file_id, url, bitrate):
    base_path = f"{DOWNLOAD_DIR}/{file_id}"
    try:
        jobs[file_id]["status"] = "downloading"
        jobs[file_id]["progress"] = 0
        jobs[file_id]["text"] = "Initializing..."

        def progress_hook(download_data):
            status = download_data.get("status")

            if status == "downloading":
                percent = parse_download_percent(download_data)
                update_download_progress(file_id, percent)
            elif status == "finished":
                jobs[file_id]["progress"] = 60
                jobs[file_id]["text"] = "Download complete, converting..."

        ydl_opts = {
            "format": "bestaudio",
            "outtmpl": f"{base_path}.%(ext)s",
            "noplaylist": True,
            "progress_hooks": [progress_hook],
            "cookiesfrombrowser": ("chrome",),
        }

        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)

        raw_title = info.get("title", "audio")
        duration_seconds = parse_time_to_seconds(info.get("duration")) or 0
        safe_title = clean_filename(raw_title) or f"audio_{file_id}"

        # STEP 2 — Find downloaded file
        jobs[file_id]["status"] = "processing"
        jobs[file_id]["progress"] = 61
        jobs[file_id]["text"] = "Finding file..."
        input_file = None
        for f in os.listdir(DOWNLOAD_DIR):
            if f.startswith(file_id):
                input_file = os.path.join(DOWNLOAD_DIR, f)
                break

        if not input_file:
            jobs[file_id]["status"] = "error"
            jobs[file_id]["text"] = "Downloaded file not found"
            return

        jobs[file_id]["progress"] = 63
        jobs[file_id]["text"] = "File found"

        # STEP 3 — Convert to MP3 using FFmpeg
        jobs[file_id]["status"] = "converting"
        jobs[file_id]["progress"] = 64
        jobs[file_id]["text"] = "Starting conversion..."
        output_file = f"{DOWNLOAD_DIR}/{safe_title}.mp3"
        jobs[file_id]["filename"] = f"{safe_title}.mp3"
        run_ffmpeg_with_progress(file_id, input_file, output_file, bitrate, duration_seconds)
        safe_delete_file(input_file)

        # STEP 4 — Ensure file exists
        if not os.path.exists(output_file):
            jobs[file_id]["status"] = "error"
            jobs[file_id]["text"] = "MP3 file not created"
            return

        jobs[file_id]["progress"] = 100
        jobs[file_id]["text"] = "Complete"
        jobs[file_id]["status"] = "complete"
        mark_job_for_expiry(file_id, COMPLETED_JOB_TTL_SECONDS)
    except Exception as e:
        jobs[file_id]["status"] = "error"
        jobs[file_id]["text"] = str(e)
        mark_job_for_expiry(file_id, FAILED_JOB_TTL_SECONDS)

def clean_filename(name):
    # remove invalid characters
    name = re.sub(r'[\\/*?:"<>|]', "", name)
    # trim to 100 chars
    return name[:100]

@app.get("/convert")
def convert(url: str, bitrate: str = "192"):
    cleanup_expired_jobs()
    print("Received request to convert:", url, "with bitrate:", bitrate)
    file_id = str(uuid.uuid4())
    jobs[file_id] = {"status": "processing", "progress": 0, "text": "Initializing..."}
    executor.submit(process_convert, file_id, url, bitrate)
    return {"job_id": file_id}

@app.get("/progress/{file_id}")
def get_progress(file_id: str):
    cleanup_expired_jobs()
    job = jobs.get(file_id, {"status": "not found"})
    return {
        "status": job.get("status"),
        "progress": job.get("progress"),
        "text": job.get("text"),
        "filename": job.get("filename")  # 👈 ADD THIS
    }

@app.get("/download/{file_id}")
def download_file(file_id: str):
    cleanup_expired_jobs()
    job = jobs.get(file_id)

    if not job or job["status"] != "complete":
        return {"error": "File not ready"}

    filename = job.get("filename")
    file_path = f"{DOWNLOAD_DIR}/{filename}"

    if os.path.exists(file_path):
        return FileResponse(
            file_path,
            media_type="audio/mpeg",
            filename=filename,  # 👈 important
            background=BackgroundTask(cleanup_completed_job, file_id, file_path),
        )

    return {"error": "File not found"}
