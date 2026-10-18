from flask import Flask, request, jsonify
import yt_dlp
import os
import gc
import time
import uuid
import threading
from queue import Queue

app = Flask(__name__)

# ==================== কনফিগারেশন (512MB RAM Optimized) ====================
BATCH_SIZE = 10          # এক ব্যাচে সর্বোচ্চ ১০টি করে লিংক নেওয়া হবে
MAX_WORKERS = 2          # ৫১২ এমবি র‍্যাম সুরক্ষিত রাখতে ২টি থ্রেড একসাথে কাজ করবে
CACHE_LIMIT = 300        # ৩০০টি ভিডিওর লিংক ক্যাশে থাকবে (র‍্যাম খরচ মাত্র ২ এমবি)
CACHE_EXPIRY = 7200      # ২ ঘণ্টা পর্যন্ত ক্যাশ মেমোরিতে থাকবে

task_queue = Queue()
jobs = {}                # job_id -> status, result, position, created_at
url_cache = {}           # video_url -> {data, timestamp}
lock = threading.Lock()

# ==================== ক্যাশ ও মেমোরি ক্লিনার ====================
def clean_old_records():
    now = time.time()
    with lock:
        # পুরনো জব ডিলিট করা (১০ মিনিটের বেশি পুরনো)
        expired_jobs = [jid for jid, j in jobs.items() if now - j.get("created_at", now) > 600]
        for jid in expired_jobs:
            del jobs[jid]

        # পুরনো ক্যাশ ডিলিট করা
        expired_cache = [u for u, c in url_cache.items() if now - c["time"] > CACHE_EXPIRY]
        for u in expired_cache:
            del url_cache[u]

        if len(url_cache) > CACHE_LIMIT:
            oldest_key = min(url_cache.keys(), key=lambda k: url_cache[k]["time"])
            del url_cache[oldest_key]
    gc.collect()

# ==================== yt-dlp সিঙ্গেল এক্সট্রাক্টর ====================
def extract_single_video(video_url):
    now = time.time()
    # ১. আগে ক্যাশ চেক করবে (হাজার ইউজার একই লিংক দিলে ০.০১ সেকেন্ডে রেজাল্ট যাবে)
    with lock:
        if video_url in url_cache and (now - url_cache[video_url]["time"] < CACHE_EXPIRY):
            return url_cache[video_url]["data"]

    ydl_opts = {
        'format': 'best',
        'quiet': True,
        'no_warnings': True,
        'noplaylist': True,
        'cachedir': False,
        'socket_timeout': 15,
    }

    # যদি রিপোজিটরিতে cookies.txt ফাইল থাকে তবে অটোমেটিক ব্যবহার করবে
    if os.path.exists("cookies.txt"):
        ydl_opts['cookiefile'] = "cookies.txt"

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
            
            # আলাদা আলাদা কোয়ালিটি ও অডিও লিংক বের করা
            formats_list = []
            audio_url = ""
            best_url = info.get('url', '')

            for f in info.get('formats', []):
                f_url = f.get('url')
                if not f_url:
                    continue
                vcodec = f.get('vcodec', 'none')
                acodec = f.get('acodec', 'none')
                height = f.get('height')
                ext = f.get('ext', 'mp4')

                # অডিও স্ট্রিম সংরক্ষণ
                if vcodec == 'none' and acodec != 'none' and not audio_url:
                    audio_url = f_url

                # ভিডিও + অডিও একসাথে আছে এমন ফরম্যাট (প্লেয়ার ও ডাউনলোডের জন্য সেরা)
                if vcodec != 'none' and acodec != 'none' and height:
                    label = f"{height}p ({ext})"
                    formats_list.append({"label": label, "url": f_url})

            if not best_url and formats_list:
                best_url = formats_list[-1]["url"]

            data = {
                "success": True,
                "url_input": video_url,
                "title": info.get('title', 'Video'),
                "thumbnail": info.get('thumbnail', ''),
                "duration": info.get('duration', 0),
                "stream_url": best_url,
                "audio_url": audio_url if audio_url else best_url,
                "qualities": formats_list[-5:] # সেরা ৫টি কোয়ালিটি
            }

            with lock:
                url_cache[video_url] = {"data": data, "time": time.time()}
            return data

    except Exception as e:
        return {
            "success": False,
            "url_input": video_url,
            "error": str(e)
        }

# ==================== ব্যাচ প্রসেসর ওয়ার্কার ====================
def batch_worker():
    while True:
        batch_items = []
        # কিউ থেকে একসাথে সর্বোচ্চ ১০টি (BATCH_SIZE) কাজ তুলে নেবে
        first_item = task_queue.get()
        batch_items.append(first_item)

        while len(batch_items) < BATCH_SIZE and not task_queue.empty():
            try:
                batch_items.append(task_queue.get_nowait())
            except:
                break

        # ব্যাচের প্রতিটি আইটেম প্রসেস করা
        for job_id, urls in batch_items:
            with lock:
                if job_id in jobs:
                    jobs[job_id]["status"] = "processing"

            results = []
            for u in urls[:BATCH_SIZE]: # প্রতি রিকোয়েস্টে সর্বোচ্চ ১০টি লিংক
                res = extract_single_video(u.strip())
                results.append(res)

            with lock:
                if job_id in jobs:
                    jobs[job_id]["status"] = "completed"
                    jobs[job_id]["results"] = results
                    jobs[job_id]["event"].set()

            task_queue.task_done()

        # প্রতি ব্যাচ শেষ হওয়ার সাথে সাথে র‍্যাম পরিষ্কার করা
        clean_old_records()

# ব্যাকগ্রাউন্ড ওয়ার্কার চালু করা
for _ in range(MAX_WORKERS):
    t = threading.Thread(target=batch_worker, daemon=True)
    t.start()

# ==================== API Endpoints ====================
@app.route('/', methods=['GET'])
def home():
    return jsonify({
        "status": "online",
        "queue_size": task_queue.qsize(),
        "cached_videos": len(url_cache)
    })

@app.route('/extract', methods=['GET', 'POST'])
def submit_extraction():
    # সিঙ্গেল লিংক বা কমা দেওয়া একাধিক (ব্যাচ) লিংক গ্রহণ করবে
    raw_urls = request.args.get('url') or (request.json.get('url') if request.is_json else None)
    if not raw_urls:
        return jsonify({"success": False, "error": "URL দেওয়া হয়নি"}), 400

    # কমা (,) দিয়ে আলাদা করা থাকলে সর্বোচ্চ ১০টি লিংক এক ব্যাচে নেবে
    url_list = [u.strip() for u in raw_urls.split(',') if u.strip()][:BATCH_SIZE]

    job_id = str(uuid.uuid4())[:12]
    done_event = threading.Event()
    queue_pos = task_queue.qsize() + 1

    with lock:
        jobs[job_id] = {
            "status": "queued",
            "position": queue_pos,
            "results": [],
            "created_at": time.time(),
            "event": done_event
        }

    task_queue.put((job_id, url_list))

    # সার্ভার ফ্রি থাকলে ১০ সেকেন্ড অপেক্ষা করে সাথে সাথেই রেজাল্ট দিয়ে দেবে
    is_finished = done_event.wait(timeout=10.0)

    with lock:
        job_data = jobs.get(job_id)
        if is_finished and job_data and job_data["status"] == "completed":
            results = job_data["results"]
            del jobs[job_id]
            return jsonify({
                "status": "completed",
                "job_id": job_id,
                "is_batch": len(url_list) > 1,
                "data": results[0] if len(results) == 1 else results,
                "results": results
            })
        else:
            # সার্ভারে হাজার ইউজারের ভিড় থাকলে সাথে সাথে job_id ও সিরিয়াল নম্বর জানাবে
            return jsonify({
                "status": "queued",
                "job_id": job_id,
                "position": task_queue.qsize() + 1,
                "message": "আপনার রিকোয়েস্টটি ব্যাচ কিউতে প্রসেস হচ্ছে..."
            }), 202

@app.route('/status/<job_id>', methods=['GET'])
def check_status(job_id):
    with lock:
        job_data = jobs.get(job_id)
        if not job_data:
            return jsonify({"status": "error", "error": "Job ID পাওয়া যায়নি বা মেয়াদ শেষ"}), 404

        if job_data["status"] == "completed":
            results = job_data["results"]
            del jobs[job_id]
            return jsonify({
                "status": "completed",
                "job_id": job_id,
                "data": results[0] if len(results) == 1 else results,
                "results": results
            })
        else:
            return jsonify({
                "status": job_data["status"],
                "job_id": job_id,
                "position": task_queue.qsize() + 1
            })

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
