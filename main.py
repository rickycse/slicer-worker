#!/usr/bin/env python3
import json, os, re, shutil, subprocess, tempfile, time
from urllib.parse import urlparse
import boto3
from botocore.config import Config
from dotenv import load_dotenv
import os, tempfile

load_dotenv()

HOME_DIR = os.path.expanduser("~")
HOME_SLICE = os.path.join(HOME_DIR, ".slice")
os.makedirs(HOME_SLICE, exist_ok=True)

BASE_FLATPAK = ["flatpak", "run", "--filesystem=home"]

QUEUE_URL = os.environ.get("QUEUE_URL")
RESULTS_QUEUE_URL = os.environ.get("RESULTS_QUEUE_URL")
PRUSA = ["flatpak", "run", "--filesystem=host", "com.prusa3d.PrusaSlicer"]
PRICE_PER_KG = float(os.environ.get("PRICE_PER_KG", "25.0"))

my_config = Config(
    region_name="us-east-2",
    signature_version="v4",
    retries={"max_attempts": 10, "mode": "standard"},
)

s3 = boto3.client("s3", config=my_config)
sqs = boto3.client("sqs", config=my_config)

def s3_download(s3_uri, dest):
    u = urlparse(s3_uri)
    s3.download_file(u.netloc, u.path.lstrip("/"), dest)

def s3_upload(src, s3_uri):
    u = urlparse(s3_uri)
    s3.upload_file(src, u.netloc, u.path.lstrip("/"))

FILAMENT_G_RE = re.compile(r"^;\s*filament used \[g\]\s*=\s*(.+)$", re.IGNORECASE)
FILAMENT_COST_RE = re.compile(r"^;\s*filament cost\s*=\s*([\d.]+)", re.IGNORECASE)
PRINT_TIME_RE = re.compile(r"^;\s*estimated printing time.*=\s*(.+)$", re.IGNORECASE)

def parse_gcode_summary(gcode_path):
    """
    Returns dict:
      {
        "filament_grams": float or None,
        "filament_cost": float or None,
        "estimated_time": str or None
      }
    For multi-extruder values (comma-separated), grams are summed.
    """
    grams = None
    cost = None
    est_time = None

    with open(gcode_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            m = FILAMENT_G_RE.match(line)
            if m:
                # Could be "12.3" or "12.3, 4.5"
                parts = [p.strip() for p in m.group(1).split(",")]
                try:
                    grams = sum(float(x) for x in parts if x)
                except ValueError:
                    pass

            m = FILAMENT_COST_RE.match(line)
            if m:
                try:
                    cost = float(m.group(1))
                except ValueError:
                    pass

            m = PRINT_TIME_RE.match(line)
            if m:
                est_time = m.group(1).strip()

    return {"filament_grams": grams, "filament_cost": cost, "estimated_time": est_time}

def compute_cost_if_missing(summary):
    """
    If filament_cost is missing but we have grams, compute:
        cost = (grams / 1000) * PRICE_PER_KG
    """
    if summary.get("filament_cost") is None and summary.get("filament_grams") is not None:
        summary["filament_cost"] = round((summary["filament_grams"] / 1000.0) * PRICE_PER_KG, 2)
    return summary

def ensure_exists(path, label):
            if not os.path.isfile(path):
                raise FileNotFoundError(f"{label} not found: {path}")

def slice_once(msg_body):
    payload = json.loads(msg_body)
    input_stl = payload["input_stl"]
    config_ini = payload.get("config_ini")
    output_gcode = payload["output_gcode"]
    job_id = payload.get("job_id")

    workdir = tempfile.mkdtemp(prefix="slice-", dir=HOME_SLICE)
    try:
        local_stl   = os.path.join(workdir, "model.stl")
        local_ini   = os.path.join(workdir, "config.ini")
        local_gcode = os.path.join(workdir, "out.gcode")

        print(f"Downloading {input_stl}...")
        s3_download(input_stl, local_stl)

        print(f"Downloading {config_ini}...")
        s3_download(config_ini, local_ini)

        print("\n=== Dumping config.ini for debug ===")
        with open(local_ini) as f:
            print(f.read())
        print("=== End of config.ini ===\n")

        ensure_exists(local_stl, "Input STL")
        ensure_exists(local_ini, "Config INI")

        # print(f"[WHOAMI] {os.popen('whoami').read().strip()}  [CWD] {os.getcwd()}")
        # print(f"[JOB] input_stl={input_stl}")
        # print(f"[JOB] config_ini={config_ini}")
        # print(f"[JOB] output_gcode={output_gcode}")

        check_cmd = [
            "flatpak", "run", "--filesystem=home", "--command=bash", "com.prusa3d.PrusaSlicer",
            "-lc", f'test -f "{local_ini}" && echo INI_OK; test -f "{local_stl}" && echo STL_OK; ls -l "{os.path.dirname(local_stl)}"'
        ]
        subprocess.check_call(check_cmd)

        cmd = [
            "flatpak", "run", "--filesystem=home", "com.prusa3d.PrusaSlicer",
            local_stl, "--load", local_ini, "--export-gcode", "--output", local_gcode
        ]
        print("Running:", " ".join(cmd), f"(cwd={workdir})")
        subprocess.check_call(cmd)

        ensure_exists(local_gcode, "Output G-code")

        print(f"Uploading {output_gcode}")
        s3_upload(local_gcode, output_gcode)
        summary = parse_gcode_summary(local_gcode)
        summary = compute_cost_if_missing(summary)

        result_msg = {
            "job_id": job_id,
            "input_stl": input_stl,
            "config_ini": config_ini,
            "output_gcode": output_gcode,
            "filament_grams": summary.get("filament_grams"),
            "filament_cost": summary.get("filament_cost"),
            "estimated_time": summary.get("estimated_time"),
            "price_per_kg_used": PRICE_PER_KG if summary.get("filament_cost") is not None else None,
            "timestamp": int(time.time()),
            "status": "OK",
        }

        if RESULTS_QUEUE_URL:
            sqs.send_message(
                QueueUrl=RESULTS_QUEUE_URL,
                MessageBody=json.dumps(result_msg),
            )
            print("Result sent to RESULTS_QUEUE_URL:", result_msg)
        else:
            print("Result (no RESULTS_QUEUE_URL set):", result_msg)

    finally:
        shutil.rmtree(workdir, ignore_errors=True)

def main():
    if not QUEUE_URL:
        raise RuntimeError("QUEUE_URL is not set")

    while True:
        print("Worker running...")
        resp = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20,
            VisibilityTimeout=900,
        )
        msgs = resp.get("Messages", [])
        if not msgs:
            continue

        m = msgs[0]
        rcpt = m["ReceiptHandle"]

        try:
            slice_once(m["Body"])
        except subprocess.CalledProcessError as e:
            print("Slicing failed:", e)
            sqs.change_message_visibility(
                QueueUrl=QUEUE_URL, ReceiptHandle=rcpt, VisibilityTimeout=120
            )
            time.sleep(5)
            continue
        except Exception as e:
            print("Worker error:", e)
            sqs.change_message_visibility(
                QueueUrl=QUEUE_URL, ReceiptHandle=rcpt, VisibilityTimeout=120
            )
            time.sleep(5)
            continue

        # success -> delete
        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=rcpt)

if __name__ == "__main__":
    main()