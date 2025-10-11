#!/usr/bin/env python3
import json, os, shutil, subprocess, tempfile, time
import boto3
from urllib.parse import urlparse

QUEUE_URL = os.environ.get("QUEUE_URL")
PRUSA = ["flatpak", "run", "com.prusa3d.PrusaSlicer"]

s3 = boto3.client("s3")
sqs = boto3.client("sqs")

def s3_download(s3_uri, dest):
    u = urlparse(s3_uri); s3.download_file(u.netloc, u.path.lstrip("/"), dest)

def s3_upload(src, s3_uri):
    u = urlparse(s3_uri); s3.upload_file(src, u.netloc, u.path.lstrip("/"))

def slice_once(msg_body):
    payload = json.loads(msg_body)
    workdir = tempfile.mkdtemp(prefix="slice-")
    try:
        local_stl = os.path.join(workdir, "model.stl")
        local_ini = os.path.join(workdir, "config.ini")
        local_gcode = os.path.join(workdir, "out.gcode")

        s3_download(payload["input_stl"], local_stl)
        s3_download(payload["config_ini"], local_ini)

        cmd = PRUSA + [local_stl, "--load", local_ini, "--export-gcode", "--output", local_gcode]
        print("Running:", " ".join(cmd))
        subprocess.check_call(cmd)

        s3_upload(local_gcode, payload["output_gcode"])
        print("Uploaded:", payload["output_gcode"])
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

def main():
    while True:
        resp = sqs.receive_message(QueueUrl=QUEUE_URL, MaxNumberOfMessages=1,
                                   WaitTimeSeconds=20, VisibilityTimeout=900)
        msgs = resp.get("Messages", [])
        if not msgs: continue
        m = msgs[0]; rcpt = m["ReceiptHandle"]
        try:
            slice_once(m["Body"])
        except subprocess.CalledProcessError as e:
            print("Slicing failed:", e)
            sqs.change_message_visibility(QueueUrl=QUEUE_URL, ReceiptHandle=rcpt, VisibilityTimeout=120)
            time.sleep(5)
            continue
        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=rcpt)

if __name__ == "__main__":
    main()
