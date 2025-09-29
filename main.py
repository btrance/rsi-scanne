import os, subprocess
from flask import Flask, Response

app = Flask(__name__)

def run_scan(force_hit=False):
    cmd = [
        "python", "rsi_scanner_binance_1h_v4_1.py",
        "--min-volume", os.getenv("MIN_VOLUME", "5000000"),
        "--quotes", os.getenv("QUOTES", "USDT,FDUSD"),
        "--timeout", os.getenv("TIMEOUT", "30"),
    ]
    if os.getenv("SKIP_MCAP", "true").lower() == "true":
        cmd.append("--skip-mcap")
    if force_hit:
        cmd += ["--overbought", "0"]  # 테스트용: 무조건 과매수 신호 발생

    print("RUN:", " ".join(cmd), flush=True)
    res = subprocess.run(cmd, capture_output=True, text=True)
    print(res.stdout)
    print(res.stderr)
    return res

@app.get("/")
def root():
    return "OK", 200

@app.get("/run")
def run():
    r = run_scan(force_hit=False)
    status = 200 if r.returncode == 0 else 500
    # 마지막 4000자만 응답으로 보여줌(로그는 Cloud Run에서 전체 확인 가능)
    return Response(r.stdout[-4000:], status=status, mimetype="text/plain")

@app.get("/test")
def test():
    r = run_scan(force_hit=True)
    status = 200 if r.returncode == 0 else 500
    return Response(r.stdout[-4000:], status=status, mimetype="text/plain")
