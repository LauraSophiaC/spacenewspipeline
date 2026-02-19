import json, sys, datetime

def log(level: str, msg: str, **kw):
    payload = {
        "ts": datetime.datetime.utcnow().isoformat() + "Z",
        "level": level,
        "msg": msg,
        **kw
    }
    sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    sys.stdout.flush()
