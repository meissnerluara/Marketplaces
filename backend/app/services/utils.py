import os
import json

def load_tokens_from_env(plataforma: str):
    env_var = f"{plataforma.upper()}_TOKENS"
    tokens_json = os.getenv(env_var)
    if not tokens_json:
        return {}
    try:
        data = json.loads(tokens_json)
        if not isinstance(data, dict):
            return {}
        return data
    except Exception:
        return {}