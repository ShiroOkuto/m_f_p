import re
import time
import hmac
import hashlib
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

def compute_dlhd_key_headers(key_url: str, secret_key: str, user_agent: str = None) -> Optional[dict]:
    """
    Compute X-Key-Timestamp, X-Key-Nonce, X-Fingerprint, and X-Key-Path for a /key/ URL.
    Algorithm ported from EasyProxy.
    """
    # Extract resource and number from URL pattern /key/{resource}/{number}
    pattern = r"/key/([^/]+)/(\d+)"
    match = re.search(pattern, key_url)

    if not match:
        # Compatibility check for newer URLs that might not have /key/ explicitly
        if '/key' not in key_url:
            return None
        # Try to find something that looks like resource/number
        fallback_pattern = r"/([a-zA-Z0-9]+)/(\d+)(?:\.css|\?|$)"
        match = re.search(fallback_pattern, key_url)
        if not match:
            return None

    resource = match.group(1)
    number = match.group(2)

    ts = int(time.time())

    # Compute HMAC-SHA256
    try:
        hmac_hash = hmac.new(
            secret_key.encode("utf-8"), resource.encode("utf-8"), hashlib.sha256
        ).hexdigest()

        # Proof-of-work loop
        nonce = 0
        for i in range(100000):
            combined = f"{hmac_hash}{resource}{number}{ts}{i}"
            md5_hash = hashlib.md5(combined.encode("utf-8")).hexdigest()
            prefix_value = int(md5_hash[:4], 16)

            if prefix_value < 0x1000:  # < 4096
                nonce = i
                break

        # Compute fingerprint
        fp_user_agent = user_agent if user_agent else "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
        fp_screen_res = "1920x1080"
        fp_timezone = "UTC"
        fp_language = "en"

        fp_string = f"{fp_user_agent}{fp_screen_res}{fp_timezone}{fp_language}"
        fingerprint = hashlib.sha256(fp_string.encode("utf-8")).hexdigest()[:16]

        # Compute key-path
        key_path_string = f"{resource}|{number}|{ts}|{fingerprint}"
        key_path = hmac.new(
            secret_key.encode("utf-8"), key_path_string.encode("utf-8"), hashlib.sha256
        ).hexdigest()[:16]

        logger.debug(f"Computed DLHD key headers: ts={ts}, nonce={nonce}, fingerprint={fingerprint}, key_path={key_path}")
        
        return {
            'X-Key-Timestamp': str(ts),
            'X-Key-Nonce': str(nonce),
            'X-Fingerprint': fingerprint,
            'X-Key-Path': key_path
        }
    except Exception as e:
        logger.error(f"Error computing DLHD key headers: {e}")
        return None
