#!/usr/bin/env python3
"""
Pushover notification helper — WSB Signal Lab

Uses stdlib only (urllib). Reads credentials from the environment:
  PUSHOVER_USER_KEY   — your Pushover user key
  PUSHOVER_API_TOKEN  — your application API token

Credentials are loaded from .env by the calling script (paper_trader.py,
check_positions.py) before this module's functions are invoked.

Usage:
  from scrapers.notify import send_pushover
  send_pushover("Trade placed")

  # Send a test notification (loads .env itself):
  python3 scrapers/notify.py
"""

import json
import os
import urllib.parse
import urllib.request

_PUSHOVER_URL = 'https://api.pushover.net/1/messages.json'


def send_pushover(message: str, title: str = 'WSB Signal Lab') -> bool:
    """POST a Pushover notification. Returns True on success, False on any failure."""
    user_key  = os.getenv('PUSHOVER_USER_KEY')
    api_token = os.getenv('PUSHOVER_API_TOKEN')
    if not user_key or not api_token:
        return False
    try:
        data = urllib.parse.urlencode({
            'token':   api_token,
            'user':    user_key,
            'title':   title,
            'message': message,
        }).encode()
        req = urllib.request.Request(_PUSHOVER_URL, data=data)
        with urllib.request.urlopen(req, timeout=8) as resp:
            body = json.loads(resp.read())
            return body.get('status') == 1
    except Exception:
        return False


if __name__ == '__main__':
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

    ok = send_pushover(
        'Test notification from WSB Signal Lab — notify.py is wired up correctly.',
        title='WSB Lab Test',
    )
    if ok:
        print('OK  Test notification sent.')
    else:
        print('FAIL  Nothing sent — check PUSHOVER_USER_KEY and PUSHOVER_API_TOKEN in .env')
        sys.exit(1)
