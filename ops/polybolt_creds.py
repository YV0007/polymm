"""Create Polymarket CLOB API credentials for a throwaway, data-only wallet and put them on the VPS.

Run it yourself, on the laptop (Austria is not geoblocked; the German VPS is):
    uv run --with eth-account --with httpx python -I ops/polybolt_creds.py

What it does, and why each step is this way:
1. Generates a fresh private key that will never hold funds. Why: the PolyBolt reference-price
   channels only need *some* CLOB credentials; a key with nothing on it has nothing to steal,
   and the account that may one day trade never touches a server.
2. Saves the private key in the macOS login Keychain (service "polymm-polymarket-data-wallet").
   It is never printed. Why: you can re-derive the same credentials later from it.
3. Signs Polymarket's L1 auth message (EIP-712 "ClobAuth" on chain 137) and calls
   POST /auth/api-key, falling back to GET /auth/derive-api-key if one already exists.
4. Pipes apiKey/secret/passphrase over ssh straight into /home/polymm/.config/polymm/polybolt.env
   (mode 600, owner polymm). Nothing secret is printed or written on the laptop.
Only the wallet address and the first 8 characters of the API key are printed.
"""
from __future__ import annotations

import subprocess
import sys
import time

import httpx
from eth_account import Account
from eth_account.messages import encode_typed_data

HOST = "https://clob.polymarket.com"
SERVICE, ACCOUNT_NAME = "polymm-polymarket-data-wallet", "polymm"
VPS, VPS_FILE = "polymm", "/home/polymm/.config/polymm/polybolt.env"
MSG = "This message attests that I control the given wallet"


def l1_headers(acct, nonce: int = 0) -> dict:
    ts = str(int(time.time()))
    typed = {
        "types": {
            "EIP712Domain": [{"name": "name", "type": "string"}, {"name": "version", "type": "string"},
                             {"name": "chainId", "type": "uint256"}],
            "ClobAuth": [{"name": "address", "type": "address"}, {"name": "timestamp", "type": "string"},
                         {"name": "nonce", "type": "uint256"}, {"name": "message", "type": "string"}],
        },
        "primaryType": "ClobAuth",
        "domain": {"name": "ClobAuthDomain", "version": "1", "chainId": 137},
        "message": {"address": acct.address, "timestamp": ts, "nonce": nonce, "message": MSG},
    }
    sig = acct.sign_message(encode_typed_data(full_message=typed)).signature.hex()
    return {"POLY_ADDRESS": acct.address, "POLY_SIGNATURE": sig if sig.startswith("0x") else "0x" + sig,
            "POLY_TIMESTAMP": ts, "POLY_NONCE": str(nonce)}


def keychain_get() -> str | None:
    r = subprocess.run(["security", "find-generic-password", "-s", SERVICE, "-a", ACCOUNT_NAME, "-w"],
                       capture_output=True, text=True)
    return r.stdout.strip() or None if r.returncode == 0 else None


def main() -> None:
    key = keychain_get()
    if key is None:
        key = Account.create().key.hex()
        # Why -U and argv: the login keychain is local to this Mac; -U updates instead of duplicating
        subprocess.run(["security", "add-generic-password", "-s", SERVICE, "-a", ACCOUNT_NAME, "-w", key, "-U"],
                       check=True)
        print("new data-only wallet created; private key saved in Keychain (Keychain Access →", SERVICE + ")")
    else:
        print("reusing the data-only wallet already in Keychain")
    acct = Account.from_key(key)
    print("wallet address:", acct.address, "(never send funds here)")

    with httpx.Client(base_url=HOST, timeout=20) as c:
        r = c.post("/auth/api-key", headers=l1_headers(acct))
        if r.status_code != 200 or "apiKey" not in r.text:
            print(f"create returned {r.status_code}; trying derive (an existing key for this wallet)")
            r = c.get("/auth/derive-api-key", headers=l1_headers(acct))
    if r.status_code != 200:
        sys.exit(f"Polymarket refused: HTTP {r.status_code} {r.text[:300]}")
    cr = r.json()
    if not all(k in cr for k in ("apiKey", "secret", "passphrase")):
        sys.exit(f"unexpected response fields: {sorted(cr)}")

    body = (f"POLY_DATA_WALLET={acct.address}\nPOLY_API_KEY={cr['apiKey']}\n"
            f"POLY_API_SECRET={cr['secret']}\nPOLY_API_PASSPHRASE={cr['passphrase']}\n")
    remote = (f"umask 077 && install -d -m 700 -o polymm -g polymm /home/polymm/.config/polymm "
              f"&& cat > {VPS_FILE} && chown polymm:polymm {VPS_FILE} && chmod 600 {VPS_FILE} && echo saved on VPS")
    subprocess.run(["ssh", VPS, remote], input=body, text=True, check=True)
    print("API key starts with", cr["apiKey"][:8] + "…", "(secret and passphrase not shown)")


if __name__ == "__main__":
    main()
