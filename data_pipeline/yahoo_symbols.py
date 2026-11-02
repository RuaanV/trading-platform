"""Shared Yahoo Finance symbol mapping helpers."""

from __future__ import annotations

import re


YAHOO_SYMBOLS = {
    "AAPL": "AAPL",
    "AMZN": "AMZN",
    "AZN": "AZN.L",
    "ASC": "ASC.L",
    "BA.": "BA.L",
    "GOOG": "GOOG",
    "GSK": "GSK.L",
    "HLN": "HLN.L",
    "IITU": "IITU.L",
    "ISF": "ISF.L",
    "IUKD": "IUKD.L",
    "LLOY": "LLOY.L",
    "MSFT": "MSFT",
    "NWG": "NWG.L",
    "NVDA": "NVDA",
    "RGTI": "RGTI",
    "SNOW": "SNOW",
    "VOD": "VOD.L",
    "ARTEMIS GLOBAL INCOME": "0P0000W36K.L",
    "ARTEMIS HIGH INCOME": "0P0001GZXO.L",
    "FUNDSMITH EQUITY": "0P0000RU81.L",
    "RATHBONE GLOBAL OPPORTUNITIES": "0P0001FE43.L",
    "TROY TROJAN (CLASS X)": "0P0001CBJA.L",
    # Hargreaves Lansdown exports identify funds by SEDOL. Each mapping below was
    # checked against the HL export price (29 Oct 2026).
    "B41YBW7": "0P0000RU81.L",  # Fundsmith Equity I Acc
    "BH0P2M9": "0P0001FE43.L",  # Rathbone Global Opportunities S Acc
    "B5ZX1M7": "0P0000W36K.L",  # Artemis Global Income I Acc
    "BJMD6R8": "0P0001GZXO.L",  # Artemis High Income I Acc
    "BZ6CNS3": "0P0001CBJA.L",  # Troy Trojan X Acc
}

_SEDOL_PATTERN = re.compile(r"^[B-DF-HJ-NP-TV-Z0-9]{6}[0-9]$")


def is_sedol(code: object) -> bool:
    """True for 7-character SEDOL codes, which HL uses to identify funds."""
    return bool(_SEDOL_PATTERN.match(str(code or "").strip().upper()))


def resolve_yahoo_symbol(symbol: object) -> str:
    normalized = str(symbol or "").strip().upper()
    return YAHOO_SYMBOLS.get(normalized, normalized)
