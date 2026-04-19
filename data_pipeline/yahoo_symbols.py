"""Shared Yahoo Finance symbol mapping helpers."""

from __future__ import annotations


YAHOO_SYMBOLS = {
    "AAPL": "AAPL",
    "AMZN": "AMZN",
    "ASC": "ASC.L",
    "BA.": "BA.L",
    "GOOG": "GOOG",
    "GSK": "GSK.L",
    "HLN": "HLN.L",
    "ISF": "ISF.L",
    "IUKD": "IUKD.L",
    "LLOY": "LLOY.L",
    "MSFT": "MSFT",
    "NWG": "NWG.L",
    "NVDA": "NVDA",
    "RGTI": "RGTI",
    "VOD": "VOD.L",
    "ARTEMIS GLOBAL INCOME": "0P0000W36K.L",
    "ARTEMIS HIGH INCOME": "0P0001GZXO.L",
    "FUNDSMITH EQUITY": "0P0000RU81.L",
    "RATHBONE GLOBAL OPPORTUNITIES": "0P0001FE43.L",
    "TROY TROJAN (CLASS X)": "0P0001CBJA.L",
}


def resolve_yahoo_symbol(symbol: object) -> str:
    normalized = str(symbol or "").strip().upper()
    return YAHOO_SYMBOLS.get(normalized, normalized)
