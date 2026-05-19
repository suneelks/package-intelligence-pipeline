"""Pure OSS-classification heuristic.

Given a NuGet package's declared license_expression and license_url, plus
an SPDX index, return a Classification: open_source / proprietary / unknown.

Kept deliberately decoupled from I/O so we can unit-test every path
without a database. The worker (`enrich.oss_status`) is responsible for
loading the SPDX index from `raw.spdx_licenses` and feeding it in.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlparse

Verdict = Literal["open_source", "proprietary", "unknown"]


@dataclass(frozen=True)
class SpdxLicense:
    license_id: str
    is_osi_approved: bool
    is_deprecated_id: bool = False


@dataclass(frozen=True)
class Classification:
    classification: Verdict
    spdx_id: str | None
    spdx_normalized: str | None
    is_osi_approved: bool | None
    reasoning: str


# ─── SPDX expression parsing ────────────────────────────────────────────────

# We don't implement the full SPDX expression grammar; v1 covers the 95%+
# real-world shapes: simple ids, AND/OR conjunctions, parenthesised groups,
# trailing `+` (any-later-version), and `WITH <exception>` clauses (we drop
# the exception and classify on the base license).
_TOKENIZER = re.compile(
    r"""
    \s*
    (?:
        (?P<paren>[()])
        | (?P<op>\bAND\b|\bOR\b|\bWITH\b)
        | (?P<id>[A-Za-z0-9.\-+]+)
    )
    \s*
    """,
    re.VERBOSE | re.IGNORECASE,
)


def _extract_license_terms(expression: str) -> list[str]:
    """Return the set of base license ids referenced in a SPDX expression.

    Drops parens/AND/OR. For `WITH`, keeps the base id (left side) and drops
    the exception (right side). Strips trailing `+`. Preserves source case
    so callers can do an exact lookup against the SPDX list."""
    if not expression:
        return []

    tokens: list[tuple[str, str]] = []
    pos = 0
    while pos < len(expression):
        m = _TOKENIZER.match(expression, pos)
        if not m:
            return []  # malformed — caller treats as unknown
        if m.end() == pos:
            return []
        pos = m.end()
        if m.group("paren"):
            tokens.append(("paren", m.group("paren")))
        elif m.group("op"):
            tokens.append(("op", m.group("op").upper()))
        elif m.group("id"):
            tokens.append(("id", m.group("id")))

    terms: list[str] = []
    skip_next = False
    for kind, value in tokens:
        if skip_next:
            if kind == "id":
                skip_next = False
            continue
        if kind == "id":
            terms.append(value.rstrip("+"))
        elif kind == "op" and value == "WITH":
            # Drop the next id (exception name)
            skip_next = True
    return terms


def _normalize_expression(expression: str) -> str:
    return " ".join(expression.split())


# ─── License URL recognition ────────────────────────────────────────────────

# Hardcoded patterns for URLs that are *definitively* proprietary and would
# never appear in SPDX `seeAlso`. SPDX-derived URLs come from the dynamic
# index passed in.
_PROPRIETARY_URL_PATTERNS: tuple[re.Pattern[str], ...] = (
    # Microsoft EULA fwlinks (the most common proprietary URL on NuGet).
    re.compile(r"^https?://go\.microsoft\.com/fwlink", re.IGNORECASE),
    # Microsoft "license URL is deprecated, see licenseExpression" stub.
    re.compile(r"^https?://aka\.ms/(deprecateLicenseUrl|pexunj)", re.IGNORECASE),
)


def _normalize_url(url: str) -> str:
    """Canonical form for URL→license matching.

    Lowercases scheme + host, strips `www.`, strips trailing slash. Keeps
    path case (some license URLs are case-sensitive on the path component).
    """
    try:
        parsed = urlparse(url.strip())
    except ValueError:
        return ""
    if not parsed.scheme or not parsed.netloc:
        return ""
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    path = parsed.path.rstrip("/")
    return f"{host}{path}"


def build_url_index(spdx_licenses: list[SpdxLicense], see_also_urls: dict[str, list[str]]) -> dict[str, str]:
    """Build {normalized_url: license_id} from SPDX `seeAlso` URLs.

    The classifier worker calls this once at startup and passes the result
    in. Kept as a separate function so tests can build small fixtures
    without going through SQL.
    """
    index: dict[str, str] = {}
    for lic in spdx_licenses:
        for url in see_also_urls.get(lic.license_id, []):
            normalized = _normalize_url(url)
            if normalized and normalized not in index:
                index[normalized] = lic.license_id
    return index


# ─── Classification ─────────────────────────────────────────────────────────


def classify(
    license_expression: str | None,
    license_url: str | None,
    spdx_dict: dict[str, SpdxLicense],
    url_index: dict[str, str],
) -> Classification:
    """Classify a single (license_expression, license_url) pair.

    Order of preference:
      1. license_expression — strongest signal (declared SPDX).
      2. license_url        — fallback when expression is missing.
      3. neither            — unknown.
    """
    expr = license_expression.strip() if license_expression else ""
    url = license_url.strip() if license_url else ""

    if expr:
        normalized = _normalize_expression(expr)
        terms = _extract_license_terms(expr)
        if not terms:
            return Classification(
                "unknown", None, normalized, None,
                f"could not parse license expression: {expr!r}",
            )

        unrecognized = [t for t in terms if t not in spdx_dict]
        if unrecognized:
            # `LicenseRef-*` is the SPDX convention for custom non-SPDX
            # licenses — by far the most common form is proprietary.
            license_refs = [t for t in unrecognized if t.lower().startswith("licenseref")]
            if license_refs and len(license_refs) == len(unrecognized):
                return Classification(
                    "proprietary", license_refs[0], normalized, False,
                    f"custom license reference: {license_refs[0]}",
                )
            return Classification(
                "unknown", None, normalized, None,
                f"unrecognized SPDX license id: {unrecognized[0]}",
            )

        # All terms recognized. Open-source if ANY term is OSI-approved
        # (a disjunction lets the consumer pick any term; for `MIT AND
        # Apache-2.0` both are OSI so the result is still OSS).
        osi_terms = [t for t in terms if spdx_dict[t].is_osi_approved]
        primary = osi_terms[0] if osi_terms else terms[0]
        if osi_terms:
            return Classification(
                "open_source", primary, normalized, True,
                f"{primary} is OSI-approved",
            )
        return Classification(
            "proprietary", primary, normalized, False,
            f"declared license {primary} is not OSI-approved",
        )

    if url:
        normalized = _normalize_url(url)
        if normalized in url_index:
            spdx_id = url_index[normalized]
            spdx = spdx_dict[spdx_id]
            verdict: Verdict = "open_source" if spdx.is_osi_approved else "proprietary"
            return Classification(
                verdict, spdx_id, None, spdx.is_osi_approved,
                f"license URL maps to {spdx_id}",
            )
        for pattern in _PROPRIETARY_URL_PATTERNS:
            if pattern.search(url):
                return Classification(
                    "proprietary", None, None, False,
                    "license URL matches a known proprietary EULA pattern",
                )
        return Classification(
            "unknown", None, None, None,
            "license URL not recognized",
        )

    return Classification(
        "unknown", None, None, None,
        "no license declared",
    )
