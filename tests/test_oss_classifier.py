"""Pure-function tests for the OSS classifier.

No DB, no I/O — every test builds a fixture SPDX dict + URL index
inline and asserts on the Classification verdict, spdx_id, and reasoning.
The integration test (`test_oss_status_sync.py`) covers the worker glue.
"""

from __future__ import annotations

import pytest

from nuget_pipeline.enrich.classifier import (
    SpdxLicense,
    build_url_index,
    classify,
)


@pytest.fixture
def spdx_dict() -> dict[str, SpdxLicense]:
    return {
        "MIT": SpdxLicense("MIT", is_osi_approved=True),
        "Apache-2.0": SpdxLicense("Apache-2.0", is_osi_approved=True),
        "BSD-3-Clause": SpdxLicense("BSD-3-Clause", is_osi_approved=True),
        "GPL-3.0-only": SpdxLicense("GPL-3.0-only", is_osi_approved=True),
        # An SPDX id that exists but is *not* OSI-approved (rare but real).
        "CC-BY-NC-4.0": SpdxLicense("CC-BY-NC-4.0", is_osi_approved=False),
    }


@pytest.fixture
def url_index(spdx_dict) -> dict[str, str]:
    see_also = {
        "MIT": ["https://opensource.org/licenses/MIT"],
        "Apache-2.0": [
            "https://www.apache.org/licenses/LICENSE-2.0",
            "https://opensource.org/licenses/Apache-2.0",
        ],
        "BSD-3-Clause": ["https://opensource.org/licenses/BSD-3-Clause"],
    }
    return build_url_index(list(spdx_dict.values()), see_also)


# ─── Expression path ────────────────────────────────────────────────────────


def test_simple_osi_license_is_open_source(spdx_dict, url_index) -> None:
    result = classify("MIT", None, spdx_dict, url_index)
    assert result.classification == "open_source"
    assert result.spdx_id == "MIT"
    assert result.is_osi_approved is True
    assert "MIT" in result.reasoning


def test_disjunction_with_osi_term_is_open_source(spdx_dict, url_index) -> None:
    """`MIT OR Apache-2.0` — a consumer can pick either; both are OSI."""
    result = classify("MIT OR Apache-2.0", None, spdx_dict, url_index)
    assert result.classification == "open_source"
    assert result.is_osi_approved is True


def test_conjunction_of_osi_terms_is_open_source(spdx_dict, url_index) -> None:
    """`MIT AND Apache-2.0` — every term is OSI, so the conjunction is OSS."""
    result = classify("MIT AND Apache-2.0", None, spdx_dict, url_index)
    assert result.classification == "open_source"


def test_conjunction_with_non_osi_term_is_proprietary(spdx_dict, url_index) -> None:
    """`MIT AND CC-BY-NC-4.0` — AND binds the consumer to *both* licenses,
    so one OSI-approved term cannot carry the verdict. The v1 any-OSI rule
    classified this open_source; that was a bug."""
    result = classify("MIT AND CC-BY-NC-4.0", None, spdx_dict, url_index)
    assert result.classification == "proprietary"
    assert "CC-BY-NC-4.0" in result.reasoning


def test_recognized_but_non_osi_is_proprietary(spdx_dict, url_index) -> None:
    """`CC-BY-NC-4.0` exists in SPDX but is not OSI-approved."""
    result = classify("CC-BY-NC-4.0", None, spdx_dict, url_index)
    assert result.classification == "proprietary"
    assert result.is_osi_approved is False


def test_license_ref_is_proprietary(spdx_dict, url_index) -> None:
    """SPDX `LicenseRef-*` is the convention for custom non-SPDX licenses;
    overwhelmingly proprietary in practice."""
    result = classify("LicenseRef-MyCorp-Proprietary", None, spdx_dict, url_index)
    assert result.classification == "proprietary"
    assert "custom license reference" in result.reasoning


def test_unrecognized_spdx_term_is_unknown(spdx_dict, url_index) -> None:
    result = classify("MS-EULA-NonCommercial", None, spdx_dict, url_index)
    assert result.classification == "unknown"
    assert "MS-EULA-NonCommercial" in result.reasoning


def test_with_clause_drops_exception(spdx_dict, url_index) -> None:
    """`GPL-3.0-only WITH Classpath-exception-2.0` should classify on
    the base license, not the (unrecognized) exception name."""
    result = classify(
        "GPL-3.0-only WITH Classpath-exception-2.0", None, spdx_dict, url_index
    )
    assert result.classification == "open_source"
    assert result.spdx_id == "GPL-3.0-only"


def test_trailing_plus_is_stripped(spdx_dict, url_index) -> None:
    """`Apache-2.0+` (any-later-version) — the `+` suffix is dropped."""
    result = classify("Apache-2.0+", None, spdx_dict, url_index)
    assert result.classification == "open_source"
    assert result.spdx_id == "Apache-2.0"


def test_parenthesised_expression(spdx_dict, url_index) -> None:
    result = classify(
        "(MIT OR Apache-2.0) AND BSD-3-Clause", None, spdx_dict, url_index
    )
    assert result.classification == "open_source"


def test_mixed_expression_deferral_is_pinned(spdx_dict, url_index) -> None:
    """Expressions mixing AND and OR lose their grouping in tokenization,
    so they are *not* evaluated as boolean logic — they fall back to the
    any-OSI rule. Under proper evaluation this example would be
    proprietary (the AND side is non-OSI); until that lands, it
    classifies open_source. This test pins the deferral: if it fails,
    mixed-expression evaluation has changed — update the docs and this
    test together."""
    result = classify(
        "(MIT OR Apache-2.0) AND CC-BY-NC-4.0", None, spdx_dict, url_index
    )
    assert result.classification == "open_source"


# ─── URL path ───────────────────────────────────────────────────────────────


def test_known_oss_url_classifies_as_open_source(spdx_dict, url_index) -> None:
    result = classify(None, "https://opensource.org/licenses/MIT", spdx_dict, url_index)
    assert result.classification == "open_source"
    assert result.spdx_id == "MIT"


def test_url_normalization_strips_www(spdx_dict, url_index) -> None:
    """SPDX lists `apache.org` without `www.` for some entries; matcher
    must treat `www.` as equivalent so packages linking to either form
    classify the same."""
    result = classify(
        None, "https://www.apache.org/licenses/LICENSE-2.0", spdx_dict, url_index
    )
    assert result.classification == "open_source"
    assert result.spdx_id == "Apache-2.0"


def test_microsoft_eula_fwlink_is_proprietary(spdx_dict, url_index) -> None:
    result = classify(
        None,
        "https://go.microsoft.com/fwlink/?LinkId=329770",
        spdx_dict,
        url_index,
    )
    assert result.classification == "proprietary"
    assert "proprietary" in result.reasoning.lower()


def test_unrecognized_url_is_unknown(spdx_dict, url_index) -> None:
    result = classify(
        None, "https://example.com/some/license.txt", spdx_dict, url_index
    )
    assert result.classification == "unknown"


def test_github_license_url_is_unknown_in_v1(spdx_dict, url_index) -> None:
    """A LICENSE file inside a GitHub repo is ambiguous without fetching
    the file. v1 leaves it unknown — v2 will probe."""
    result = classify(
        None,
        "https://github.com/foo/bar/blob/main/LICENSE",
        spdx_dict,
        url_index,
    )
    assert result.classification == "unknown"


# ─── Empty / fallback ───────────────────────────────────────────────────────


def test_no_signals_is_unknown(spdx_dict, url_index) -> None:
    result = classify(None, None, spdx_dict, url_index)
    assert result.classification == "unknown"
    assert result.reasoning == "no license declared"


def test_empty_string_treated_as_missing(spdx_dict, url_index) -> None:
    result = classify("   ", "   ", spdx_dict, url_index)
    assert result.classification == "unknown"


def test_expression_takes_precedence_over_url(spdx_dict, url_index) -> None:
    """Even a known proprietary URL is overridden by a clean SPDX expression."""
    result = classify(
        "MIT",
        "https://go.microsoft.com/fwlink/?LinkId=329770",
        spdx_dict,
        url_index,
    )
    assert result.classification == "open_source"
    assert result.spdx_id == "MIT"


def test_malformed_expression_is_unknown(spdx_dict, url_index) -> None:
    """An expression we can't tokenize at all — leave as unknown rather
    than guess. (Picking `~~~` because non-ASCII garbage might still
    tokenize; this won't.)"""
    result = classify("???$$$", None, spdx_dict, url_index)
    assert result.classification == "unknown"
