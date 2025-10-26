#!/usr/bin/env python3
import json
import re
import warnings
from typing import Dict, Any, List

from acl_anthology import Anthology
from acl_anthology.anthology import SchemaMismatchWarning
from py4j.java_gateway import JavaGateway, GatewayParameters, CallbackServerParameters

# Suppress warnings globally
warnings.filterwarnings("ignore", category=SchemaMismatchWarning)

# Global cache for the Anthology instance
_anthology_cache = None


def get_anthology():
    """Get or create cached Anthology instance."""
    global _anthology_cache
    if _anthology_cache is None:
        print("[Python] Loading ACL Anthology from repository (this may take a while)...")
        _anthology_cache = Anthology.from_repo()
        print("[Python] ACL Anthology loaded successfully!")
    return _anthology_cache


# Helper to convert non-JSON-serializable objects (e.g., MarkupText) to plain strings
def to_plain(value: Any) -> str:
    """Convert non-JSON-serializable objects to plain strings, removing TeX commands."""
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        result = str(value)
    else:
        # Try common text attributes/callables that acl_anthology objects might expose
        result = ""
    for attr in ("text", "plain", "plaintext", "as_text"):
        if hasattr(value, attr):
            try:
                v = getattr(value, attr)
                v = v() if callable(v) else v
                result = str(v) if v is not None else ""
                break
            except Exception:
                pass
    # Fallback
    if not result:
        try:
            result = str(value)
        except Exception:
            return ""

    # Remove common TeX commands for cleaner output
    result = re.sub(r'\\[a-zA-Z]+(\{[^}]*})?', '', result)
    result = re.sub(r'[{}]', '', result)
    result = re.sub(r'\s+', ' ', result).strip()

    return result


def normalize_venue(v: str) -> str | None:
    if not v:
        return None
    v_up = v.strip().upper()
    if v_up.startswith("FINDINGS"):
        return "FINDINGS"
    for key in ("ACL", "NAACL", "EACL", "EMNLP", "CONLL", "COLING", "LREC"):
        if key in v_up:
            return key if key != "CONLL" else "CONLL"
    return None


VENUE_ID_TO_ENUM = {
    "acl": "ACL",
    "naacl": "NAACL",
    "eacl": "EACL",
    "emnlp": "EMNLP",
    "conll": "CONLL",
    "coling": "COLING",
    "lrec": "LREC",
    "findings": "FINDINGS",
    "acl-findings": "FINDINGS",
}


def pick_venue(venue_ids: List[str]) -> str | None:
    if not venue_ids:
        return None
    for vid in venue_ids:
        v = VENUE_ID_TO_ENUM.get(vid.lower())
        if v:
            return v
    return None


def harvest(year_from: int, year_to: int) -> List[Dict[str, Any]]:
    print(f"[Python] Harvesting papers from {year_from} to {year_to}...")
    anth = get_anthology()  # Use cached instance

    out: List[Dict[str, Any]] = []
    paper_count = 0
    for p in anth.papers():
        paper_count += 1
        if paper_count % 1000 == 0:
            print(f"[Python] Processed {paper_count} papers, collected {len(out)} matching papers so far...")

        try:
            y = int(p.year) if p.year else 0
        except Exception:
            y = 0
        if not (year_from <= y <= year_to):
            continue

        vids = list(getattr(p, "venue_ids", []) or [])
        venue_enum = pick_venue(vids)
        if not venue_enum:
            continue
        authors = []
        # try:
        #     for a in (getattr(p, "authors", []) or []):
        #         name = to_plain(getattr(a, "name", None))
        #         affiliation = to_plain(getattr(a, "affiliation", None))
        #         authors.append({"name": name, "affiliation": affiliation})
        # except Exception:
        #     pass
        for a in (getattr(p, "authors", []) or []):
            try:
                authors.append({"name": str(a), "affiliation": ""})
            except Exception:
                pass

        # Convert possibly non-serializable fields to plain strings
        pid = getattr(p, "id", None) or getattr(p, "anthology_id", None)
        title = str(getattr(p, "title", "") or "")
        abstract = str(getattr(p, "abstract", "") or "")

        # PDF
        pdf_url = ""
        try:
            pdf_ref = getattr(p, "pdf", None)
            if pdf_ref is not None:
                # pdf_ref est un PDFReference -> pdf_ref.url donne l'URL complète
                pdf_url = getattr(pdf_ref, "url", "") or ""
        except Exception:
            pdf_url = ""

        # Page web (toujours utile même si pas de pdf)
        web_url = ""
        try:
            web_url = getattr(p, "web_url", "") or ""
        except Exception:
            pass

        # DOI (quand dispo)
        doi = ""
        try:
            doi = getattr(p, "doi", "") or ""
        except Exception:
            doi = ""

        # Skip frontmatter (ex: "Proceedings of ...")
        if getattr(p, "is_frontmatter", False):
            continue

        if getattr(p, "type", None) in {"frontmatter", "backmatter"}:
            continue

        rec = {
            "id": to_plain(pid),
            "title": title,
            "abs": abstract,
            "year": y,
            "venue": venue_enum,
            "venueRaw": vids,
            "pdfUrl": pdf_url or web_url,  # priorité au PDF ; fallback vers la page
            "authors": authors,
            "doi": doi or None,
            "openAlexId": None,
            "citedByCount": None,
            "isClassificationCandidate": None,
            "isDatasetOrBenchmarkCandidate": None,
            "signals": None,
            "referencedWorks": None
        }
        if rec["id"] and rec["title"]:
            out.append(rec)

    print(f"[Python] Harvest complete! Collected {len(out)} papers matching criteria.")
    return out


class EndPoint(object):
    class Java:
        implements = ['udem.taln.wrapper.ACLInterface']

    @staticmethod
    def getPapers(year_from: int, year_to: int) -> str:
        print(f"[Python] getPapers called with year_from={year_from}, year_to={year_to}")
        data = harvest(year_from, year_to)
        # default=str as a safety net in case any unexpected object slips through
        return json.dumps({"papers": data}, ensure_ascii=False, default=str)


def main():
    gateway = JavaGateway(
        gateway_parameters=GatewayParameters(address="127.0.0.1", port=25333),
        callback_server_parameters=CallbackServerParameters()
    )
    gateway.entry_point.registerPythonObject(EndPoint())
    print("Python side registered. Waiting for calls...")
    import time
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
