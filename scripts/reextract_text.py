"""Re-extract article text from existing ns-pond source XML, preserving cross-references.

Run this after updating pubget and elsevier-coordinate-extraction packages to the
versions that support preserve_cross_references / preserve-crossrefs. It overwrites
processed/{source}/text.txt in-place for every article that has a source XML file.

Usage:
    .venv/bin/python scripts/reextract_text.py [--workers N] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

NS_POND_DATA = Path("/data/alejandro/projects/ns-pond/data")


def _reextract_pubget(article_dir: Path) -> tuple[bool, str]:
    source_xml = article_dir / "source" / "pubget" / "article.xml"
    text_path = article_dir / "processed" / "pubget" / "text.txt"

    if not source_xml.exists():
        return False, "no source XML"

    try:
        from lxml import etree
        from pubget._utils import load_stylesheet

        article_tree = etree.parse(str(source_xml))
        stylesheet = load_stylesheet("text_extraction.xsl")
        transformed = stylesheet(
            article_tree, **{"preserve-crossrefs": etree.XSLT.strparam("true")}
        )

        text_parts = []
        for field in ("title", "keywords", "abstract", "body"):
            elem = transformed.find(field)
            if elem is not None and elem.text:
                part = elem.text.strip()
                if part:
                    text_parts.append(part)
        full_text = "\n\n".join(text_parts)

        text_path.parent.mkdir(parents=True, exist_ok=True)
        text_path.write_text(full_text, encoding="utf-8")
        return True, ""
    except Exception as exc:
        return False, str(exc)


def _reextract_elsevier(article_dir: Path) -> tuple[bool, str]:
    source_xml = article_dir / "source" / "elsevier" / "content.xml"
    metadata_json = article_dir / "source" / "elsevier" / "metadata.json"
    text_path = article_dir / "processed" / "elsevier" / "text.txt"

    if not source_xml.exists():
        return False, "no source XML"

    try:
        from elsevier_coordinate_extraction.extract.text import save_article_text
        from elsevier_coordinate_extraction.types import ArticleContent

        payload = source_xml.read_bytes()
        metadata: dict = {}
        if metadata_json.exists():
            try:
                metadata = json.loads(metadata_json.read_text(encoding="utf-8"))
            except Exception:
                pass

        article_content = ArticleContent(
            doi=metadata.get("doi") or "",
            payload=payload,
            content_type="application/xml",
            format="xml",
            retrieved_at=metadata.get("retrieved_at") or "",
            metadata=metadata,
        )

        text_path.parent.mkdir(parents=True, exist_ok=True)
        save_article_text(
            article_content,
            text_path.parent,
            stem="text",
            preserve_cross_references=True,
        )
        return True, ""
    except Exception as exc:
        return False, str(exc)


def _process_article(args: tuple[str, Path]) -> tuple[str, Path, bool, str]:
    source, article_dir = args
    if source == "pubget":
        ok, msg = _reextract_pubget(article_dir)
    else:
        ok, msg = _reextract_elsevier(article_dir)
    return source, article_dir, ok, msg


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=NS_POND_DATA,
        help="Root ns-pond data directory",
    )
    opts = parser.parse_args()

    data_dir: Path = opts.data_dir
    article_dirs = sorted(d for d in data_dir.iterdir() if d.is_dir())
    logger.info("Scanning %d article directories under %s", len(article_dirs), data_dir)

    pubget_dirs = [d for d in article_dirs if (d / "source" / "pubget" / "article.xml").exists()]
    elsevier_dirs = [
        d for d in article_dirs if (d / "source" / "elsevier" / "content.xml").exists()
    ]
    logger.info(
        "Found %d pubget and %d elsevier source XML files",
        len(pubget_dirs),
        len(elsevier_dirs),
    )

    if opts.dry_run:
        logger.info("Dry-run: exiting without writing.")
        return

    tasks: list[tuple[str, Path]] = (
        [("pubget", d) for d in pubget_dirs] + [("elsevier", d) for d in elsevier_dirs]
    )
    total = len(tasks)
    ok_count = 0
    fail_count = 0

    with ProcessPoolExecutor(max_workers=opts.workers) as pool:
        futures = {pool.submit(_process_article, t): t for t in tasks}
        done = 0
        for future in as_completed(futures):
            done += 1
            try:
                source, article_dir, ok, msg = future.result()
            except Exception as exc:
                source, article_dir = futures[future]
                ok, msg = False, str(exc)

            if ok:
                ok_count += 1
            else:
                fail_count += 1
                logger.warning("[%s] %s: %s", source, article_dir.name, msg)

            if done % 1000 == 0 or done == total:
                logger.info(
                    "Progress: %d/%d  ok=%d  failed=%d", done, total, ok_count, fail_count
                )

    logger.info("Done. ok=%d  failed=%d", ok_count, fail_count)
    if fail_count:
        sys.exit(1)


if __name__ == "__main__":
    main()
