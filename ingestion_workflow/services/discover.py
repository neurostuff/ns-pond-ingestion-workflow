"""Find base studies in Neurostore that this pipeline has not processed.

Read-only. Uses the same database the upload stage writes to, so discovery and
upload cannot drift apart.
"""

from __future__ import annotations

import logging
from typing import Iterator, Optional

from sqlalchemy import and_, select

from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.services.upload_models import BaseStudy, Study, StudysetStudy

logger = logging.getLogger(__name__)

#: Studies this pipeline produced. A base study that already has one has been
#: processed and is not work.
LLM_SOURCE = "llm"

#: `base_studies.level` is one of 'group' or 'meta'. Meta-analyses are not
#: papers to extract coordinates from.
GROUP_LEVEL = "group"


def unprocessed_base_studies(session, *, limit: Optional[int] = None) -> Iterator[Identifier]:
    """Group-level base studies that are in a studyset and have no llm study.

    In a studyset, because that is what marks a base study as one someone
    actually wants; without an llm study, because that is what this pipeline
    would add.
    """
    in_a_studyset = (
        select(Study.id)
        .join(StudysetStudy, StudysetStudy.study_id == Study.id)
        .where(Study.base_study_id == BaseStudy.id)
        .exists()
    )
    already_processed = (
        select(Study.id)
        .where(and_(Study.base_study_id == BaseStudy.id, Study.source == LLM_SOURCE))
        .exists()
    )

    query = (
        select(BaseStudy.id, BaseStudy.doi, BaseStudy.pmid, BaseStudy.pmcid)
        .where(BaseStudy.level == GROUP_LEVEL)
        .where(in_a_studyset)
        .where(~already_processed)
        .order_by(BaseStudy.id)
    )
    if limit:
        query = query.limit(limit)

    unusable = 0
    for base_study_id, doi, pmid, pmcid in session.execute(query):
        if not (doi or pmid or pmcid):
            # Nothing any download source could address; registering it would
            # create an article the pipeline can never act on.
            unusable += 1
            continue
        yield Identifier(neurostore=base_study_id, doi=doi, pmid=pmid, pmcid=pmcid)

    if unusable:
        logger.warning(
            "Skipped %d base studies with no doi, pmid or pmcid to download from", unusable
        )


__all__ = ["unprocessed_base_studies"]
