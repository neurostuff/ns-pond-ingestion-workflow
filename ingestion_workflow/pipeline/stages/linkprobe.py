"""Whether the OSF projects and OpenNeuro datasets an article links to hold NIfTI files.

Reads the links `links` found in the text triage judges (`judged_links`) and asks about
those that name an OSF guid or an OpenNeuro dataset (`paper_links.probe_target`); no other
link is asked about. OSF lists the project's files (`clients.osf`); OpenNeuro names the
snapshot's modalities (`clients.openneuro`). Each answer records what was seen and when.

A project or dataset that is private or gone is an answer: `has_nifti` stays None with
the reason. A repository down or busy is not: the link is noted `transient` and asked
again after `retry_after`, and once one has failed this run its other links wait for the
next run rather than each spending its retries.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Sequence

from ingestion_workflow.catalog import ArticleRef, Artifact, Notes, Outcome, Status, fingerprint
from ingestion_workflow.utils.http import transient
from ingestion_workflow.utils.isolation import error_text

from ..plan import StagePlan, Work
from ..stage import Context, alone
from .links import judged_links

logger = logging.getLogger(__name__)

#: Bump when what is asked, or how an answer is read, changes.
LINKPROBE_VERSION = 1

#: Each repository's name in a probe and in its notes.
REPOSITORY_NAMES = {"osf": "OSF", "openneuro": "OpenNeuro"}


class LinkprobeStage:
    name = "linkprobe"
    requires = "links"

    def __init__(self, settings) -> None:
        self.settings = settings
        self._osf = None
        self._openneuro = None
        #: This run's answers, by target: a dataset many papers link to is asked once.
        self._answered: Dict[object, dict] = {}
        #: The repositories that were down or busy this run, with what they said.
        self._down: Dict[str, str] = {}
        self._waiting = 0

    def clients(self):
        from ingestion_workflow.clients.openneuro import OpenNeuroClient
        from ingestion_workflow.clients.osf import OsfClient

        if self._osf is None:
            self._osf = OsfClient()
            self._openneuro = OpenNeuroClient()
        return self._osf, self._openneuro

    def fingerprint_for(self, urls: Sequence[str]) -> str:
        return fingerprint("linkprobe", LINKPROBE_VERSION, *sorted(urls))

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        found = judged_links(ctx, refs, upstream)
        attempts = ctx.catalog.attempt_counts([ref.id for ref in refs], self.name, "")
        for ref in refs:
            links = found.get(ref.id)
            if links is None:
                plan.blocked += 1
                continue
            fp = self.fingerprint_for((links.summary or {}).get("probe") or [])
            existing = artifacts.get(ref.id, {}).get("")
            if ctx.is_fresh(existing, fp):
                plan.fresh += 1
                continue
            count, last = attempts.get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name):
                plan.permanent += 1
                continue
            plan.pending.append(Work(ref=ref, source="", fingerprint=fp, upstream=links))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        for work in works:
            yield alone(self.name, work, lambda work=work: self._outcome(work))

    def _outcome(self, work: Work) -> Outcome:
        from ingestion_workflow.services.paper_links import probe_target

        notes = Notes()
        probes = []
        for url in (work.upstream.summary or {}).get("probe") or []:
            target = probe_target(url)
            probe = {"url": url, **self.probe(target)}
            if probe.pop("transient", False):
                notes.error(url, probe["reason"], transient=True)
            probes.append(probe)
        counts = {"probed": len(probes),
                  "with_nifti": sum(p["has_nifti"] is True for p in probes),
                  "without_nifti": sum(p["has_nifti"] is False for p in probes),
                  "unknown": sum(p["has_nifti"] is None for p in probes)}
        if notes.transient:
            self._waiting += 1
        return Outcome(article_id=work.article_id, stage=self.name, source="", status=Status.OK,
                       fingerprint=work.fingerprint, payload={"probes": probes},
                       summary={**counts, **notes.summary()})

    def probe(self, target) -> dict:
        """{repository, id, version, has_nifti, reason, seen, checked_at} for one target,
        with `transient` when the repository was down or busy."""
        if target in self._answered:
            return dict(self._answered[target])
        name = REPOSITORY_NAMES[target.repository]
        base = {"repository": target.repository, "id": target.id, "version": target.version}
        if target.repository in self._down:
            return {**base, "has_nifti": None, "transient": True,
                    "reason": f"{name} did not answer earlier in this run: "
                              f"{self._down[target.repository]}"}
        osf, openneuro = self.clients()
        try:
            if target.repository == "osf":
                answer = osf.listing(target.id, target.view_only)
            else:
                answer = openneuro.snapshot(target.id, target.version)
        except Exception as exc:  # noqa: BLE001 - a repository down is noted; others raise
            if not transient(exc):
                raise
            self._down[target.repository] = error_text(exc)
            logger.warning("linkprobe: %s did not answer: %s", name, exc)
            return {**base, "has_nifti": None, "transient": True,
                    "reason": f"{name} did not answer: {error_text(exc)}"}
        probe = {**base, "has_nifti": answer.has_nifti, "reason": answer.reason,
                 "seen": answer.seen(),
                 "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        if getattr(answer, "transient", False) and answer.has_nifti is None:
            probe["transient"] = True
        else:
            self._answered[target] = probe
        return dict(probe)

    def take_notes(self) -> List[str]:
        """What the run report says of the articles a repository busy or down left unsettled."""
        notes = ([f"{self._waiting:,} article(s) wait for a repository that was busy or down"]
                 if self._waiting else [])
        if self._down:
            notes.append("repositories down this run: " + ", ".join(
                f"{REPOSITORY_NAMES[r]} ({why})" for r, why in sorted(self._down.items())))
        self._waiting = 0
        self._down.clear()
        return notes
