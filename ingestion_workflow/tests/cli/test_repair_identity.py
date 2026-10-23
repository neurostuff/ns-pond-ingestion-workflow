"""`ingest repair-identity`: a reviewed plan, dry run by default, all or nothing with a backup."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing

import ingestion_workflow.cli.main as cli
import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.models.ids import Identifier, Identifiers
from ingestion_workflow.pipeline.selection import resolve_manifest
from ingestion_workflow.tests import cassette_secrets
from typer.testing import CliRunner


def _config(root):
    root.mkdir(parents=True, exist_ok=True)
    config = root / "settings.yaml"
    config.write_text(json.dumps({
        "data_root": str(root / "data"),
        "cache_root": str(root / "cache"),
        "catalog_root": str(root / "catalog"),
        "ns_pond_root": str(root / "pond"),
        "log_to_file": False,
        "show_progress": False,
        "pubmed_email": cassette_secrets.REPLAY_EMAIL,
    }), encoding="utf-8")
    return config


@pytest.fixture()
def setup(tmp_path):
    config = _config(tmp_path)
    with Catalog.open(tmp_path / "catalog") as cat:
        # Two articles with each other's PMIDs; one holding a companion abstract's DOI and
        # its download; one holding a repository copy's DOI.
        ids = {
            "a": cat.register(Identifier(doi="10.1016/j.ajog.2011.02.050", pmid="21529779")).id,
            "b": cat.register(Identifier(doi="10.1016/j.apmr.2010.12.021", pmid="21529758")).id,
            "c": cat.register(Identifier(doi="10.1016/j.bpsc.2021.12.008", pmid="34954395")).id,
            "d": cat.register(Identifier(doi="10.1176/ajp.2006.163.4.735", pmid="16585452")).id,
        }
        # Written as the catalog took them before registration refused a second journal DOI.
        cat._conn.executemany("INSERT INTO aliases VALUES ('doi', ?, ?)", [
            ("10.1016/j.biopsych.2021.02.423", ids["c"]),
            ("10.1176/appi.ajp.163.4.735", ids["d"]),
        ])
        cat.record([Outcome(ids["c"], "download", "elsevier", Status.OK, payload={"files": []}),
                    Outcome(ids["c"], "download", "pubget", Status.OK, payload={"files": []})])
    return config, tmp_path, ids


def _plan(tmp_path, ids):
    a, b, c, d = ids["a"], ids["b"], ids["c"], ids["d"]
    lines = [
        {"op": "reassign", "moves": [
            {"kind": "pmid", "value": "21529779", "from": a, "to": b},
            {"kind": "pmid", "value": "21529758", "from": b, "to": a}],
         "why": "PubMed 21529758 is 10.1016/j.ajog.2011.02.050"},
        {"op": "split", "article": c, "aliases": [["doi", "10.1016/j.biopsych.2021.02.423"]],
         "artifacts": [["download", "elsevier"]]},
        {"op": "remove", "article": d, "kind": "doi", "value": "10.1176/appi.ajp.163.4.735"},
        {"op": "add", "article": d, "kind": "neurostore", "value": "3Rob7cK8y6gY"},
    ]
    path = tmp_path / "plan.jsonl"
    path.write_text("\n".join(json.dumps(line) for line in lines) + "\n", encoding="utf-8")
    return path


def _invoke(config, *args):
    return CliRunner().invoke(
        cli.app, ["repair-identity", *map(str, args), "--config", str(config)])


def _db(tmp_path):
    with closing(sqlite3.connect(tmp_path / "catalog" / "catalog.sqlite")) as conn:
        return [sorted(conn.execute(f"SELECT * FROM {t}"))
                for t in ("articles", "aliases", "artifacts")]


@pytest.mark.vcr()
def test_the_default_runs_the_plan_and_writes_nothing(setup):
    config, tmp_path, ids = setup
    before = _db(tmp_path)

    result = _invoke(config, _plan(tmp_path, ids))

    assert result.exit_code == 0, result.output
    assert "4 of 4 operations would apply; 1 new articles; 5 articles touched" in result.output
    assert "dry run: nothing written" in result.output
    assert _db(tmp_path) == before


def test_apply_needs_a_new_backup_and_a_manifest(setup):
    config, tmp_path, ids = setup
    plan = _plan(tmp_path, ids)
    assert _invoke(config, plan, "--apply").exit_code == 2
    assert _invoke(config, plan, "--apply", "--backup", tmp_path / "b.sqlite").exit_code == 2
    (tmp_path / "taken.sqlite").write_text("")
    assert _invoke(config, plan, "--apply", "--backup", tmp_path / "taken.sqlite",
                   "--manifest", tmp_path / "m.jsonl").exit_code == 2


@pytest.mark.vcr()
def test_apply_writes_the_plan_backs_up_first_and_lists_the_touched_articles(setup):
    config, tmp_path, ids = setup
    before = _db(tmp_path)
    backup, manifest = tmp_path / "pre-identity.sqlite", tmp_path / "touched.jsonl"

    result = _invoke(config, _plan(tmp_path, ids), "--apply", "--backup", backup,
                     "--manifest", manifest)

    assert result.exit_code == 0, result.output
    assert ("ingest run -s metadata --refresh metadata -s triage --refresh triage "
            "-s prose --refresh prose --manifest") in result.output
    with closing(sqlite3.connect(backup)) as conn:
        assert [sorted(conn.execute(f"SELECT * FROM {t}"))
                for t in ("articles", "aliases", "artifacts")] == before
    with Catalog.open(tmp_path / "catalog") as cat:
        assert cat.identifier(ids["a"]).pmid == "21529758"
        assert cat.identifier(ids["b"]).pmid == "21529779"
        companion = cat.resolve(Identifier(doi="10.1016/j.biopsych.2021.02.423")).id
        assert companion not in ids.values()
        assert cat.artifact(companion, "download", "elsevier") is not None
        assert cat.artifact(ids["c"], "download", "pubget") is not None
        assert cat.aliases(ids["d"], "doi") == ["10.1176/ajp.2006.163.4.735"]
        assert cat.identifier(ids["d"]).neurostore == "3Rob7cK8y6gY"

        listed = Identifiers.load(manifest).identifiers
        assert len(listed) == 5
        articles = cat.count_articles()
        # Registered again by `ingest run --manifest`, each line is the article it came from.
        refs = resolve_manifest(cat, manifest, register=True).refs
        assert sorted(r.id for r in refs) == sorted([*ids.values(), companion])
        assert cat.count_articles() == articles and cat.conflicts == 0


@pytest.mark.vcr()
def test_a_plan_the_catalog_no_longer_matches_is_refused_whole(setup):
    config, tmp_path, ids = setup
    plan = _plan(tmp_path, ids)
    with Catalog.open(tmp_path / "catalog") as cat:
        # Since the plan was made, the repository DOI was settled by someone else.
        cat.remove_aliases([("doi", "10.1176/appi.ajp.163.4.735", ids["d"])])
    before = _db(tmp_path)

    result = _invoke(config, plan, "--apply", "--backup", tmp_path / "b.sqlite",
                     "--manifest", tmp_path / "m.jsonl")

    assert result.exit_code == 1
    assert ("refused line 3 (remove): doi 10.1176/appi.ajp.163.4.735 is held by no article"
            in result.output)
    assert "3 of 4 operations would apply" in result.output
    assert _db(tmp_path) == before
    assert not (tmp_path / "m.jsonl").exists()
    assert not (tmp_path / "b.sqlite").exists()


@pytest.mark.vcr()
def test_a_plan_adding_an_identifier_another_article_holds_is_refused(setup):
    config, tmp_path, ids = setup
    plan = tmp_path / "plan.jsonl"
    plan.write_text(json.dumps({"op": "add", "article": ids["a"], "kind": "pmid",
                                "value": "34954395"}) + "\n", encoding="utf-8")
    result = _invoke(config, plan)
    assert result.exit_code == 1
    assert f"pmid 34954395 is held by {ids['c']}" in result.output


@pytest.mark.parametrize("line, why", [
    ({"op": "add", "article": "{a}", "kind": "isbn", "value": "x"}, "isbn is not an alias kind"),
    ({"op": "add", "article": "aaaaaaaaaaaa", "kind": "neurostore", "value": "3Rob7cK8y6gY"},
     "aaaaaaaaaaaa is not a live article"),
])
@pytest.mark.vcr()
def test_an_add_of_no_kind_or_to_no_article_is_refused(setup, line, why):
    config, tmp_path, ids = setup
    plan = tmp_path / "plan.jsonl"
    plan.write_text(json.dumps(line).replace("{a}", ids["a"]) + "\n", encoding="utf-8")
    result = _invoke(config, plan)
    assert result.exit_code == 1
    assert f"refused line 1 (add): {why}" in result.output


@pytest.mark.vcr()
def test_a_plan_whose_result_breaks_an_invariant_is_not_committed(setup):
    config, tmp_path, ids = setup
    plan = _plan(tmp_path, ids)
    # a repository copy's DOI beside the paper's own, once the rest is repaired
    with plan.open("a", encoding="utf-8") as f:
        f.write(json.dumps({"op": "add", "article": ids["d"], "kind": "doi",
                            "value": "10.17169/refubium-31155"}) + "\n")
    before = _db(tmp_path)

    result = _invoke(config, plan, "--apply", "--backup", tmp_path / "b.sqlite",
                     "--manifest", tmp_path / "m.jsonl")

    assert result.exit_code == 1
    assert (f"violation {ids['d']} journal-dois: holds 10.1176/ajp.2006.163.4.735, "
            "10.17169/refubium-31155") in result.output
    assert "0 refused, 1 violations the plan makes. Nothing was written" in result.output
    assert _db(tmp_path) == before
    assert not (tmp_path / "m.jsonl").exists()
    assert not (tmp_path / "b.sqlite").exists()


def test_the_pmids_are_checked_against_pubmed_so_pubmed_is_required(setup):
    config, tmp_path, ids = setup
    settings = json.loads(config.read_text())
    del settings["pubmed_email"]
    config.write_text(json.dumps(settings))
    result = _invoke(config, _plan(tmp_path, ids))
    assert result.exit_code == 2
    assert "pubmed_email" in result.output


@pytest.mark.vcr()
def test_a_dry_run_given_a_backup_path_writes_neither(setup):
    config, tmp_path, ids = setup
    before = _db(tmp_path)
    result = _invoke(config, _plan(tmp_path, ids), "--backup", tmp_path / "b.sqlite")
    assert result.exit_code == 0, result.output
    assert _db(tmp_path) == before
    assert not (tmp_path / "b.sqlite").exists()


@pytest.fixture()
def cases(tmp_path):
    """The production catalog's cases each operation repairs, in a catalog of their own."""
    tmp_path = tmp_path / "cases"
    config = _config(tmp_path)
    files = tmp_path / "files"
    with Catalog.open(tmp_path / "catalog") as cat:
        ids = {
            # w5rawsklx347, and 5qxpg3twr524 holding only PubMed's second record of its paper
            "paper": cat.register(Identifier(doi="10.1684/epd.2022.1433", pmid="35770780")).id,
            "duplicate": cat.register(Identifier(pmid="37401785")).id,
            # vrr7x2ygtjdt, and btk5tlwqap5k holding only its preprint's PubMed record
            "elife": cat.register(Identifier(doi="10.7554/elife.92563", pmid="38345852")).id,
            "preprint": cat.register(Identifier(pmid="37808793")).id,
            # mwni3etc4i7l holding an Elsevier copy of 54xpppoekzpj's paper
            "holder": cat.register(Identifier(doi="10.1016/j.neuropsychologia.2011.03.007")).id,
            "owner": cat.register(Identifier(doi="10.1016/j.neuroimage.2004.05.015")).id,
            # og3a4u5wsy26 holding a Neurobiol Learn Mem paper beside its own
            "fused": cat.register(
                Identifier(doi="10.1016/j.biopsych.2011.09.005", pmid="22015111")).id,
            # ki7akud6c347 holding PubMed's Springer DOI and the journal's earlier mEDRA DOI
            "alias": cat.register(Identifier(doi="10.1007/bf03337758", pmid="20885123")).id,
        }
        conn = cat._conn
        conn.executemany("INSERT INTO aliases VALUES (?, ?, ?)", [
            ("doi", "10.1101/2023.09.27.559792", ids["elife"]),
            ("doi", "10.1016/j.nlm.2011.06.006", ids["fused"]),
            ("doi", "10.3275/7271", ids["alias"]),
        ])
        copy = files / "elsevier" / "content.xml"
        copy.parent.mkdir(parents=True)
        copy.write_text("<prism:doi>10.1016/j.neuroimage.2004.05.015</prism:doi>")
        payload = {"files": [{"file_path": str(copy)}]}
        cat.record([
            Outcome(ids["holder"], "download", "elsevier", Status.OK, payload=payload),
            Outcome(ids["holder"], "extract", "elsevier", Status.OK, payload={"text": "x"}),
            Outcome(ids["holder"], "passages", "", Status.OK, payload={"passages": []}),
            Outcome(ids["duplicate"], "download", "pubget", Status.OK, payload={"files": []}),
            Outcome(ids["fused"], "download", "ace", Status.OK, payload={"files": []}),
        ])
    lines = [
        {"op": "merge", "articles": [ids["paper"], ids["duplicate"]],
         "drop": [["pmid", "37401785"]]},
        {"op": "merge", "articles": [ids["elife"], ids["preprint"]],
         "rekind": [["pmid", "37808793"]]},
        {"op": "move_artifacts", "from": ids["holder"], "to": ids["owner"],
         "artifacts": [["download", "elsevier"], ["extract", "elsevier"]]},
        {"op": "split", "article": ids["fused"], "aliases": [["doi", "10.1016/j.nlm.2011.06.006"]],
         "add": [["pmid", "21704176"]]},
        {"op": "forget_artifacts", "article": ids["fused"], "artifacts": [["download", "ace"]]},
        {"op": "rekind", "article": ids["alias"], "kind": "doi", "value": "10.3275/7271"},
    ]
    plan = tmp_path / "every-op.jsonl"
    plan.write_text("".join(json.dumps(line) + "\n" for line in lines), encoding="utf-8")
    return config, tmp_path, ids, plan


@pytest.mark.vcr()
def test_every_operation_applies_and_its_articles_are_listed(cases):
    config, tmp_path, ids, plan = cases
    manifest = tmp_path / "touched.jsonl"

    result = _invoke(config, plan, "--apply", "--backup", tmp_path / "b.sqlite",
                     "--manifest", manifest)

    assert result.exit_code == 0, result.output
    assert "6 of 6 operations applied; 1 new articles" in result.output
    assert "1 articles were left with no extraction and their passages taken back" in result.output
    with Catalog.open(tmp_path / "catalog") as cat:
        paper = cat.current_id(ids["paper"])
        assert cat.aliases(paper, "pmid") == ["35770780"]
        assert cat.holder("pmid", "37401785") == paper
        elife = cat.current_id(ids["elife"])
        assert cat.aliases(elife, "preprint_pmid") == ["37808793"]
        assert cat.artifact(ids["owner"], "download", "elsevier") is not None
        taken = cat.artifact(ids["holder"], "passages", "")
        assert (taken.status, taken.error, taken.fingerprint) == (
            Status.FAILED, "no extraction text", "no extraction text")
        nlm = cat.holder("doi", "10.1016/j.nlm.2011.06.006")
        assert nlm != ids["fused"] and cat.aliases(nlm, "pmid") == ["21704176"]
        assert cat.artifact(ids["fused"], "download", "ace") is None
        assert cat.aliases(ids["alias"], "alternate_doi") == ["10.3275/7271"]
        listed = {cat.resolve(i).id for i in Identifiers.load(manifest).identifiers}
        assert listed == {paper, elife, ids["holder"], ids["owner"], ids["fused"], nlm,
                          ids["alias"]}


@pytest.mark.vcr()
def test_an_article_a_plan_does_not_touch_is_checked_when_the_plan_breaks_it(tmp_path):
    # B holds PMID 21704176 alone; the plan gives A that PMID's DOI.
    config = _config(tmp_path)
    with Catalog.open(tmp_path / "catalog") as cat:
        a = cat.register(Identifier(doi="10.1016/j.biopsych.2011.09.005")).id
        b = cat.register(Identifier(pmid="21704176")).id
    plan = tmp_path / "plan.jsonl"
    plan.write_text(json.dumps({"op": "add", "article": a, "kind": "doi",
                                "value": "10.1016/j.nlm.2011.06.006"}) + "\n", encoding="utf-8")
    result = _invoke(config, plan, "--apply", "--backup", tmp_path / "b.sqlite",
                     "--manifest", tmp_path / "m.jsonl")
    assert result.exit_code == 1
    assert (f"violation {b} pmid-doi: pmid 21704176 is 10.1016/j.nlm.2011.06.006 in PubMed"
            in result.output)


@pytest.mark.vcr()
def test_an_article_whose_pmc_record_a_plan_gives_away_is_checked(tmp_path):
    # B holds PMC5582689 and its PubMed record 28290265 (Psychol Med, lg6selcsnr7f's paper);
    # the plan gives A that paper's DOI.
    config = _config(tmp_path)
    with Catalog.open(tmp_path / "catalog") as cat:
        a = cat.register(Identifier(neurostore="3Rob7cK8y6gY")).id
        b = cat.register(Identifier(pmcid="PMC5582689", pmid="28290265")).id
    plan = tmp_path / "plan.jsonl"
    plan.write_text(json.dumps({"op": "add", "article": a, "kind": "doi",
                                "value": "10.1017/s0033291717000265"}) + "\n", encoding="utf-8")
    result = _invoke(config, plan, "--apply", "--backup", tmp_path / "b.sqlite",
                     "--manifest", tmp_path / "m.jsonl")
    assert result.exit_code == 1
    assert (f"violation {b} pmcid: PMC5582689 is 10.1017/s0033291717000265 in PMC, which {a} holds"
            in result.output)


@pytest.mark.vcr()
def test_an_article_holding_two_of_a_kind_before_the_plan_is_reported_and_does_not_block(tmp_path):
    # B is linked to nothing the plan touches; it holds two PMCIDs, as the catalog took
    # them before registration refused a second paper's, and waits for a person.
    config = _config(tmp_path)
    with Catalog.open(tmp_path / "catalog") as cat:
        a = cat.register(Identifier(neurostore="3Rob7cK8y6gY")).id
        b = cat.register(Identifier(pmcid="PMC3189437")).id
        cat._conn.execute("INSERT INTO aliases VALUES ('pmcid', 'PMC5382267', ?)", (b,))
    plan = tmp_path / "plan.jsonl"
    plan.write_text(json.dumps({"op": "add", "article": a, "kind": "doi",
                                "value": "10.1017/s0033291717000265"}) + "\n", encoding="utf-8")
    result = _invoke(config, plan, "--apply", "--backup", tmp_path / "b.sqlite",
                     "--manifest", tmp_path / "m.jsonl")
    assert result.exit_code == 0, result.output
    assert f"pre-existing, not caused by this plan: {b} pmcids:" in result.output
    with Catalog.open(tmp_path / "catalog") as cat:
        assert cat.holder("doi", "10.1017/s0033291717000265") == a


@pytest.mark.vcr()
def test_a_plan_giving_an_article_a_second_of_a_kind_is_not_committed(tmp_path):
    config = _config(tmp_path)
    with Catalog.open(tmp_path / "catalog") as cat:
        b = cat.register(Identifier(pmcid="PMC3189437")).id
        cat._conn.execute("INSERT INTO aliases VALUES ('pmcid', 'PMC5382267', ?)", (b,))
    plan = tmp_path / "plan.jsonl"
    plan.write_text(json.dumps({"op": "add", "article": b, "kind": "neurostore",
                                "value": "3Rob7cK8y6gY"}) + "\n", encoding="utf-8")
    with Catalog.open(tmp_path / "catalog") as cat:
        cat._conn.execute("INSERT INTO aliases VALUES ('neurostore', 'AkYZauDLLAS5', ?)", (b,))
    before = _db(tmp_path)
    result = _invoke(config, plan, "--apply", "--backup", tmp_path / "b.sqlite",
                     "--manifest", tmp_path / "m.jsonl")
    assert result.exit_code == 1
    assert f"violation {b} base-studies:" in result.output
    assert f"pre-existing, not caused by this plan: {b} pmcids:" in result.output
    assert _db(tmp_path) == before


@pytest.mark.vcr()
def test_an_alternate_doi_the_registry_cannot_describe_is_not_committed(tmp_path):
    # mxp7s3fbegzb: doi.org has the publisher's alias spelling of PubMed's DOI, and
    # sends it to Crossref, which has no record of it.
    config = _config(tmp_path)
    with Catalog.open(tmp_path / "catalog") as cat:
        a = cat.register(Identifier(doi="10.1176/jnp.2009.21.3.307")).id
        cat._conn.execute(
            "INSERT INTO aliases VALUES ('doi', '10.1176/appi.neuropsych.21.3.307', ?)", (a,))
    plan = tmp_path / "plan.jsonl"
    line = {"op": "rekind", "article": a, "kind": "doi",
            "value": "10.1176/appi.neuropsych.21.3.307"}
    plan.write_text(json.dumps(line) + "\n", encoding="utf-8")
    result = _invoke(config, plan, "--apply", "--backup", tmp_path / "b.sqlite",
                     "--manifest", tmp_path / "m.jsonl")
    assert result.exit_code == 1
    assert (f"violation {a} evidence-unreadable: 10.1176/appi.neuropsych.21.3.307: doi.org sends "
            "it to api.crossref.org, which has no record") in result.output


@pytest.mark.vcr()
def test_an_article_keeping_an_extraction_has_nothing_taken_back(tmp_path):
    config = _config(tmp_path)
    with Catalog.open(tmp_path / "catalog") as cat:
        a = cat.register(Identifier(doi="10.1016/j.neuropsychologia.2011.03.007")).id
        b = cat.register(Identifier(doi="10.1016/j.neuroimage.2004.05.015")).id
        cat.record([Outcome(a, stage, source, Status.OK, payload={"files": []})
                    for stage in ("download", "extract") for source in ("elsevier", "pubget")]
                   + [Outcome(a, "passages", "", Status.OK, payload={"passages": []})])
    plan = tmp_path / "plan.jsonl"
    plan.write_text(json.dumps({"op": "move_artifacts", "from": a, "to": b, "artifacts": [
        ["download", "elsevier"], ["extract", "elsevier"]]}) + "\n", encoding="utf-8")
    result = _invoke(config, plan, "--apply", "--backup", tmp_path / "b.sqlite",
                     "--manifest", tmp_path / "m.jsonl")
    assert result.exit_code == 0, result.output
    assert "taken back" not in result.output
    with Catalog.open(tmp_path / "catalog") as cat:
        assert cat.artifact(a, "passages", "").ok


@pytest.mark.parametrize("line, why", [
    ("{not json", "line 1: not JSON"),
    (json.dumps({"op": "rename"}), "line 1: op must be one of"),
    (json.dumps({"op": "merge"}), "line 1: merge needs articles"),
])
def test_a_malformed_plan_is_a_usage_error(setup, line, why):
    config, tmp_path, _ = setup
    plan = tmp_path / "plan.jsonl"
    plan.write_text(line + "\n", encoding="utf-8")
    result = _invoke(config, plan)
    assert result.exit_code == 2
    assert why in result.output


@pytest.mark.vcr()
def test_an_article_whose_download_names_an_id_the_plan_hands_out_is_checked(tmp_path):
    # X's ACE copy is 22015111.html, the Biol Psychiatry paper's, though X holds the
    # Neurobiol Learn Mem PMID; the plan gives Y, which holds the Biol Psychiatry DOI, its PMID.
    config = _config(tmp_path)
    with Catalog.open(tmp_path / "catalog") as cat:
        x = cat.register(Identifier(pmid="21704176")).id
        y = cat.register(Identifier(doi="10.1016/j.biopsych.2011.09.005")).id
        cat.record([Outcome(x, "download", "ace", Status.OK, payload={"files": [
            {"file_path": str(tmp_path / "ace" / "22015111.html")}]})])
    plan = tmp_path / "plan.jsonl"
    plan.write_text(json.dumps({"op": "add", "article": y, "kind": "pmid", "value": "22015111"})
                    + "\n", encoding="utf-8")
    result = _invoke(config, plan, "--apply", "--backup", tmp_path / "b.sqlite",
                     "--manifest", tmp_path / "m.jsonl")
    assert result.exit_code == 1
    assert (f"violation {x} download: download/ace names pmid 22015111, which {y} holds"
            in result.output)


@pytest.mark.vcr()
def test_an_article_already_taken_back_is_not_taken_back_again(tmp_path):
    config = _config(tmp_path)
    with Catalog.open(tmp_path / "catalog") as cat:
        a = cat.register(Identifier(doi="10.1016/j.neuropsychologia.2011.03.007")).id
        b = cat.register(Identifier(doi="10.1016/j.neuroimage.2004.05.015")).id
        cat.record([Outcome(a, stage, "elsevier", Status.OK, payload={"files": []})
                    for stage in ("download", "extract")]
                   + [Outcome.failure(a, "passages", "", "no extraction text",
                                      fingerprint="no extraction text")])
    plan = tmp_path / "plan.jsonl"
    plan.write_text(json.dumps({"op": "move_artifacts", "from": a, "to": b, "artifacts": [
        ["download", "elsevier"], ["extract", "elsevier"]]}) + "\n", encoding="utf-8")
    result = _invoke(config, plan, "--apply", "--backup", tmp_path / "b.sqlite",
                     "--manifest", tmp_path / "m.jsonl")
    assert result.exit_code == 0, result.output
    assert "taken back" not in result.output


@pytest.mark.vcr()
def test_a_plan_swaps_two_crossed_downloads(tmp_path):
    # ml7z5xc7cvgo and su74e7qsacoi: each article's ACE copy is the other's paper
    config = _config(tmp_path)
    with Catalog.open(tmp_path / "catalog") as cat:
        a = cat.register(Identifier(pmid="15658005")).id
        b = cat.register(Identifier(pmid="15465288")).id
        cat.record([Outcome(x, "download", "ace", Status.OK,
                            payload={"files": [{"file_path": f"/ace/{p}.html"}]})
                    for x, p in ((a, "15465288"), (b, "15658005"))])
    plan = tmp_path / "plan.jsonl"
    plan.write_text(json.dumps({"op": "swap_artifacts", "articles": [a, b],
                                "artifacts": [["download", "ace"]]}) + "\n", encoding="utf-8")
    result = _invoke(config, plan, "--apply", "--backup", tmp_path / "b.sqlite",
                     "--manifest", tmp_path / "m.jsonl")
    assert result.exit_code == 0, result.output
    assert "1 of 1 operations applied" in result.output and "violation" not in result.output
    with Catalog.open(tmp_path / "catalog") as cat:
        assert cat.payload(cat.artifact(a, "download", "ace"))["files"][0]["file_path"] == \
            "/ace/15658005.html"


@pytest.mark.vcr()
def test_a_violation_naming_an_article_the_plan_merges_away_is_still_the_one_before(tmp_path):
    # X holds the Neurobiol Learn Mem PMID; its DOI is L's. The plan merges L into S, its other
    # record, so the violation then names S.
    config = _config(tmp_path)
    with Catalog.open(tmp_path / "catalog") as cat:
        s = cat.register(Identifier(neurostore="3Rob7cK8y6gY")).id
        x = cat.register(Identifier(pmid="21704176")).id
        lone = cat.register(Identifier(doi="10.1016/j.nlm.2011.06.006")).id
    plan = tmp_path / "plan.jsonl"
    plan.write_text(json.dumps({"op": "merge", "articles": [s, lone]}) + "\n", encoding="utf-8")
    result = _invoke(config, plan, "--apply", "--backup", tmp_path / "b.sqlite",
                     "--manifest", tmp_path / "m.jsonl")
    assert result.exit_code == 0, result.output
    assert (f"pre-existing, not caused by this plan: {x} pmid-doi: pmid 21704176 is "
            f"10.1016/j.nlm.2011.06.006 in PubMed, which {s} holds") in result.output


@pytest.mark.vcr()
def test_an_accepted_version_pair_is_listed_and_does_not_block(tmp_path):
    # bdarfou5chmg and nj52vmozfa3k, each with a base study; the plan forgets the paper's
    # unreadable ACE copy
    config = _config(tmp_path)
    with Catalog.open(tmp_path / "catalog") as cat:
        paper = cat.register(Identifier(pmid="42230985", pmcid="PMC13387087",
                                        doi="10.1038/s41366-026-02114-2",
                                        neurostore="vY5mktCMTn9P")).id
        cat.register(Identifier(pmid="41674811", pmcid="PMC12889832",
                                doi="10.21203/rs.3.rs-8694593/v1", neurostore="JAJmM4dhUPuZ"))
        cat.record([Outcome(paper, "download", "ace", Status.OK, payload={"files": []})])
    plan = tmp_path / "plan.jsonl"
    plan.write_text(json.dumps({"op": "forget_artifacts", "article": paper,
                                "artifacts": [["download", "ace"]]}) + "\n", encoding="utf-8")
    result = _invoke(config, plan, "--apply", "--backup", tmp_path / "b.sqlite",
                     "--manifest", tmp_path / "m.jsonl")
    assert result.exit_code == 0, result.output
    assert "accepted, kept apart until neurostore makes one base study a version" in result.output
    assert "violation " not in result.output
    assert not [line for line in result.output.splitlines()
                if line.startswith("pre-existing") and "two-versions" in line]


@pytest.mark.vcr()
def test_a_plan_breaking_an_accepted_pair_is_not_committed(tmp_path):
    # bdarfou5chmg and nj52vmozfa3k, accepted while each holds a base study; the plan takes
    # the paper's away
    config = _config(tmp_path)
    with Catalog.open(tmp_path / "catalog") as cat:
        paper = cat.register(Identifier(pmid="42230985", pmcid="PMC13387087",
                                        doi="10.1038/s41366-026-02114-2",
                                        neurostore="vY5mktCMTn9P")).id
        cat.register(Identifier(pmid="41674811", pmcid="PMC12889832",
                                doi="10.21203/rs.3.rs-8694593/v1", neurostore="JAJmM4dhUPuZ"))
    plan = tmp_path / "plan.jsonl"
    plan.write_text(json.dumps({"op": "remove", "article": paper, "kind": "neurostore",
                                "value": "vY5mktCMTn9P"}) + "\n", encoding="utf-8")
    result = _invoke(config, plan, "--apply", "--backup", tmp_path / "b.sqlite",
                     "--manifest", tmp_path / "m.jsonl")
    assert result.exit_code == 1
    assert [line for line in result.output.splitlines()
            if line.startswith("violation ") and "two-versions" in line]


@pytest.mark.vcr()
def test_a_download_moved_onto_an_article_with_an_unreadable_one_is_still_checked(tmp_path):
    config = _config(tmp_path)
    with Catalog.open(tmp_path / "catalog") as cat:
        a = cat.register(Identifier(neurostore="3Rob7cK8y6gY")).id
        b = cat.register(Identifier(pmcid="PMC5582689", pmid="28290265")).id
        cat.record([Outcome(a, "download", "ace", Status.OK),
                    Outcome(b, "download", "pubget", Status.OK, payload={"files": [
                        {"file_path": "/pubget/articles/pmcid_5582689/tables.xml"}]})])
    plan = tmp_path / "plan.jsonl"
    plan.write_text(json.dumps({"op": "move_artifacts", "from": b, "to": a,
                                "artifacts": [["download", "pubget"]]}) + "\n", encoding="utf-8")
    result = _invoke(config, plan, "--apply", "--backup", tmp_path / "b.sqlite",
                     "--manifest", tmp_path / "m.jsonl")
    assert result.exit_code == 1
    assert f"violation {a} download: download/pubget names pmcid PMC5582689, which {b} holds" \
        in result.output


@pytest.mark.vcr()
def test_a_violation_of_an_article_the_plan_merges_away_is_still_the_one_before(tmp_path):
    # B holds two PMCIDs and waits for a person; the plan merges it into O, its paper's other
    config = _config(tmp_path)
    with Catalog.open(tmp_path / "catalog") as cat:
        o = cat.register(Identifier(neurostore="3Rob7cK8y6gY")).id
        b = cat.register(Identifier(pmcid="PMC3189437")).id
        cat._conn.execute("INSERT INTO aliases VALUES ('pmcid', 'PMC5382267', ?)", (b,))
        cat._conn.execute("UPDATE articles SET created_at='2000-01-01' WHERE id=?", (o,))
    plan = tmp_path / "plan.jsonl"
    plan.write_text(json.dumps({"op": "merge", "articles": [o, b]}) + "\n", encoding="utf-8")
    result = _invoke(config, plan, "--apply", "--backup", tmp_path / "b.sqlite",
                     "--manifest", tmp_path / "m.jsonl")
    assert result.exit_code == 0, result.output
    assert f"pre-existing, not caused by this plan: {o} pmcids:" in result.output


def test_a_plan_in_which_an_operation_undoes_another_is_refused_whole(setup):
    config, tmp_path, ids = setup
    a = ids["a"]
    plan = tmp_path / "cycle.jsonl"
    plan.write_text("".join(json.dumps(line) + "\n" for line in [
        {"op": "rekind", "article": a, "kind": "doi", "value": "10.1016/j.ajog.2011.02.050"},
        {"op": "remove", "article": a, "kind": "alternate_doi",
         "value": "10.1016/j.ajog.2011.02.050"},
        {"op": "add", "article": a, "kind": "alternate_doi",
         "value": "10.1016/j.ajog.2011.02.050"},
    ]), encoding="utf-8")
    before = _db(tmp_path)
    result = _invoke(config, plan)
    assert result.exit_code == 2
    assert "comes back to" in result.output and a in result.output
    assert _db(tmp_path) == before
