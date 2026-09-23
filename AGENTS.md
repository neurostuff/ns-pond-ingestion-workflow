# AGENTS.md

## Where design documentation goes

Architecture, reasoning and measurements go in the [wiki]. Keep them out of
`docs/` — that directory was retired so the reasoning survives the branch
being merged.

A change earns a **wiki record** when any of these hold:

- it adds, removes or renames a package under `ingestion_workflow/`
- it changes how the catalog, the stages or the CLI fit together
- it rests on measurements someone might want to re-run
- it answers a "why is it built like this?" that the diff cannot

Everything else — a bug fix, a dependency bump, a new flag, a changed default —
belongs in the PR description alone.

For a change that earns one, the record is `PR-<n>-<Short-Name>`, plus
`PR-<n>-<Topic>` pages for anything long enough to stand on its own.
[Changes] carries the naming rules and the checklist for adding a record;
follow it rather than inventing a layout.

**Living pages** — [Design], [Data safety] and [Migration] — describe the
pipeline as it stands. A change that alters what they say edits them in place,
so there is only ever one page describing the current system.

[wiki]: https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki
[Changes]: https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/Changes
[Design]: https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/Design
[Data safety]: https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/Data-Safety
[Migration]: https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/Migration
