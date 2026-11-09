# Research dashboard

One page for every number Yehor reviews: **https://claude.ai/artifact/Lp6FVttHUNMFGWziGZ5uBk** (private, a claude.ai Dashboard artifact). Decision D-025.

## What goes on it

The dashboard is the review surface, not a log. A chat that produces a result worth tracking adds it there in the same session, before reporting back.

- **Belongs:** each stage's headline numbers and the 1–3 charts that carry its finding (held-out where anything is fitted, with standard errors), its Gate verdicts, and every automated check that can pass or fail.
- **Doesn't belong:** exploration, intermediate tables, anything not reproducible from a `polymm` command.
- Keep it simple for now. The set of sections will be refined as it becomes clear what is relevant; ask Yehor before removing a section someone else added.

## How it is built

```
ops/daily_refresh.sh   pull → quality + vol per new day → fair → markouts → polymm dashboard
polymm dashboard       src/polymm/dashboard.py → reports/dashboard/dashboard_data.json (one file, all sections)
scheduled task         polymm-dashboard-refresh, 07:30 Vienna: runs the script, uploads the file,
                       repoints every dataset whose source is dashboard_data.json
```

The page lives in the artifact's store, not in this repo: `files/index.html` holds the HTML and scripts, one `datasets/<key>` document per section says it reads key `<key>` of the uploaded file, and a `dash.loader('<key>', …)` line in the page picks that key out.

## Adding a section

1. **Data.** In `build()` in `src/polymm/dashboard.py`, add a key to the `data` dict: a list of flat row objects (numbers rounded, no NaN). Read from the stage's report outputs under `reports/`, never recompute research there. Add a headline field to `kpis`, and to the `history` snapshot, if it should show at the top or be tracked over time. Run `.venv/bin/polymm dashboard`.
2. **Upload.** Artifact tool: `publish` with the dashboard url, `file_path` = `reports/dashboard/dashboard_data.json`, `asset: true`. Then repoint all `dashboard_data.json` datasets to the new `/_blob/<id>`, as step 3 of the scheduled task does.
3. **Dataset.** ArtifactData `set` on `datasets/<key>`: `title`, one-sentence `description`, `source: {kind: "file", url: <the new /_blob url>, name: "dashboard_data.json", format: "json"}`, `updated: {at, by: "Claude"}`, `schedule`, `staleAfterHours: 48`, `edit: {via: "claude", at}`. The daily task picks it up automatically from then on.
4. **Page.** Read `files/index.html` first (ArtifactData `get`): someone may have changed it since. Before writing, read the type's guides with Artifact `read`, paths `artifact-type/reference/one-writer.md` and `artifact-type/reference/design-defaults.md`. Then:
   - add `dash.loader('<key>', {description: …, fn: ({answer}) => answer.<key>})` next to the others;
   - add a `<section class="pm-sec" id="…">` (eyebrow, `h2`, cards) and its drawing in the same style;
   - every number or chart has `data-source="<key>"`, `data-field`, and `data-row`/`data-where` or a table `data-row-key`;
   - static words go in the HTML, data-decided values in `data-draw` elements;
   - charts clamp every mark to their axes (the svg is clipped);
   - write with an `update` of `edit` first, then `str_replace` on the `text` field, each pinned with `if_version`.
5. **Check.** Read ArtifactData `data/users/me` on the dashboard: `runs` lists failed loads and marks whose shown value differs from the data; `check` says whether the page passed. Both appear only while someone has the page open.

Style: dark page, silver gradient title, monospace numbers and uppercase labels, pill navigation, one section per research stage. Series colours: mid `#8b8b95`, microprice `#5ab0f0`, book `#3dd6c0`, fair value `#a78bfa`, raw model `#f47c8c`, highlight `#f5c451`.
