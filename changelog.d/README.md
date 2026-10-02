# Changelog fragments

Each pull request adds one file here, named after its issue, such as `305.md`, and doesn't
edit `CHANGELOG.md`. No two pull requests touch the same file, so neither conflicts with the
other.

A fragment is a heading followed by its entry, written exactly as it will appear in
`CHANGELOG.md`:

```markdown
### Fixed

- **A pushed `LIMIT` could stop reading too early.** Cursor and link-header pagination
  stopped after `ceil(limit / max-page-size)` pages, assuming full pages, so `LIMIT 2` over
  one-record pages returned one row (#298).
```

- **Headings**, in the order the changelog uses: `Breaking`, `Added`, `Fixed`, `Removed`,
  `Changed`, `Deprecated`, `Security`, `Known limitations`. A fragment can hold more than one.
- **Leave a blank line** after each heading.
- **Continue an entry** on lines indented by two spaces. Nested lists and blank lines within
  an entry are fine, as long as what follows them is indented.
- **Reference the issue** as `(#305)`, as the rest of the changelog does.

`python .github/scripts/release_changelog.py --check` validates the fragments, and CI runs it.
`--preview` prints what a release would add. The release pull request runs the script with
the version and date, for example `release_changelog.py 1.1.0 2026-11-02`, which folds every
fragment into a new section of `CHANGELOG.md` and deletes them.
