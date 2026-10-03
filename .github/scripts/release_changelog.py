"""Folds the changelog fragments in changelog.d/ into CHANGELOG.md (#304).

Each pull request adds changelog.d/<issue>.md instead of editing CHANGELOG.md, so no two pull
requests touch the same file and none conflicts with another. A fragment is a heading followed
by its entry, written exactly as it will appear in CHANGELOG.md:

    ### Fixed

    - **What changed, in a phrase.** Why it mattered, and what to do about it (#123).

Usage:

    python .github/scripts/release_changelog.py --check            # fragments are well formed
    python .github/scripts/release_changelog.py --preview          # print what a release would add
    python .github/scripts/release_changelog.py 1.1.0 2026-11-02   # write the release, delete fragments

The release form puts every fragment, and anything under an `## [Unreleased]` section if there
is one, into a new `## [1.1.0] - 2026-11-02` section above the latest release, grouped by
heading in the order this changelog uses. Then it deletes the fragments. Run it in the release
pull request and commit the result.

Unlike Flare's version, this changelog references issues as plain `(#N)` and keeps no link
definitions or compare links, so there are none to add.
"""
import datetime
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FRAGMENTS = ROOT / "changelog.d"
CHANGELOG = ROOT / "CHANGELOG.md"
# The order this changelog's sections use; Keep a Changelog's own, plus Breaking first and
# Known limitations last.
ORDER = ["Breaking", "Added", "Fixed", "Removed", "Changed", "Deprecated", "Security", "Known limitations"]


def fragment_files():
    return sorted(p for p in FRAGMENTS.glob("*.md") if p.name != "README.md")


def parse(text, source):
    """Entries by heading.

    An entry is a `- ` line with text on it, plus everything after it up to the next entry or
    heading: lines indented by two spaces or more (continuations and nested lists) and the
    blank lines between them. Blank lines at an entry's end are dropped.
    """
    sections, current, errors = {}, None, []
    for n, line in enumerate(text.rstrip("\n").split("\n"), 1):
        heading = re.match(r"^### (.+?)\s*$", line)
        if heading:
            current = heading.group(1)
            if current not in ORDER:
                errors.append(f"{source}:{n}: unknown heading '{current}', use one of {', '.join(ORDER)}")
            sections.setdefault(current, [])
        elif line.startswith("- ") or line.rstrip() == "-":
            if current is None:
                errors.append(f"{source}:{n}: entry before any '### Heading'")
            elif not line[1:].strip():
                errors.append(f"{source}:{n}: empty entry, the text starts on the '- ' line")
            else:
                sections[current].append(line)
        elif not line.strip():
            if current and sections[current]:
                sections[current][-1] += "\n"
        elif line.startswith("  ") and current and sections[current]:
            sections[current][-1] += "\n" + line
        else:
            errors.append(f"{source}:{n}: not a heading, an entry or an indented continuation: {line[:60]}")
    for heading in sections:
        sections[heading] = [re.sub(r"\n+$", "", e) for e in sections[heading]]
    return sections, errors


def load_fragments():
    merged, errors = {}, []
    for path in fragment_files():
        sections, errs = parse(path.read_text(encoding="utf-8"), path.name)
        errors += errs
        if not any(sections.values()):
            errors.append(f"{path.name}: no entries")
        for heading, entries in sections.items():
            merged.setdefault(heading, []).extend(entries)
    return merged, errors


def split_unreleased(text):
    """(head, entries under `## [Unreleased]`, the rest from the latest release on).

    Release mode folds those entries in and drops the section; preview shows them, so it shows
    everything a release would add.
    """
    first = re.search(r"^## \[", text, re.M)
    if not first:
        sys.exit("CHANGELOG.md has no '## [' section to release above")
    head, rest = text[:first.start()], text[first.start():]
    unreleased = re.match(r"## \[Unreleased\][^\n]*\n(.*?)(?=^## \[)", rest, re.S | re.M)
    if not unreleased:
        return head, {}, rest
    entries, errors = parse(unreleased.group(1), "CHANGELOG.md [Unreleased]")
    if errors:
        sys.exit("\n".join(errors))
    return head, entries, rest[unreleased.end():]


def render(sections):
    """Sections in this changelog's order, a blank line after each heading, entries together."""
    return "\n\n".join(f"### {h}\n\n" + "\n".join(sections[h]) for h in ORDER if sections.get(h))


def main(argv):
    fragments, errors = load_fragments()
    if errors:
        sys.exit("\n".join(errors))

    if argv == ["--check"]:
        print(f"{len(fragment_files())} changelog fragment(s), well formed")
        return

    text = CHANGELOG.read_text(encoding="utf-8")
    head, existing, rest = split_unreleased(text)
    for heading, entries in fragments.items():
        existing.setdefault(heading, []).extend(entries)

    if argv == ["--preview"]:
        print(render(existing) or "(no fragments and no [Unreleased] entries)")
        return
    if len(argv) != 2 or not re.fullmatch(r"\d+\.\d+\.\d+", argv[0]) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", argv[1]):
        sys.exit(__doc__)
    version, date = argv
    try:
        datetime.date.fromisoformat(date)
    except ValueError:
        sys.exit(f"{date} is not a calendar date")
    if re.search(rf"^## \[{re.escape(version)}\]", text, re.M):
        sys.exit(f"CHANGELOG.md already has a [{version}] section")
    if not any(existing.values()):
        sys.exit("nothing to release: no fragments and no [Unreleased] entries")

    result = head + f"## [{version}] - {date}\n\n" + render(existing) + "\n\n" + rest
    with open(CHANGELOG, "w", encoding="utf-8", newline="\n") as f:
        f.write(result)
    for path in fragment_files():
        path.unlink()
    print(f"released {version}: {sum(len(v) for v in existing.values())} entries; "
          "fragments deleted, commit CHANGELOG.md and changelog.d/")


if __name__ == "__main__":
    main(sys.argv[1:])
