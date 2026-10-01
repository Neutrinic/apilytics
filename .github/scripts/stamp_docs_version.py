"""Sets the APIlytics version in the documentation's snippets, the only place the docs name it.

    python .github/scripts/stamp_docs_version.py v1.0.0

The docs workflow runs this on a release tag before building, so the published site always
names the release it was built from. Run it in a release PR too, so the repository matches.

It fails rather than succeed quietly: if the snippets' format drifts so that no version is
found, or any version other than the new one is left afterwards, the site would name a stale
release.
"""
import re
import sys
from pathlib import Path

tag = sys.argv[1] if len(sys.argv) > 1 else ""
if not re.fullmatch(r"v\d+\.\d+\.\d+", tag):
    sys.exit(f"not a release tag: {tag!r}")
version = tag[1:]

SNIPPETS = Path(__file__).resolve().parents[2].joinpath("docs", "snippets")
V = r"\d+\.\d+\.\d+"

replacements = [
    # Maven coordinates: io.github.neutrinic:apilytics_2.13:1.0.0
    (re.compile(rf"(io\.github\.neutrinic:apilytics_2\.13):{V}"), rf"\g<1>:{version}"),
    # sbt: "io.github.neutrinic" %% "apilytics" % "1.0.0"
    (re.compile(rf'("apilytics" % "){V}(")'), rf"\g<1>{version}\g<2>"),
    # The release the docs describe, in prose: **APIlytics 1.0.0**
    (re.compile(rf"(\*\*APIlytics ){V}(\*\*)"), rf"\g<1>{version}\g<2>"),
]
# Every APIlytics version the snippets name, in any of the forms above.
any_version = re.compile(
    rf'io\.github\.neutrinic:apilytics_2\.13:({V})|"apilytics" % "({V})"|\*\*APIlytics ({V})\*\*')

changed = 0
for path in sorted(SNIPPETS.iterdir()):
    text = path.read_text(encoding="utf-8")
    new = text
    for pattern, replacement in replacements:
        new = pattern.sub(replacement, new)
    if new != text:
        with open(path, "w", encoding="utf-8", newline="\n") as fh:  # LF on every platform
            fh.write(new)
        changed += 1

found = [v for path in sorted(SNIPPETS.iterdir())
         for m in any_version.finditer(path.read_text(encoding="utf-8"))
         for v in m.groups() if v]
stale = sorted({v for v in found if v != version})
if not found:
    sys.exit("no APIlytics version found in docs/snippets: the patterns no longer match the snippets")
if stale:
    sys.exit(f"docs/snippets still name {', '.join(stale)} after stamping {version}")
print(f"stamped {version} into {changed} snippet file(s); all {len(found)} versions now read {version}")
