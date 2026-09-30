"""Parse the GHCN-Daily `readme.txt` data dictionary into machine-readable metadata.

The readme is the authoritative spec for:
  * fixed-width column layouts of the reference files (section IV-VII tables),
  * element codes, descriptions and units (section III, "ELEMENT"),
  * measurement / quality / source flag definitions (section III, "MFLAG1" ...).

Deriving these from the dictionary (instead of hand-copying them into code) keeps
the pipeline metadata-driven: a new element or a changed layout shows up in the
warehouse without a code change, and a *structural* change to the readme fails
loudly here rather than silently mis-slicing data downstream.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final

_SECTION_FORMAT_RE: Final = re.compile(r'^[IVX]+\.\s+FORMAT OF "(?P<file>[^"]+)"')
_LAYOUT_ROW_RE: Final = re.compile(
    r"^(?P<name>[A-Z][A-Z /]*?)\s+(?P<start>\d+)-\s*(?P<end>\d+)\s+"
    r"(?P<type>Character|Integer|Real)\s*$"
)
_ELEMENT_RE: Final = re.compile(r"^\s+(?P<code>[A-Z][A-Z0-9*#]{3}) = (?P<text>.+)$")
_SUBCODE_RE: Final = re.compile(r"^\s+(?P<code>\d{2}) = (?P<text>.+)$")
_FLAG_RE: Final = re.compile(r"^\s+(?P<code>Blank|[A-Za-z0-9])\s+= (?P<text>.+)$")
_UNIT_RE: Final = re.compile(r"\((?P<unit>[^()]*)\)")
_FLAG_SECTIONS: Final = {"MFLAG1": "M", "QFLAG1": "Q", "SFLAG1": "S"}
# Families whose two-digit sub-codes are listed beneath the family header.
_EXPANDABLE_FAMILIES: Final = {"WT**": "Weather type", "WV**": "Weather in the vicinity"}

REQUIRED_LAYOUTS: Final = {
    "ghcnd-stations.txt": {"id", "latitude", "longitude", "elevation", "state", "name", "wmo_id"},
    "ghcnd-inventory.txt": {"id", "element", "firstyear", "lastyear"},
    "ghcnd-countries.txt": {"code", "name"},
}


class ReadmeParseError(ValueError):
    """The readme no longer matches the structure this parser understands."""


@dataclass(frozen=True, slots=True)
class LayoutColumn:
    name: str
    start: int  # 1-based, inclusive (as documented)
    end: int  # 1-based, inclusive
    data_type: str

    @property
    def length(self) -> int:
        return self.end - self.start + 1


@dataclass(frozen=True, slots=True)
class ElementDefinition:
    element: str
    description: str
    unit_text: str | None


@dataclass(frozen=True, slots=True)
class FlagDefinition:
    flag_type: str  # M | Q | S
    code: str  # '' represents "Blank"
    description: str


@dataclass(frozen=True, slots=True)
class ReadmeMetadata:
    layouts: dict[str, list[LayoutColumn]]
    elements: list[ElementDefinition]
    flags: list[FlagDefinition]


def _normalise_column_name(raw: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", raw.strip().lower()).strip("_")


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def parse_layouts(lines: list[str]) -> dict[str, list[LayoutColumn]]:
    """Extract every `Variable / Columns / Type` table keyed by the file it describes."""
    layouts: dict[str, list[LayoutColumn]] = {}
    current: str | None = None
    for line in lines:
        header = _SECTION_FORMAT_RE.match(line)
        if header:
            current = header["file"]
            continue
        if current and (row := _LAYOUT_ROW_RE.match(line)):
            layouts.setdefault(current, []).append(
                LayoutColumn(
                    name=_normalise_column_name(row["name"]),
                    start=int(row["start"]),
                    end=int(row["end"]),
                    data_type=row["type"].lower(),
                )
            )
    for file_name, required in REQUIRED_LAYOUTS.items():
        found = {c.name for c in layouts.get(file_name, [])}
        if missing := required - found:
            raise ReadmeParseError(f"Layout for {file_name} is missing columns {sorted(missing)}")
    return layouts


def _extract_unit(description: str) -> str | None:
    match = _UNIT_RE.search(description)
    if not match:
        return None
    # "(tenths of mm; use with DAEV)" -> "tenths of mm"
    return _clean(match["unit"].split(";")[0]) or None


def _definition_block(lines: list[str], variable: str, next_variable: str) -> list[str]:
    """Lines of the prose definition `<VARIABLE>   is the ...` up to the next variable."""
    start_re = re.compile(rf"^{variable}\s+is ")
    end_re = re.compile(rf"^{next_variable}\s+is ")
    try:
        start = next(i for i, ln in enumerate(lines) if start_re.match(ln))
        end = next(i for i in range(start + 1, len(lines)) if end_re.match(lines[i]))
    except StopIteration as exc:
        raise ReadmeParseError(f"Could not find definition of {variable} in readme") from exc
    return lines[start:end]


def parse_elements(lines: list[str]) -> list[ElementDefinition]:
    block = _definition_block(lines, "ELEMENT", "VALUE1")
    elements: dict[str, ElementDefinition] = {}
    family: str | None = None  # e.g. "WT**" while reading its two-digit sub-codes
    code: str | None = None  # definition currently being accumulated
    parts: list[str] = []

    def flush() -> None:
        nonlocal code, parts
        if code and code not in _EXPANDABLE_FAMILIES:
            text = _clean(" ".join(parts))
            if family and code.startswith(family[:2]):
                elements[code] = ElementDefinition(
                    code, f"{_EXPANDABLE_FAMILIES[family]}: {text}", "presence flag"
                )
            else:
                unit = _extract_unit(text)
                # keep the human text before the unit parenthesis as the description
                label = _clean(text.split("(")[0]) if unit else text
                elements[code] = ElementDefinition(code, label, unit)
        code, parts = None, []

    for line in block[1:]:
        stripped = line.strip()
        if m := _ELEMENT_RE.match(line):
            flush()
            code, parts = m["code"], [m["text"]]
            family = code if code in _EXPANDABLE_FAMILIES else None
        elif family and (sub := _SUBCODE_RE.match(line)):
            flush()
            code, parts = family[:2] + sub["code"], [sub["text"]]
        elif (
            not stripped
            or stripped.startswith(("[", "where", "Ground", "Depth"))
            or re.match(r"^\d+ = ", stripped)
        ):
            # blank line, bracketed note or soil-code legend terminates a definition
            flush()
        elif code:
            parts.append(stripped)
    flush()

    if not {"PRCP", "SNOW", "SNWD", "TMAX", "TMIN"} <= elements.keys():
        raise ReadmeParseError("Core elements (PRCP, SNOW, SNWD, TMAX, TMIN) not found in readme")
    return sorted(elements.values(), key=lambda e: e.element)


def parse_flags(lines: list[str]) -> list[FlagDefinition]:
    flags: list[FlagDefinition] = []
    active: str | None = None
    last: FlagDefinition | None = None
    for line in lines:
        section = next((t for k, t in _FLAG_SECTIONS.items() if line.startswith(k)), None)
        if section:
            active, last = section, None
            continue
        if active is None:
            continue
        if line and not line[0].isspace():  # next top-level variable ends the section
            active, last = None, None
            continue
        if m := _FLAG_RE.match(line):
            code = "" if m["code"] == "Blank" else m["code"]
            last = FlagDefinition(active, code, _clean(m["text"]))
            flags.append(last)
        elif last and line.strip() and not line.strip().startswith("When data"):
            flags[-1] = FlagDefinition(
                last.flag_type, last.code, _clean(f"{last.description} {line}")
            )
            last = flags[-1]
        else:
            last = None
    if {f.flag_type for f in flags} != {"M", "Q", "S"}:
        raise ReadmeParseError("Did not find all of MFLAG/QFLAG/SFLAG definitions")
    return flags


def parse_readme(text: str) -> ReadmeMetadata:
    lines = [ln.expandtabs(8).rstrip() for ln in text.splitlines()]
    return ReadmeMetadata(
        layouts=parse_layouts(lines),
        elements=parse_elements(lines),
        flags=parse_flags(lines),
    )
