import glob
import os
import re


PLACEHOLDER = "LOC_Lat_Long"
CONTEXT_CHARS = 450


def scan_xml_files(xml_dir):
    """Scan all XML files and return place names with LOC_Lat_Long."""
    pattern = os.path.join(xml_dir, "*.xml")
    files = sorted(glob.glob(pattern))
    place_names = {}  # {text: [{"file": ..., "line": ...}, ...]}
    placeholder_pattern = re.compile(
        rf'<placeName\s+ref="{re.escape(PLACEHOLDER)}">([^<]+)</placeName>'
    )

    for filepath in files:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
                    clean_line = line.strip()
                    for match in placeholder_pattern.finditer(clean_line):
                        name = match.group(1).strip()
                        if name not in place_names:
                            place_names[name] = []

                        start, end = match.span()
                        context_start = max(0, start - CONTEXT_CHARS)
                        context_end = min(len(clean_line), end + CONTEXT_CHARS)

                        context_prefix = ("..." if context_start > 0 else "") + clean_line[context_start:start]
                        context_suffix = clean_line[end:context_end] + ("..." if context_end < len(clean_line) else "")

                        place_names[name].append({
                            "file": os.path.basename(filepath),
                            "line": line_num,
                            "context": (
                                f"{context_prefix}<mark class='bg-yellow-500/30 text-yellow-200 px-1 rounded'>"
                                f"{match.group(0)}</mark>{context_suffix}"
                            ),
                            "exact_name": name,
                        })
        except Exception as e:
            print(f"Error reading {filepath}: {e}")

    return place_names
