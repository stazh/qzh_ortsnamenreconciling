import glob
import html
import os
import re
import xml.etree.ElementTree as ET


PLACEHOLDER = "LOC_Lat_Long"
CONTEXT_CHARS = 450
MARK_CLASS = "bg-yellow-500/30 text-yellow-200 px-1 rounded"
PLACEHOLDER_OPEN_TAG_PATTERN = re.compile(
    rf'<placeName\b[^>]*\bref="{re.escape(PLACEHOLDER)}"[^>]*>',
    re.DOTALL,
)
PLACEHOLDER_PLACE_NAME_PATTERN = re.compile(
    rf'(<placeName\b[^>]*\bref="){re.escape(PLACEHOLDER)}("[^>]*>)(.*?)</placeName>',
    re.DOTALL,
)
BREAK_NO_LB_TAG_PATTERN = re.compile(r"<lb\b[^>]*\bbreak=\"no\"[^>]*/>", re.DOTALL)
SEPARATOR_TAG_PATTERN = re.compile(r"</?(?:lb|pb|cb)\b[^>]*>", re.DOTALL)
GENERIC_TAG_PATTERN = re.compile(r"<[^>]+>", re.DOTALL)
BLOCK_SEPARATOR_TAGS = {
    "ab",
    "body",
    "closer",
    "dateline",
    "div",
    "head",
    "item",
    "l",
    "list",
    "note",
    "opener",
    "p",
    "row",
    "seg",
    "table",
    "title",
    "titlePart",
}
SEPARATOR_TAGS = {"cb", "lb", "pb"}
CHOICE_PREFERRED_CHILDREN = ("expan", "corr", "reg", "lem", "orig", "sic", "abbr")
SIBLING_RENDER_PREFERENCES = {
    ("abbr", "expan"): 1,
    ("sic", "corr"): 1,
    ("orig", "reg"): 0,
}


class RawTextBuilder:
    def __init__(self):
        self.parts = []
        self.length = 0

    def append(self, text):
        if not text:
            return
        self.parts.append(text)
        self.length += len(text)

    def build(self):
        return "".join(self.parts)


def local_name(tag):
    if not isinstance(tag, str):
        return ""
    return tag.rsplit("}", 1)[-1]


def append_visible_text(builder, text):
    if text:
        builder.append(text)


def render_choice(element, builder, occurrences):
    chosen = None
    children = list(element)
    for preferred in CHOICE_PREFERRED_CHILDREN:
        chosen = next((child for child in children if local_name(child.tag) == preferred), None)
        if chosen is not None:
            break
    if chosen is None and children:
        chosen = children[0]
    if chosen is None:
        append_visible_text(builder, element.text)
        return

    append_visible_text(builder, element.text)
    render_element(chosen, builder, occurrences)
    append_visible_text(builder, chosen.tail)


def render_children(element, builder, occurrences):
    children = list(element)
    index = 0
    while index < len(children):
        child = children[index]
        next_child = children[index + 1] if index + 1 < len(children) else None
        current_name = local_name(child.tag)
        next_name = local_name(next_child.tag) if next_child is not None else None

        pair_choice = SIBLING_RENDER_PREFERENCES.get((current_name, next_name))
        if pair_choice is not None:
            chosen_child = child if pair_choice == 0 else next_child
            render_element(chosen_child, builder, occurrences)
            append_visible_text(builder, chosen_child.tail)
            index += 2
            continue

        render_element(child, builder, occurrences)
        append_visible_text(builder, child.tail)
        index += 1


def render_element(element, builder, occurrences):
    tag = local_name(element.tag)

    if tag == "choice":
        render_choice(element, builder, occurrences)
        return

    if tag == "lb":
        if element.attrib.get("break") != "no":
            builder.append(" ")
        return

    if tag in SEPARATOR_TAGS:
        builder.append(" ")
        return

    is_placeholder = tag == "placeName" and element.attrib.get("ref") == PLACEHOLDER
    if is_placeholder:
        raw_start = builder.length

    append_visible_text(builder, element.text)
    render_children(element, builder, occurrences)

    if tag in BLOCK_SEPARATOR_TAGS:
        builder.append(" ")

    if is_placeholder:
        raw_end = builder.length
        occurrences.append({
            "raw_start": raw_start,
            "raw_end": raw_end,
        })


def normalize_raw_text(raw_text):
    normalized_parts = []
    raw_to_normalized = [0] * (len(raw_text) + 1)
    normalized_length = 0
    pending_space = False

    for index, char in enumerate(raw_text):
        raw_to_normalized[index] = normalized_length
        if char.isspace():
            if normalized_length > 0:
                pending_space = True
        else:
            if pending_space and normalized_length > 0:
                normalized_parts.append(" ")
                normalized_length += 1
            pending_space = False
            normalized_parts.append(char)
            normalized_length += 1
        raw_to_normalized[index + 1] = normalized_length

    return "".join(normalized_parts), raw_to_normalized


def build_context(normalized_text, start, end):
    context_start = max(0, start - CONTEXT_CHARS)
    context_end = min(len(normalized_text), end + CONTEXT_CHARS)

    prefix = ("..." if context_start > 0 else "") + html.escape(normalized_text[context_start:start])
    highlight = html.escape(normalized_text[start:end])
    suffix = html.escape(normalized_text[end:context_end]) + ("..." if context_end < len(normalized_text) else "")

    return f"{prefix}<mark class='{MARK_CLASS}'>{highlight}</mark>{suffix}"


def normalize_fragment_text(fragment):
    fragment = BREAK_NO_LB_TAG_PATTERN.sub("", fragment)
    fragment = SEPARATOR_TAG_PATTERN.sub(" ", fragment)
    fragment = GENERIC_TAG_PATTERN.sub(" ", fragment)
    return " ".join(fragment.split())


def get_line_numbers(xml_text):
    return [
        xml_text.count("\n", 0, match.start()) + 1
        for match in PLACEHOLDER_OPEN_TAG_PATTERN.finditer(xml_text)
    ]


def collect_occurrences(xml_text):
    root = ET.fromstring(xml_text)
    builder = RawTextBuilder()
    raw_occurrences = []

    render_element(root, builder, raw_occurrences)

    normalized_text, raw_to_normalized = normalize_raw_text(builder.build())
    line_numbers = get_line_numbers(xml_text)
    occurrences = []

    for index, raw_occurrence in enumerate(raw_occurrences):
        start = raw_to_normalized[raw_occurrence["raw_start"]]
        end = raw_to_normalized[raw_occurrence["raw_end"]]

        while start < end and normalized_text[start].isspace():
            start += 1
        while end > start and normalized_text[end - 1].isspace():
            end -= 1

        exact_name = normalized_text[start:end]
        if not exact_name:
            continue

        occurrences.append({
            "line": line_numbers[index] if index < len(line_numbers) else 1,
            "context": build_context(normalized_text, start, end),
            "exact_name": exact_name,
        })

    return occurrences


def replace_placeholder_refs(content, resolved_places):
    def replace_match(match):
        normalized_name = normalize_fragment_text(match.group(3))
        coords = resolved_places.get(normalized_name)
        if not coords:
            return match.group(0)

        ref_value = f'LOC_{coords["lat"]}_{coords["lng"]}'
        return f"{match.group(1)}{ref_value}{match.group(2)}{match.group(3)}</placeName>"

    return PLACEHOLDER_PLACE_NAME_PATTERN.sub(replace_match, content)


def scan_xml_files(xml_dir):
    """Scan all XML files and return place names with LOC_Lat_Long."""
    pattern = os.path.join(xml_dir, "*.xml")
    files = sorted(glob.glob(pattern))
    place_names = {}

    for filepath in files:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                xml_text = f.read()

            for occurrence in collect_occurrences(xml_text):
                name = occurrence["exact_name"]
                if name not in place_names:
                    place_names[name] = []

                place_names[name].append({
                    "file": os.path.basename(filepath),
                    "line": occurrence["line"],
                    "context": occurrence["context"],
                    "exact_name": occurrence["exact_name"],
                })
        except Exception as e:
            print(f"Error reading {filepath}: {e}")

    return place_names
