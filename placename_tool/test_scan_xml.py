import re
import sys
import tempfile
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent))
import xml_scan as placename_scan


def mark(text):
    return f"<mark class='{placename_scan.MARK_CLASS}'>{text}</mark>"


class ScanXmlFilesTests(unittest.TestCase):
    def write_xml(self, directory, name, body):
        path = Path(directory) / name
        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<TEI xmlns="http://www.tei-c.org/ns/1.0">\n'
            "  <text>\n"
            "    <body>\n"
            f"{body}\n"
            "    </body>\n"
            "  </text>\n"
            "</TEI>\n"
        )
        path.write_text(xml, encoding="utf-8")
        return path

    def test_scan_xml_files_only_matches_placeholder(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            self.write_xml(
                tmp_dir,
                "sample.xml",
                (
                    '      <p><placeName ref="LOC_Lat_Long">OpenPlace</placeName></p>\n'
                    '      <p><placeName ref="LOC_47.27938_8.76205">ResolvedPlace</placeName></p>'
                ),
            )

            place_names = placename_scan.scan_xml_files(tmp_dir)

        self.assertEqual(set(place_names), {"OpenPlace"})
        self.assertEqual(place_names["OpenPlace"][0]["file"], "sample.xml")
        self.assertEqual(place_names["OpenPlace"][0]["line"], 5)

    def test_scan_xml_files_keeps_multiple_placeholder_hits_on_same_line(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            self.write_xml(
                tmp_dir,
                "same_line.xml",
                '      <p><placeName ref="LOC_Lat_Long">Alpha</placeName> und <placeName ref="LOC_Lat_Long">Beta</placeName></p>',
            )

            place_names = placename_scan.scan_xml_files(tmp_dir)

        self.assertEqual(set(place_names), {"Alpha", "Beta"})
        self.assertEqual(place_names["Alpha"][0]["line"], 5)
        self.assertEqual(place_names["Beta"][0]["line"], 5)

    def test_scan_xml_files_returns_plain_text_context_across_lines(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            self.write_xml(
                tmp_dir,
                "context.xml",
                (
                    "      <p>Vorheriger Satz.</p>\n"
                    '      <head><persName>Heini Frygman</persName> von <lb/><placeName ref="LOC_Lat_Long">Adlischwyls</placeName> antwurt</head>\n'
                    "      <p><seg><lb/>Er ward uff syn trungenliche pitt uff hoffnung der besserung.</seg></p>"
                ),
            )

            place_names = placename_scan.scan_xml_files(tmp_dir)

        context = place_names["Adlischwyls"][0]["context"]
        plain_text = re.sub(r"</?mark[^>]*>", "", context)

        self.assertIn("Vorheriger Satz.", plain_text)
        self.assertIn("Heini Frygman von Adlischwyls antwurt", plain_text)
        self.assertIn("Er ward uff syn trungenliche pitt uff hoffnung der besserung.", plain_text)
        self.assertNotRegex(plain_text, r"<[^>]+>")
        self.assertIn(mark("Adlischwyls"), context)
        self.assertEqual(place_names["Adlischwyls"][0]["line"], 6)

    def test_scan_xml_files_handles_break_no_without_inserting_space(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            self.write_xml(
                tmp_dir,
                "break_no.xml",
                '      <p>zu <placeName ref="LOC_Lat_Long">Wellen<lb break="no"/>berg</placeName> gegangen</p>',
            )

            place_names = placename_scan.scan_xml_files(tmp_dir)

        self.assertIn("Wellenberg", place_names)
        occurrence = place_names["Wellenberg"][0]
        self.assertEqual(occurrence["exact_name"], "Wellenberg")
        self.assertIn(f"zu {mark('Wellenberg')} gegangen", occurrence["context"])

    def test_scan_xml_files_triples_context_window_and_adds_ellipses_when_trimmed(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            prefix = "A" * 500
            suffix = "B" * 500
            self.write_xml(
                tmp_dir,
                "long_context.xml",
                f"      <p>{prefix}<placeName ref=\"LOC_Lat_Long\">Gamma</placeName>{suffix}</p>",
            )

            place_names = placename_scan.scan_xml_files(tmp_dir)

        context = place_names["Gamma"][0]["context"]
        prefix_part, suffix_part = context.split(mark("Gamma"))

        self.assertEqual(prefix_part, "..." + ("A" * placename_scan.CONTEXT_CHARS))
        self.assertEqual(suffix_part, ("B" * placename_scan.CONTEXT_CHARS) + "...")

    def test_replace_placeholder_refs_updates_split_place_names(self):
        content = (
            '<p><placeName ref="LOC_Lat_Long">Wellen<lb break="no"/>berg</placeName></p>'
            '<p><placeName ref="LOC_47.0_8.0">Resolved</placeName></p>'
        )
        resolved_places = {
            "Wellenberg": {
                "lat": 47.12345,
                "lng": 8.54321,
            }
        }

        updated = placename_scan.replace_placeholder_refs(content, resolved_places)

        self.assertIn('ref="LOC_47.12345_8.54321"', updated)
        self.assertIn('Wellen<lb break="no"/>berg</placeName>', updated)
        self.assertIn('ref="LOC_47.0_8.0"', updated)


if __name__ == "__main__":
    unittest.main()
