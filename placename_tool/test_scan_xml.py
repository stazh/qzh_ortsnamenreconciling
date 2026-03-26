import tempfile
import unittest
from pathlib import Path
import sys


sys.path.insert(0, str(Path(__file__).parent))
import xml_scan as placename_scan


class ScanXmlFilesTests(unittest.TestCase):
    def write_xml(self, directory, name, content):
        path = Path(directory) / name
        path.write_text(content, encoding="utf-8")
        return path

    def test_scan_xml_files_only_matches_placeholder(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            self.write_xml(
                tmp_dir,
                "sample.xml",
                (
                    '<p><placeName ref="LOC_Lat_Long">OpenPlace</placeName></p>\n'
                    '<p><placeName ref="LOC_47.27938_8.76205">ResolvedPlace</placeName></p>\n'
                ),
            )

            place_names = placename_scan.scan_xml_files(tmp_dir)

        self.assertEqual(set(place_names), {"OpenPlace"})
        self.assertEqual(place_names["OpenPlace"][0]["file"], "sample.xml")

    def test_scan_xml_files_keeps_multiple_placeholder_hits_on_same_line(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            self.write_xml(
                tmp_dir,
                "same_line.xml",
                (
                    '<p><placeName ref="LOC_Lat_Long">Alpha</placeName> und '
                    '<placeName ref="LOC_Lat_Long">Beta</placeName></p>\n'
                ),
            )

            place_names = placename_scan.scan_xml_files(tmp_dir)

        self.assertEqual(set(place_names), {"Alpha", "Beta"})
        self.assertEqual(place_names["Alpha"][0]["line"], 1)
        self.assertEqual(place_names["Beta"][0]["line"], 1)

    def test_scan_xml_files_triples_context_window_and_adds_ellipses_when_trimmed(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            prefix = "A" * 500
            suffix = "B" * 500
            self.write_xml(
                tmp_dir,
                "long_context.xml",
                "  " + prefix + '<placeName ref="LOC_Lat_Long">Gamma</placeName>' + suffix + "  \n",
            )

            place_names = placename_scan.scan_xml_files(tmp_dir)

        context = place_names["Gamma"][0]["context"]
        marked_tag = (
            "<mark class='bg-yellow-500/30 text-yellow-200 px-1 rounded'>"
            '<placeName ref="LOC_Lat_Long">Gamma</placeName>'
            "</mark>"
        )
        prefix_part, suffix_part = context.split(marked_tag)

        self.assertEqual(prefix_part, "..." + ("A" * placename_scan.CONTEXT_CHARS))
        self.assertEqual(suffix_part, ("B" * placename_scan.CONTEXT_CHARS) + "...")

    def test_scan_xml_files_does_not_add_ellipses_at_line_boundaries(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            self.write_xml(
                tmp_dir,
                "edge_context.xml",
                '<placeName ref="LOC_Lat_Long">Edge</placeName> nah dran\n',
            )

            place_names = placename_scan.scan_xml_files(tmp_dir)

        context = place_names["Edge"][0]["context"]

        self.assertFalse(context.startswith("..."))
        self.assertFalse(context.endswith("..."))


if __name__ == "__main__":
    unittest.main()
