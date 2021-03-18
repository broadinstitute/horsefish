"""Unitest for Upload GATK Tools Docs."""
import io
import unittest
from GatkDocs import GatkDocs
from upload_gatk_tools_docs import clean_html_file


class TestUploadGATKToolsDocs(unittest.TestCase):
    """Unitest for Upload GATK Tools Docs."""

    def test_gatk_docs_dict(self):
        """Test if the gatk_docs_dict that holds all the GATK Docs information still works."""
        test_gatk_docs_dict = {'index.html': GatkDocs(title='* Tool Documentation Index', file_name='index.html', local_path='scripts/upload_gatk_docs/gatkdoc_clean_test/index.html', url=None, article_id=None),
                               'unfiltered_Coverage.html': GatkDocs(title='Coverage', file_name='unfiltered_Coverage.html', local_path='scripts/upload_gatk_docs/gatkdoc_clean_test/unfiltered_Coverage.html', url=None, article_id=None),
                               'unfiltered_AlignmentReadFilter.html': GatkDocs(title='AlignmentAgreesWithHeaderReadFilter', file_name='unfiltered_AlignmentReadFilter.html', local_path='scripts/upload_gatk_docs/gatkdoc_clean_test/unfiltered_AlignmentReadFilter.html', url=None, article_id=None),
                               'unfiltered_GenotypeSummaries.html': GatkDocs(title='GenotypeSummaries', file_name='unfiltered_GenotypeSummaries.html', local_path='scripts/upload_gatk_docs/gatkdoc_clean_test/unfiltered_GenotypeSummaries.html', url=None, article_id=None)}
        gatk_docs_dict = clean_html_file("test", "scripts/upload_gatk_docs/test_unfiltered_html", 'scripts/upload_gatk_docs/gatkdoc_clean_test')
        self.assertEqual(test_gatk_docs_dict, gatk_docs_dict, "The GATK Docs Dicts don't match")

    def test_clean_html_file(self):
        """Test the clean_html_file fuction."""
        test_gatk_docs_dict = clean_html_file("test", "scripts/upload_gatk_docs/test_unfiltered_html", 'scripts/upload_gatk_docs/gatkdoc_clean_test')
        gatk_docs = test_gatk_docs_dict.values()
        test_file_paths = ["scripts/upload_gatk_docs/test_clean_html/clean_index.html",
                           "scripts/upload_gatk_docs/test_clean_html/clean_Coverage.html",
                           "scripts/upload_gatk_docs/test_clean_html/clean_AlignmentReadFilter.html",
                           "scripts/upload_gatk_docs/test_clean_html/clean_GenotypeSummaries.html"]
        index = 0
        for gatk_doc in gatk_docs:
            if index != -1:
                self.assertEqual(list(io.open(gatk_doc.local_path)), list(io.open(test_file_paths[index])), "The " + gatk_doc.title + " file don't match")
                # print(filecmp(gatk_doc.local_path, test_file_paths[index], shallow=False))
                # self.assertTrue(filecmp(gatk_doc.local_path, test_file_paths[index], shallow=False), "The " + gatk_doc.title + " file don't match")
            index = index + 1


if __name__ == "__main__":
    unittest.main()
