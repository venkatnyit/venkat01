from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"\b\w+\b")

class InvertedIndex(MRJob):

    def mapper(self, _, line):
        # Split the line into words
        words = WORD_RE.findall(line.lower())

        # Extract the document identifier (e.g., "Document 1")
        doc_id = re.match(r'Document (\d+):', line)
        if doc_id:
            doc_id = doc_id.group(1)
        else:
            doc_id = "Unknown"  # Assign a default identifier if not found

        # Iterate through the words and emit word-document pairs
        for word in words:
            yield word, doc_id

    def reducer(self, word, doc_ids):
        # Combine the list of document identifiers into a comma-separated string
        document_list = ", ".join(sorted(set(doc_ids)))
        yield word, document_list

if __name__ == '__main__':
    InvertedIndex.run()