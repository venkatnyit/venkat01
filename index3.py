from mrjob.job import MRJob
import re

class MRWordBigramCounter(MRJob):

    def mapper(self, _, line):
        # Split the line into words using whitespace and remove punctuation
        words = re.findall(r'\b\w+\b', line.lower())

        # Emit word bigrams as key-value pairs
        for i in range(len(words) - 1):
            bigram = words[i] + "," + words[i + 1]
            yield bigram, 1

    def combiner(self, bigram, counts):
        # Sum the counts for each bigram locally
        total_count = sum(counts)
        yield bigram, total_count

    def reducer(self, bigram, counts):
        # Sum the counts for each bigram globally
        total_count = sum(counts)
        yield bigram, total_count

if __name__ == '__main__':
    MRWordBigramCounter.run()
