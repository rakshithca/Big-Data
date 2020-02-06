from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")
class MRWordCount(MRJob):
	def mapper(self, _,line):
		letters = 'abcdefghijklmn'
		for word in WORD_RE.findall(line):
			if any(word.startswith(i) for i in letters):
				yield 'a_to_n',1
			else:
				yield 'other',1
	def combiner(self,word,counts):
		yield word,sum(counts)
	def reducer(self,word,counts):
		yield word, sum(counts)
if __name__ == '__main__':
	MRWordCount.run()