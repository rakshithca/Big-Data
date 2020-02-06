from mrjob.job import MRJob

class MRMovieRating(MRJob):
    def mapper(self, _, line):
		(userID,movieID,movieRate,timeStamp) = line.split(',')
		yield userID, 1
    def combiner(self, userID, counts):
        yield userID, sum(counts)
    def reducer(self, userID, counts):
        yield userID, sum(counts)
if __name__ == '__main__':
    MRMovieRating.run()


