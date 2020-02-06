from mrjob.job import MRJob

class MRSalaries(MRJob):
    def mapper(self, _, line):
		(name,jobTitle,agencyID,agency,hireDate,annualSalary,grossPay) = line.split('\t')
		annual_sal = float(annualSalary)
		if (annual_sal >=100000.00):
			yield 'High ',1
		elif (annual_sal >= 50000.00):
			yield 'Medium',1
		else:
			yield 'Low',1
    def combiner(self, jobTitle, counts):
        yield jobTitle, sum(counts)
    def reducer(self, jobTitle, counts):
        yield jobTitle, sum(counts)
if __name__ == '__main__':
    MRSalaries.run()


