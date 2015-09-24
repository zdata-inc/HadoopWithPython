from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

cols = 'Name,JobTitle,AgencyID,Agency,HireDate,AnnualSalary,GrossPay'.split(',')


class salaryavg(MRJob):

    def avgmapper(self, _, line):
        row = dict(zip(cols, [ a.strip() for a in csv.reader([line]).next()]))

        self.increment_counter('depts', row['Agency'], 1)

        yield row['JobTitle'], (int(float(row['AnnualSalary'][1:])), 1)

    def avgreducer(self, key, values):
        s = 0
        c = 0

        for average, count in values:
            s += average * count
            c += count

        if c > 3:
            self.increment_counter('stats', 'below3', 1)
            yield key, (s/c, c)

    def ttmapper(self, key, value):
        yield None, (value[0], key) # group by all, keep average and job title

    def ttreducer(self, key, values):
        topten = []
        for average, job in values:
            topten.append((average, job))
            topten.sort()
            topten = topten[-10:]

        for average, job in topten:
            yield None, (average, job)

    def steps(self):
        return [
            MRStep(mapper=self.avgmapper,
                   combiner=self.avgreducer,
                   reducer=self.avgreducer),
            MRStep(mapper=self.ttmapper,
                   combiner=self.ttreducer,
                   reducer=self.ttreducer) ]


if __name__ == '__main__':
    salaryavg.run()