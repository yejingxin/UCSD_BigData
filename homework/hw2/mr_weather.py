#!/usr/bin/python
"""
count the number of measurements of each type
"""
import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
import re
from sys import stderr

WORD_RE = re.compile(r"[\w']+")

#logfile=open('log','w')
#logfile=stderr

class MRWeather(MRJob):

    def mapper(self, _, line):
        try:
            self.increment_counter('MrJob Counters','mapper-all',1)
            elements=line.split(',')
            #logfile.write('%s\n' % type(line))
            if elements[1]=='TMAX' or elements[1]=='TMIN':
                Ndays = sum([e!='' for e in elements[3:]])
                meas = 1
                key = (elements[0],elements[2])
                #out=(elements[0],elements[2]), 
            else:
                key = 'Useless'
                meas = 0
                Ndays = 0
        except Exception, e:
            stderr.write('Error in line:\n'+line)
            stderr.write(e)
            self.increment_counter('MrJob Counters','mapper-error',1)
            key = 'error'
            meas = 0
            Ndays = 0

        finally:
            yield key, (meas,Ndays)

            
    def combiner(self, word, counts):
        self.increment_counter('MrJob Counters','combiner',1)
        sum1 = 0
        sum2 = 0
        for meas,Ndays in counts:
            sum1 = sum1 + meas
            sum2 = sum2 + Ndays
        yield word, (sum1, sum2)
#    def combiner(self, word, counts):
#        self.increment_counter('MrJob Counters','combiner',1)
#        l_counts=[c for c in counts]
#        sum1 = 0
#        sum2 = 0
#        for e in l_counts:
#            sum1 = sum1 + float(e[0])
#            sum2 = sum2 + float(e[1])
#        sum3 = (sum1,sum2)
#        yield (word, sum3)
        #l_counts=[c for c in counts]  # extract list from iterator
        #S=sum(l_counts)
        #logfile.write('combiner '+word+' ['+','.join([str(c) for c in l_counts])+']='+str(S)+'\n')
        #yield (word, S)

    def reducer(self, word, counts):
        self.increment_counter('MrJob Counters','reducer',1)
        sum1 = 0
        sum2 = 0
        for meas,Ndays in counts:
            sum1 = sum1 + meas
            sum2 = sum2 + Ndays
        yield word, (sum1, sum2)
        #l_counts=[c for c in counts]  # extract list from iterator
        #S=sum(l_counts)
        #logfile.write('reducer '+word+' ['+','.join([str(c) for c in l_counts])+']='+str(S)+'\n')
        #yield (word, S)

if __name__ == '__main__':
    MRWeather.run()