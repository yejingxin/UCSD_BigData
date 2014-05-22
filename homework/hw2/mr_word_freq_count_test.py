#!/usr/bin/python
# Copyright 2009-2010 Yelp
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""The classic MapReduce job: count the frequency of words.
"""
from mrjob.job import MRJob
import re
from sys import stderr

WORD_RE = re.compile(r"[\w']+")

#logfile=open('log','w')
logfile=stderr

class MRWordFreqCount(MRJob):

    def mapper(self, _, line):
        logfile.write('%s\n' % WORD_RE.findall(line))
        for word in WORD_RE.findall(line):
            logfile.write('mapper '+word.lower()+'\n')
            yield ((word.lower(),word.lower()), (1,2))

    def combiner(self, word, counts):
        #yield (word, sum(counts))
        l_counts=[c for c in counts]  # extract list from iterator
        #S=sum(l_counts),
        S1=0
        S2=0
        for c in l_counts:
            S1 = S1 + c[0]
            S2 = S2 + c[1]
        logfile.write('combiner '+word[0]+word[1]+' ['+','.join([str(c[0]) for c in l_counts])+','.join([str(c[1]) for c in l_counts])+']='+str(S1)+','+str(S2)+'\n')
        yield (word, (S1,S2))

    def reducer(self, word, counts):
        #yield (word, sum(counts))
        l_counts=[c for c in counts]  # extract list from iterator
        #S=sum(l_counts),
        S1=0
        S2=0
        for c in l_counts:
            S1 = S1 + c[0]
            S2 = S2 + c[1]
        logfile.write('reducer '+word[0]+word[1]+' ['+','.join([str(c[0]) for c in l_counts])+','.join([str(c[1]) for c in l_counts])+']='+str(S1)+','+str(S2)+'\n')
        yield (word, (S1,S2))

if __name__ == '__main__':
    MRWordFreqCount.run()