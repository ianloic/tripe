#!/usr/bin/env python

import sys
if len(sys.argv) < 3:
  sys.stderr.write('tripe_search.py filename.tripe phrase...\n')
  sys.exit(1)

# open the supplied filename
from tripe import Tripe, TripeStore
tripe = Tripe(TripeStore(sys.argv[1], True))

# construct the phrase
phrase = ' '.join(sys.argv[2:])

# search
for instance in tripe.search(phrase):
  print 'matched in document %d at %d' % (instance.doc, instance.offset)

