#!/usr/bin/env python

import sys
if len(sys.argv) != 3:
  sys.stderr.write('tripe_add.py filename.tripe documentid < document\n')
  sys.exit(1)

documentid = int(sys.argv[2])

# open the supplied filename
from tripe import Tripe, TripeStore
tripe = Tripe(TripeStore(sys.argv[1], True))

# add the document
tripe.add(sys.stdin.read(), documentid)
