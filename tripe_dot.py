#!/usr/bin/env python

def dot(tripe):
  print 'digraph Tripe {'
  nodes = [(tripe.root, '')]
  while len(nodes) > 0:
    node, prefix = nodes.pop(0)
    print '  N%s[label="%s"]' % (node.handle, prefix)

    for name, child in node.children().items():
      print '  N%s -> N%s [label="%s"]' % (node.handle, child.handle, name)
      nodes.append((child, prefix+name))

    for match in node.matches():
      print '  N%s -> M%s' % (node.handle, match.handle)
      print '  M%s [label="%s" shape=box]' % (match.handle, match.raw)
      if match.next_handle != 0:
        print '  M%s -> M%s [style=dashed]' % (match.handle, match.next_handle)

  print '}'

import sys
if len(sys.argv) != 2:
  sys.stderr.write('tripe_dot.py filename.tripe\n')
  sys.exit(1)

# open the supplied filename
from tripe import Tripe, TripeStore
tripe = Tripe(TripeStore(sys.argv[1], False))

# generate a directed graph
dot(tripe)
