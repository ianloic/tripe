#!/usr/bin/env python
'''Tripe - an information retrieval experiment'''

import re
from struct import pack, unpack, calcsize

STEMRE = re.compile(r'\W')
def stem(term):
  '''stem and canonicalize a term'''
  return STEMRE.sub('', term.lower())

TOKRE = re.compile(r'(\s*)(\S+)(\s*)')
def tokenize(text):
  '''split text into tokens'''
  off = 0
  for pre, term, post in TOKRE.findall(text):
    off = off + len(pre)
    yield (off, stem(term), term)
    off = off + len(term) + len(post)



class Tripe(object):
  '''a text index'''
  MAGIC = unpack('Q', 'Tripe001')[0]
  def __init__(self):
    self.root = TrieNode()

  def search(self, phrase, exact=False):
    '''match the phrase'''
    # tokenize the phrases
    tokenized = list(tokenize(phrase))

    # search for the first token in the phrase
    off, stemmed, raw = tokenized[0]
    instances = self.root.search(stemmed)

    # optionally check the raw version
    if exact:
      instances = [instance for instance in instances 
          if instance.matches_exact(raw)]

    # check the rest of the words in the phrase
    instances = [instance for instance in instances
        if instance.matches_phrase(tokenized[1:], exact)]

    return instances

  def add(self, text, docid):
    doc = Document(docid, text)
    tokens = list(tokenize(text))
    tokens.reverse()
    next = None
    for off, stemmed, raw in tokens:
      ti = TermInstance(doc, off, raw, next)
      self.root.add(stemmed, ti)
      next = ti

  def dot(self):
    print 'digraph Tripe {'
    self.root.dot()
    print '}'

  def write(self, file):
    '''make sure we're at the start of the file'''
    assert file.tell() == 0
    # write 16 64bit ints
    #   magic 'Tripe001"
    #   root node offset
    #   14 x 0 for future space
    file.write(pack('QQQQQQQQQQQQQQQQ', Tripe.MAGIC, 0, *((0,)*14)))
    # write all of the nodes, get the offset of the root node
    root_off = self.root.write(file)
    # write it to the second place in the file
    file.seek(calcsize('Q'))
    file.write(pack('Q', root_off))


class TrieNode(object):
  '''a node in the trie'''
  def __init__(self):
    self.matches = []
    self.children = {}
    self.dotname = None
    self.offset = 0

  def search(self, term):
    if term == '':
      return self.matches
    else:
      return self.children[term[0]].search(term[1:])

  def add(self, term, value):
    if term == '':
      self.matches.append(value)
    else:
      if not self.children.has_key(term[0]):
        self.children[term[0]] = TrieNode()
      self.children[term[0]].add(term[1:], value)

  def dot(self, label=''):
    if self.dotname:
      return self.dotname
    self.dotname = 'N%s' % id(self)
    print '%s [label="%s"]' % (self.dotname, label)
    for k, c in self.children.items():
      print '%s -> %s [label=\"%s\"]' % (self.dotname, c.dot(label+k), k)
    for m in self.matches:
      print '%s -> %s' % (self.dotname, m.dot())
    return self.dotname

  def write(self, file):
    if self.offset:
      return self.offset

    # force all matches and children to be written, collect offsets
    match_offsets = [m.write(file) for m in self.matches]
    child_offsets = [(k, v.write(file)) for k,v in self.children.items()]

    # now prepare to write the record for this trie node
    self.offset = file.tell() # remember where we're writing
    # write the number of matches and children
    file.write(pack('QQ', len(self.matches), len(self.children)))
    # write the matches offsets
    file.write(pack('Q'*len(match_offsets), *match_offsets))
    # write each of the char/offset pairs for the children
    child_offsets.sort()
    for k,o in child_offsets:
      file.write(pack('QQ', ord(k), o))

    return self.offset


class TermInstance(object):
  '''an instance of a term in a document'''
  def __init__(self, doc, off, raw, next):
    self.doc = doc # document
    self.off = off # offset within document
    self.raw = raw # original text of the term
    self.next = next # next term instance in the document
    self.dotname = None
    self.offset = 0

  def matches_exact(self, raw):
    return self.raw == raw

  def matches_phrase(self, phrase, exact=False):
    instance = self.next
    for off, stemmed, raw in phrase:
      # if we run out of words in the document we fail
      if not instance: return False

      # if the next word in the document doesn't match the next word in the
      # phrase we fail
      if exact:
        if raw != instance.raw:
          return False
      else:
        # FIXME: sucks to re-stem - should we store the stemmed version
        # at the leaf nodes? that sucks too.
        if stemmed != stem(instance.raw):
          return False

      # this word looked good, next please
      instance = instance.next
    return True

  def __repr__(self):
    return 'TermInstance<%s, off=%s, raw=%s>' % (`self.doc`, self.off, `self.raw`)

  def dot(self):
    if self.dotname:
      return self.dotname
    self.dotname = 'M%s' % id(self)
    print '%s [label=\"%s\" shape=box]' % (self.dotname, self.raw)
    if self.next:
      print '%s -> %s [style=dashed]' % (self.dotname, self.next.dot())
    return self.dotname

  def write(self, file):
    if self.offset: return self.offset

    # write (and get the offset for) the next term instance
    next_offset = 0
    if self.next:
      next_offset = self.next.write(file)

    # write the raw string version for reference
    # FIXME: avoid duplication, other instances of the same term may be the same
    raw_offset = file.tell()
    utf8 = self.raw.encode('utf8') + '\0'
    # pad to 64 bits
    while len(utf8) % calcsize('Q'): utf8 = utf8 + '\0'
    # write it out
    file.write(utf8)

    # write the record for this instance
    self.offset = file.tell()
    file.write(pack('QQQQ', self.doc.docid, self.off, raw_offset, next_offset))

    return self.offset


class Document(object):
  '''a document contains terms'''
  def __init__(self, docid, text):
    self.docid = docid
    self.text = text
  def __repr__(self):
    return 'Document<%s>' % self.docid

tripe = Tripe()
tripe.add('Hello world', 1)
tripe.add('Hello, World', 2)
tripe.add('Goodbye, cruel world...', 3)
tripe.add('This is a test.', 4)
tripe.add('This is not a pipe', 5)
tripe.add('Thistle, bristle and whistle!', 6)
tripe.add('A bird in the hand is worth two in the bush.', 7)

#tripe.dot()
tripe.write(open('/tmp/test.tripe', 'w'))
