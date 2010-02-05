#!/usr/bin/env python
'''Tripe - an information retrieval experiment'''

import re, os
from struct import pack, unpack, calcsize
from mmap import mmap, MAP_SHARED, PROT_READ, PROT_WRITE

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

INTSIZE = calcsize('Q') # size of ints
HEADERCOUNT = 16 # number of ints in header
HEADERSIZE = INTSIZE * HEADERCOUNT # size of header in bytes

HEADER_MAGIC = 0 # where the magic number resides
HEADER_ROOT = 1 * INTSIZE # root node
HEADER_FIRST_FREE = 2 * INTSIZE # first free block

class TripeStore(object):
  def __init__(self, filename, writable=False):
    self.filename = filename
    if writable:
      open_mode = 'r+'
      mmap_mode = PROT_READ | PROT_WRITE
    else:
      open_mode = 'r'
      mmap_mode = PROT_READ
    if not os.path.exists(filename):
      # create empty file
      open(filename, 'w').write(pack('Q'*16, Tripe.MAGIC, 
        *((0,)*(HEADERCOUNT-1))))
    # open the file
    self.file = open(filename, open_mode)
    # map the file
    self.mmap = mmap(self.file.fileno(), 0, MAP_SHARED, mmap_mode)

  def __load_number(self, offset):
    '''load a number from the map'''
    return unpack('Q', self.mmap[offset:offset+INTSIZE])[0]

  def __store_number(self, offset, number):
    '''store a number in the map'''
    self.mmap[offset:offset+INTSIZE] = pack('Q', number)

  def __allocate(self, size):
    '''allocate a block of the given size'''
    # where's the first unused thing
    free = self.__load_number(HEADER_FIRST_FREE)
    prev_free = HEADER_FIRST_FREE
    while free != 0:
      free_size = self.__handle_size(free)
      if free_size > size:
        # found a valid block, remember where it is
        offset = free - INTSIZE
        # remove it from the linked list
        self.__store_number(prev_free, self.__load_number(free))
        break
      else:
        # look at the next free block
        prev_free = free
        free = self.__load_number(free)
    else:
      # no free blocks, grow the file
      offset = len(self.mmap)
      self.mmap.resize(offset + INTSIZE + size)

    # store the length
    self.__store_number(offset, size)
    # return the offset to put data in
    return offset + INTSIZE

  def __handle_size(self, handle):
    '''return the size of the handle'''
    return self.__load_number(handle-INTSIZE)

  def __get_handle(self, handle):
    return self.mmap[handle:handle+self.__handle_size(handle)]

  def __set_handle(self, handle, bytes):
    self.mmap[handle:handle+self.__handle_size(handle)] = bytes

  def get_root(self):
    '''the handle of the root node'''
    return self.__load_number(HEADER_ROOT)

  def set_root(self, handle):
    '''the handle of the root node'''
    return self.__store_number(HEADER_ROOT, handle)

  def store_numbers(self, numbers):
    '''allocate storage and store numbers. return a handle.'''
    fmt = 'Q' * len(numbers)
    size = calcsize(fmt)
    handle = self.__allocate(size)
    self.__set_handle(handle, pack(fmt, *numbers))
    return handle

  def store_text(self, text):
    '''allocate storage and store text. return a handle.'''
    # FIXME: pad to 64-bit boundary for performance?
    utf8 = text.encode('utf-8')
    utf8_len = len(utf8)
    handle = self.__allocate(utf8_len)
    self.__set_handle(handle, utf8)
    return handle

  def update_numbers(self, handle, numbers):
    '''update numbers in an existing handle'''
    fmt = 'Q' * len(numbers)
    size = calcsize(fmt)
    assert size == self.__handle_size(handle)
    self.__set_handle(handle, pack(fmt, *numbers))

  def load_numbers(self, handle):
    '''fetch numbers stored at a handle'''
    num_numbers = self.__handle_size(handle) / INTSIZE
    return unpack('Q'*num_numbers, self.__get_handle(handle))

  def load_text(self, handle):
    '''fetch text stored at a handle'''
    # FIXME: drop trailing '\0'?
    return self.__get_handle(handle).decode('utf-8')

  def free(self, handle):
    '''mark a handle as unused'''
    print 'freeing block of size %d' % self.__handle_size(handle)
    self.store_text('F'*self.__handle_size(handle))
    # add this block to the end of the free-list
    self.__store_number(handle, self.__load_number(HEADER_FIRST_FREE))
    self.__store_number(HEADER_FIRST_FREE, handle)


class Tripe(object):
  '''a text index'''
  MAGIC = unpack('Q', 'Tripe001')[0]
  def __init__(self, store):
    self.store = store
    if store.get_root() == 0:
      # no root found - we should make one
      self.root = TrieNode(self, None)
      # store its location
      store.set_root(self.root.handle)
    else:
      # load the root node
      self.root = TrieNode(self, store.get_root())

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

  def add(self, text, doc):
    tokens = list(tokenize(text))
    tokens.reverse()
    next = None
    for off, stemmed, raw in tokens:
      ti = TermInstance(self, None, doc, off, raw, next)
      self.root.add(stemmed, ti)
      next = ti


class TrieNode(object):
  '''a node in the trie'''
  def __init__(self, tripe, handle):
    self.tripe = tripe
    # if this is a new node, allocate it
    if handle == None:
      handle = tripe.store.store_numbers((0,0))
    self.handle = handle

  def __matches(self):
    '''returns list of match handles'''
    matches_handle, children_handle = \
        self.tripe.store.load_numbers(self.handle)
    if matches_handle == 0: return []
    return self.tripe.store.load_numbers(matches_handle)

  def __children(self):
    '''returns list of pairs of (key, handle)'''
    matches_handle, children_handle = \
        self.tripe.store.load_numbers(self.handle)
    if children_handle == 0: return []
    children = self.tripe.store.load_numbers(children_handle)
    return zip(children[::2], children[1::2])

  def __find_child(self, name):
    # FIXME: binary search, or something better
    for key, offset in self.__children():
      if key == name: return TrieNode(self.tripe, offset)
    return None

  def children(self):
    return dict([(chr(k), TrieNode(self.tripe, v)) for k, v in self.__children()])

  def matches(self):
    return [TermInstance(self.tripe, handle) for handle in self.__matches()]

  def search(self, term):
    if term == '':
      return [TermInstance(self.tripe, h) for h in self.__matches()]
    else:
      child = self.__find_child(ord(term[0]))
      if child: return child.search(term[1:])
      else: return []

  def add(self, term, value):
    if term == '':
      # get the current list of matches
      matches = list(self.__matches())
      # append this new match
      matches.append(value.handle)
      # store the new list
      new_matches = self.tripe.store.store_numbers(matches)
      # update the record
      old_matches, children_handle = \
          self.tripe.store.load_numbers(self.handle)
      self.tripe.store.update_numbers(self.handle, (new_matches, children_handle))
      # free the old matches array
      if old_matches:
        self.tripe.store.free(old_matches)
    else:
      # look for an existing child node
      character = ord(term[0])
      child = self.__find_child(character)
      if child == None:
        # create a new child
        child = TrieNode(self.tripe, None)
        # none, time to rewrite the children list
        children = self.__children()
        children.append((character, child.handle))
        children.sort()
        # flatten the list for storage
        children_flat = []
        for n,v in children:
          children_flat.append(n)
          children_flat.append(v)
        # store the new list of children
        new_children = self.tripe.store.store_numbers(children_flat)
        # update the record
        matches_handle, old_children = \
            self.tripe.store.load_numbers(self.handle)
        self.tripe.store.update_numbers(self.handle, (matches_handle, new_children))
        # free the old children array
        if old_children:
          self.tripe.store.free(old_children)
      child.add(term[1:], value)


class TermInstance(object):
  '''an instance of a term in a document'''
  def __init__(self, tripe, handle, doc=None, offset=None, raw=None, next=None):
    self.tripe = tripe
    # if this is a new term instance, allocate it
    if handle == None:
      # store the supplied values
      self.doc = doc
      self.offset = offset
      self.__next = next
      if next:
        self.next_handle = next.handle
      else:
        self.next_handle = 0
      self.raw_handle = tripe.store.store_text(raw)
      handle = tripe.store.store_numbers((doc, offset, self.raw_handle, 
        self.next_handle))
    else:
      # load the values
      self.doc, self.offset, self.raw_handle, self.next_handle = \
          tripe.store.load_numbers(handle)
      assert self.raw_handle != None
      self.raw = tripe.store.load_text(self.raw_handle)
      self.__next = None
    self.handle = handle

  def next(self):
    if self.__next != None: return self.__next
    if self.next_handle != 0:
      self.__next = TermInstance(self.tripe, self.next_handle)
      return self.__next
    else:
      return None

  def matches_exact(self, raw):
    return self.raw == raw

  def matches_phrase(self, phrase, exact=False):
    instance = self.next()
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
      instance = instance.next()
    return True

  def __repr__(self):
    return 'TermInstance<doc=%s, offset=%s, raw=%s>' % (`self.doc`, self.offset, `self.raw`)

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




#tripe = Tripe(TripeStore('/tmp/test.tripe', False))
tripe = Tripe(TripeStore('/tmp/test.tripe', True))
tripe.add('Hello world', 1)
tripe.add('Hello, World', 2)
tripe.add('Goodbye, cruel world...', 3)
tripe.add('This is a test.', 4)
tripe.add('This is not a pipe', 5)
tripe.add('Thistle, bristle and whistle!', 6)
tripe.add('A bird in the hand is worth two in the bush.', 7)

#tripe.write(TripeStore(open('/tmp/test.tripe', 'w')))
#tripe = Tripe.read(TripeStore(open('/tmp/test.tripe')))
#dot(tripe)
