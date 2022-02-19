"""Word Finder: finds random words from a dictionary."""

import random
class WordFinder:
    """>>> wf = WordFinder("./test.txt")
3 words read

    >>> wf.random() in ["cat", "dog", "elephant"]
    True

    >>> wf.random() in ["cat", "dog", "elephant"]
    True

    >>> wf.random() in ["cat", "dog", "elephant"]
    True
    """
    READ_DATA =[]
    def __init__(self,file):
        """Reads words from the file. File parameter includes
        the path of the file followed by filename"""
        self.file = file
        self.readFile()
        print(f"{len(WordFinder.READ_DATA)} words read")
        
    def readFile(self):
        """Reads words from the file"""
        with open(self.file) as f:
            WordFinder.READ_DATA = f.readlines()

    def random(self):
        """Prints a random word from a list of words"""
        random_word = random.choice(WordFinder.READ_DATA).rstrip('\n')
        return random_word

class SpecialWordFinder(WordFinder):
    """>>> wf = SpecialWordFinder("./complex.txt")
3 words read

    >>> wf.random() in ["pear", "carrot", "kale"]
    True

    >>> wf.random() in ["pear", "carrot", "kale"]
    True

    >>> wf.random() in ["pear", "carrot", "kale"]
    True
    """
    def readFile(self):
        """Reads words from the file"""
        WordFinder.readFile(self)
        newlist =[]
        for i in WordFinder.READ_DATA:
            if i.strip() and not i.startswith('#'):
                newlist.append(i)
        WordFinder.READ_DATA=newlist
 
                




