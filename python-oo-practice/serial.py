"""Python serial number generator."""

class SerialGenerator:
    """Machine to create unique incrementing serial numbers.
    
    >>> serial = SerialGenerator(start=100)

    >>> serial.generate()
    100

    >>> serial.generate()
    101

    >>> serial.generate()
    102

    >>> serial.reset()

    >>> serial.generate()
    100
    """
    GEN_COUNT = 0
    def __init__(self,start):
        self.start = start

    def __repr__(self):
        return f"<SerialGenerator start={self.start} next={self.start+SerialGenerator.GEN_COUNT}"

    def generate(self):
        """Generate next number in sequence each time this 
        method is invoked after initialization"""
        SerialGenerator.GEN_COUNT +=1
        return self.start+SerialGenerator.GEN_COUNT-1

    def reset(self):
        """Reset the sequence to the start value initialized"""
        SerialGenerator.GEN_COUNT = 0