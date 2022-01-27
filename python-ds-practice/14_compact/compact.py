def compact(lst):
    """Return a copy of lst with non-true elements removed.

        >>> compact([0, 1, 2, '', [], False, (), None, 'All done'])
        [1, 2, 'All done']
    """
    non_false =[]
    for i in lst:
        if i:
            non_false.append(i)
    return non_false
