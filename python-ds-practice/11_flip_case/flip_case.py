def flip_case(phrase, to_swap):
    """Flip [to_swap] case each time it appears in phrase.

        >>> flip_case('Aaaahhh', 'a')
        'aAAAhhh'

        >>> flip_case('Aaaahhh', 'A')
        'aAAAhhh'

        >>> flip_case('Aaaahhh', 'h')
        'AaaaHHH'

    """
    updated_phrase =''
    for i in phrase:
        if i.lower() == to_swap.lower():
            updated_phrase = updated_phrase + i.swapcase()
        else:
            updated_phrase = updated_phrase + i
    return updated_phrase

