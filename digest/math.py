"""
Some math helpes
"""


def median(data_list):
    """
    Finds the median in a list of numbers.

    :type data_list list
    """
    data_list = list(map(float, data_list))
    length = len(data_list)
    data_list.sort()
    # Test whether the length is odd
    if length & 1:
        # If is is, get the index simply by dividing it in half
        index = int(length / 2)
        return data_list[index]

    # If the length is even, average the two values at the center
    low_index = int(length / 2) - 1
    high_index = int(length / 2)
    average = (data_list[low_index] + data_list[high_index]) / 2
    return average
