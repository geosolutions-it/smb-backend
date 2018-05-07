'''
Created on 13 apr 2018

@author: gnafu

Math and other utilities
'''


def limit_int(inputVal, minVal = None, maxVal = None):
    '''
    limits the given integer to the given min and max
    '''
    if minVal is None and maxVal is None: return inputVal
    
    if minVal is not None and inputVal < minVal : return minVal
    if maxVal is not None and inputVal > maxVal : return maxVal
    
    return inputVal
        