from scipy.stats import norm as N


def find(list, lStr):
    result = []
    for each in list:
        is_match = 1
        for ch in lStr:
            count = 0
            # print ch
            for w in each:
                if w == ch:
                    count += 1
            if count == 1:
                is_match = is_match and 1
            else:
                is_match = 0
                break
        if is_match == 1:
            if len(each) == len(lStr):
                result.append(each)
    return result

# print find(['eat', 'ate', 'at', 'atte', 'late', 'tea', 'atem'], 'aet')
# print 4 ** 0.5
#
#-*- coding:utf-8 -*-
# def describe(a):
#     meansum = 0
#     mean = 0
#     varsum = 0
#     var = 0
#     biassum = 0
#     bias = 0
#     kusum = 0
#     kus = 0
#     for each in a:
#         meansum += each
#     if len(a) == 0:
#         mean = None
#     else:
#         mean = float(meansum) / float(len(a))
#     for each in a:
#         varsum += (each - mean) ** 2
#     if len(a) <= 1:
#         var = None
#     else:
#         var = float(varsum) / float(len(a) - 1)
#     for each in a:
#         biassum += (each - mean) ** 3
#     if len(a) == 0:
#         bias = None
#     else:
#         temp = (float(varsum) / float(len(a))) ** 1.5
#         bias = float(biassum) / float(len(a))
#         bias /= float(temp)
#     for each in a:
#         kusum += (each - mean) ** 4
#     if len(a) == 0:
#         kus = None
#     else:
#         temp = (float(varsum) / float(len(a))) ** 2
#         kus = float(kusum) / float(len(a))
#         kus /= float(temp)
#         kus -= 3
#     return [round(mean, 6), round(var, 6), round(bias, 6), round(kus, 6)]
#     pass


# print describe([1.0, 2.0, 3.0])
#-*- coding:utf-8 -*-

# import numpy as np
# from scipy.stats import norm
#
#
# class Solution():
#     def solve(self):
#         x = norm.rvs(scale=25, size=3600)
#         de = norm.ppf(0.025)
#         lower = 8.5+5/np.sqrt(3600)*de
#         upper = 8.5-5/np.sqrt(3600)*de
#         return [lower, upper]
#
# p=Solution()
# print (p.solve())
volume = '20,98'
volume = volume.replace(",", "")
print int(volume)
