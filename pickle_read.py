import os
import pandas as pd
print('pandas: %s' % pd.__version__)
# import cx_Oracle
# import matplotlib.pyplot as plt
# import matplotlib.dates as mdates
# import sys
# import subprocess
import pickle
# import blpapi

print(os.getcwd())

def main():

    # with open(r'C:\SRDEV\Data\BBG\bbg_data.pickle', 'rb') as handle:
    #    test = pickle.load(handle)

    test = pd.read_pickle(r'C:\SRDEV\creds.pickle')



    print(os.path.basename(__file__), 'executed')

if __name__ == '__main__':
    main()
else:
    print("Run From Import")