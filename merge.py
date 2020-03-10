import os

import pandas as pd
import concurrent.futures


l = os.listdir('default')
count = 0
def merge(file):
    global count
    print(count,file)
    r = count/10000
    with open(file,'r') as f, open(output+str(r)):


    count+=1

    
with concurrent.futures.ThreadPoolExecutor(4) as executor:
    executor.map(merge, l)
