from multiprocessing import Pool
import re

f = open('abc.txt', 'r', encoding='latin-1')

def split_fiat_table(l):

    resp = re.split('^(.{11}).(.{15}).{13}(.{9}).{9}(.).{10}(.{9})', l)
    
    return [resp[1].lstrip('0'), resp[2].strip(), int(resp[3]), resp[4], float(resp[5])/100]


a = f.readlines()


with Pool(5) as p:
    result = list(p.map(split_fiat_table, a))

p.join()

codigos=set(x[0] for x in result)

def max_duplicados(c):
    return max(list(filter(lambda x:c==x[0],result)),key=lambda x:x[-1])
    
resp=list(map(max_duplicados,codigos))


for r in resp:
    print(r)
