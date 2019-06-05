from multiprocessing.dummy import Pool
import sqlite3
import time
import re
t1 = time.time()


def divide_chunks(l, n):

    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]


# def atk(l):
#     codigos = set(x[0] for x in l)

#     return (max(fl, key=lambda x: x[-1]) for c in codigos for fl in filter(lambda x: c == x[0], l))



f=open('LISTPR.TXT', 'r', encoding='latin-1')
# f = open('abc.txt', 'r', encoding='latin-1')

def split_fiat_table(l):

    resp=re.split('^(.{11}).(.{15}).{13}(.{9}).{9}(.).{10}(.{9})', l)

    return [resp[1].lstrip('0'), resp[2].strip(), int(resp[3]), resp[4], float(resp[5])/100]


a=f.readlines()


# result=list(map(split_fiat_table, a))[:10000]
# result=list(map(split_fiat_table, a))[:100000]
result=list(map(split_fiat_table, a))
def _max():
        # result_index = list(enumerate(result))
    codigos = set(x[0] for x in result)
    _max=[max(filter(lambda x: c == x[0], result),key=lambda g:g[-1]) for c in codigos]


_max_from_chunc=(max(filter(lambda x: c == x[0], ck),key=lambda g:g[-1]) for ck in divide_chunks(result,100) for c in set(x[0]for x in ck))
# _max_from_chunc=[max(filter(lambda x: c == x[0], ck),key=lambda g:g[-1]) for ck in divide_chunks(result,1000) for c in set(x[0]for x in ck)]
# _max_from_chunc=[c for ck in divide_chunks(result,10) for c in set(x[0]for x in ck)]
# _max_from_chunc=[ck for ck in divide_chunks(result,10)]
# _max_from_chunc=[max(filter(lambda x: c == x[0], ck),key=lambda g:g[-1]) for ck in divide_chunks(result,1000) ]
# print(_max_from_chunc)
# print(len(_max_from_chunc))

# def set_preco_venda():
#     [i.append() for i in _max_from_chunc for p in precos]



conn = sqlite3.connect('items.db')
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE items (
    v1         VARCHAR (50),
    v2         VARCHAR (200),
    v3         VARCHAR (50),
    v4         VARCHAR (1),
    v5         FLOAT
);
""")
conn.commit()
cursor.executemany('insert into items values(?,?,?,?,?)',_max_from_chunc)
conn.commit()
conn.close()
print(time.time()-t1)