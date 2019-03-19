#!/usr/bin/env python

import db
import nowapi as na
import pandas as pd
import random as rnd

IT = 1000

# describe from the database
def stats(c, o, f):
    sql = "select count(*), avg(%s), stddev(%s), \
                            max(%s), min(%s), median(%s) \
             from buys \
            where origin = %d" % (f, f, f, f, f, o)
    for row in c.execute(sql, rowformat=na.listrow):
        return {'count': row[0], 'mean': row[1], 'std': row[2], \
                                 'max' : row[3], 'min': row[4], \
                                 '50%' : row[5]}

# select a random row
def randomRow(df):
    idx = rnd.randint(0,len(df)-1)
    return df['origin'][idx]

# compare dataframe and database
def compare(df,d):
    if df['count'] != d['count']:
       raise db.TestFailed('count differs')
    if df['min'] != d['min']:
       raise db.TestFailed('min differs')
    if df['max'] != d['max']:
       raise db.TestFailed('max differs')
    mf = float(int(df['mean'] * 1000))/1000
    m  = float(int(d['mean'] * 1000))/1000
    if mf != m:
       raise db.TestFailed('mean differs')

    mf = float(int(df['std'] * 1000))/1000
    m  = float(int(d['std'] * 1000))/1000
    if mf != m:
       raise db.TestFailed('std differs')

    mf = float(int(df['50%'] * 1000))/1000
    m  = float(int(d['50%'] * 1000))/1000
    if mf != m:
       raise db.TestFailed('50% differs')

if __name__ == '__main__':
   with na.connect('localhost', '55505', None, None, 'db150') as con:
        buys = pd.read_sql('select origin, price, quantity from buys', con)
        gbbuys = buys.groupby(by=['origin'])
        desc = gbbuys.describe()

        print(buys.head())

        for i in range(IT):
            o = randomRow(buys)
            print('test %04d) %d' % (i+1,o))

            mydesc = desc.loc[o]['price']
            dbdesc = stats(con, o, 'price')
            compare(mydesc,dbdesc)

            mydesc = desc.loc[o]['quantity']
            dbdesc = stats(con, o, 'quantity')
            compare(mydesc,dbdesc)

   print "PASSED"
