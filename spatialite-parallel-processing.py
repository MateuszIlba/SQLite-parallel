import sqlite3
from sqlite3 import Error
import multiprocessing as mp
import math
import os
import time

######################################## variable ###############################################################################
database_localization="Z:\\db_very_big.sqlite"
layer1='roads_lines'
layer2='buildings_points'
spatialfunction='st_touches' ##name of any spatial function to do with two layers 
## spatial function: ST_Equals, ST_Disjoint, ST_Touches, ST_Within, ST_Overlaps, ST_Crosses, ST_Intersects, ST_Contains, ST_Covers, ST_CoveredBy.

################################### def statement ###################################################################################
def row_count():
        global limit, rowcount
        try:
                sqlstatement='select count(' + '%s'%layer1 + '.PK_UID) from '+'%s'%layer1+', '+'%s'%layer2 + ' where '+ '%s'%layer2 + '''.ROWID IN ( SELECT ROWID FROM SpatialIndex WHERE f_table_name = '''+ """ '""" +'%s'%layer2+'''' AND search_frame ='''+'%s'%layer1+'.Geometry)'
                cur.execute(sqlstatement)
                rowcount__ = cur.fetchone()
                rowcount = rowcount__[0]
                print ('row count of pairs: '+'%s'%rowcount)
                proces= mp.cpu_count()
                limit = int((rowcount/proces) + 1)########limit - depending on the number of cores - distribution of calculations into cores
                #limit = 30000000 ##only for tests
                return limit
        except Error as e:
                print(e)

def gen_list_offset():
        global listaoffset
        offset = 0
        listaoffset=[]
        while offset<rowcount:
                listaoffset.append(offset)
                offset=offset+limit
        return listaoffset

def mojafunkcja2(a):
        try:
                sqlstatement = 'select count (PKUID_I) from (select * from(select ' + '%s'%layer1 + '.PK_UID PKUID_I,' + '%s'%layer1 + '.Geometry Geom_one,' + '%s'%layer2 + '.PK_UID PKUID_II,' + '%s'%layer2 + '.Geometry Geom_two ' + ' from ' + '%s'%layer1 + ',' + '%s'%layer2 + ' where ' + '%s'%layer2 + '.ROWID IN (SELECT ROWID FROM SpatialIndex WHERE f_table_name =' + """'""" + '%s'%layer2 + """'""" + ' AND search_frame = ' + '%s'%layer1 + '.Geometry)) LIMIT ' + '%s'%limit + ' OFFSET ' + '%s'%a + ')where ' + '%s'%spatialfunction + '(Geom_one, Geom_two)'
                cur.execute(sqlstatement)
                data = cur.fetchall()
                return data, a  ##results
        except Error as e:
                print(e)

def main():
        pool = mp.Pool(processes= mp.cpu_count())
        s = time.clock()
        results = [ pool.map(mojafunkcja2, (listaoffset)) ]
        e = time.clock()
        print(results)
        print(e-s)


######################################### parallel algorithm start here ###########################################################
try:
        conn = sqlite3.connect(database_localization)
        conn.enable_load_extension(True)
        conn.execute("SELECT load_extension('mod_spatialite')")
        cur = conn.cursor()
except Error as e:
        print(e)
        
row_count()
gen_list_offset()
print ('limit pairs for thread: '+'%s'%limit)

if __name__ == '__main__':
        main()

conn.close
