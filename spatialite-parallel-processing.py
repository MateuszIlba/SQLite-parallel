import sqlite3
from sqlite3 import Error
import multiprocessing as mp
import math
import os
import time

wiersze=157573
limit = 200000
offset = 0
listaoffset=[]
while offset<wiersze:
        listaoffset.append(offset)
        offset=offset+limit


def mojafunkcja2(a):
        try:
            conn = sqlite3.connect("C:\\Users\\Mateusz\\Desktop\\testowanie\\db_small.sqlite")
            conn.enable_load_extension(True)
            conn.execute("SELECT load_extension('mod_spatialite')")
        except Error as e:
            print(e)
        try: 
            conn.execute("SELECT InitSpatialMetaData(1)")
            cur = conn.cursor()
            cur.execute("""select count (PKUID_I) from (select * from(
                        select
                        landuse_polygon.PK_UID PKUID_I,
                        landuse_polygon.Geometry Geom_one,
                        roads_lines.PK_UID PKUID_II,
                        roads_lines.Geometry Geom_two
                        from landuse_polygon, roads_lines
                        where
                        roads_lines.ROWID IN (
                            SELECT ROWID
                            FROM SpatialIndex
                            WHERE f_table_name = 'roads_lines'
                            AND search_frame = landuse_polygon.Geometry))
                        LIMIT ? OFFSET ?)
                        where st_intersects(Geom_one, Geom_two)
                        """,(limit, a,)) #a to jest zmienna z funkcji main results
            conn.commit()
            data = cur.fetchall()
            conn.execute
            conn.close()
            return data, a  ##wynik

        except Error as e:
            print(e)


def main():

	pool = mp.Pool(processes= mp.cpu_count())
	
	s = time.clock()
	results = [ pool.map(mojafunkcja2, (listaoffset)) ]
	e = time.clock()
	
	print(results)
	print(e-s)

if __name__ == '__main__':

	main()




