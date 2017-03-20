import psycopg2

import db_logging
import nds_db_stuff



file_path="/Volumes/home/gitHub/nds/sample_data/"
conn = psycopg2.connect("dbname='nds' user='postgres' host='192.168.33.10' password='ubuntu'")
cur = conn.cursor()

# reset metadata tables
nds_db_stuff.reset_meta_db(cur);
conn.commit()

# try:
print ("Total New Files", nds_db_stuff.record_all_source_files(file_path, cur))
conn.commit()
rows = nds_db_stuff.get_files(cur, 'raw')
for r in rows:
    print r[0]
    try:
        curr_file = r[0]
        curr_file_full_path = file_path + curr_file
        total=0
        # db_logging.xls_to_csv(curr_file_full_path, file_path + 'a.csv')
        totals = nds_db_stuff.process_file(curr_file_full_path, cur,conn)

        db_logging.flag_completed_file(conn,cur, curr_file, totals[0], totals[1])
        conn.commit()

    except Exception as inst:
        print type(inst)  # the exception instance
        print inst.args  # arguments stored in .args
        print inst  # __str__ allows args to be printed directly

        print "\tBad File" + curr_file_full_path
        db_logging.flag_bad_file(cur, r[0])

conn.commit()
cur.close()

#except:
 #   print "I am unable to connect to the database"

