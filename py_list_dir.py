import glob
import psycopg2

import xlrd
import unicodecsv



def flag_bad_file(cur,file_name):
    update_query = "Update nds.meta_source_files " \
                   "set file_process_state='failed'," \
                   "    is_processed_dtm = now() " \
                   "where file_name={0}"

    print ("flagged bad file", file_name,update_query)
    cur.execute(update_query ,file_name)


def open_file(path):
    """
    Open and read an Excel file
    """
    book = xlrd.open_workbook(path)

    # print sheet names
    print ("Total Sheets",book.nsheets,"Sheet Names",book.sheet_names())

    # get the first worksheet
    data_sheet = book.sheet_by_index(1)
    max_rows=data_sheet.utter_max_rows
    # read a row
    print ("Sheet Header (aka 1st Row)",data_sheet.row_values(0))

    # read a row slice
    for ii in range(1, max_rows):
        rows =data_sheet.row_slice(rowx=ii,
                                start_colx=0,
                                end_colx=5)
        print ("Load row into TempTable",rows,ii)
        #for cell in rows:
        #    print cell.value


def xls_to_csv (xls_filename, csv_filename):

    wb = xlrd.open_workbook(xls_filename)
    sh = wb.sheet_by_index(1)

    fh = open(csv_filename,"wb")
    csv_out = unicodecsv.writer(fh, encoding='utf-8', delimiter=';')

    for row_number in range (1,sh.nrows):
        csv_out.writerow(sh.row_values(row_number))

    fh.close()
file_path="/Volumes/home/gitHub/nds/sample_data/"
#try:

conn = psycopg2.connect("dbname='nds' user='postgres' host='192.168.33.10' password='ubuntu'")
cur = conn.cursor()
#cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
query="SELECT file_name from nds.meta_source_files"
insertquery="INSERT INTO nds.meta_source_files(file_name) VALUES('{0}') ON CONFLICT (file_name) DO NOTHING"
#insertquery="INSERT INTO nds.meta_source_files(file_name) VALUES('%s') ON CONFLICT (file_name) DO NOTHING"
files_list= glob.glob1(file_path,"*.xlsx")
cur.execute(query)
rows = cur.fetchall()
print files_list
for f in files_list:

    #print f
    cur.execute(insertquery.format(f))

conn.commit()

for r in rows:
    print r[0]
    try:

        curr_file=file_path+r[0]
        xls_to_csv(curr_file,file_path+'a.csv')
        open_file(curr_file)
        print "Success!!!"
    except:
        #print "\tBad File" + curr_file
        flag_bad_file(cur,r[0])
cur.close()

#except:
 #   print "I am unable to connect to the database"

