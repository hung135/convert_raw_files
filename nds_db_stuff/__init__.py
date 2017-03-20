import glob
import xlrd


def reset_meta_db(cur):
    query = "Truncate table nds.meta_source_files"
    cur.execute(query)
    query = "Truncate table nds.tmp_load"
    cur.execute(query)


def record_all_source_files(file_path, cur):
    total_inserted = 0
    insertquery = "INSERT INTO nds.meta_source_files(file_name) VALUES('{0}') ON CONFLICT (file_name) DO NOTHING"
    # cur = conn.cursor()
    files_list = glob.glob1(file_path, "*.xlsx")
    print files_list
    for f in files_list:
        # print f
        cur.execute(insertquery.format(f))
        result = cur.rowcount
        total_inserted = result + total_inserted
    return total_inserted


def get_files(cur, process_state):
    # cur = conn.cursor()
    # cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    query = "SELECT file_name from nds.meta_source_files"
    if process_state.upper() == ("ALL"):
        query = query + " where 'ALL'='{0}'"
        process_state = process_state.upper()
    else:
        query = query + " where file_process_state='{0}'"
    cur.execute(query.format(process_state))
    rows = cur.fetchall()

    return rows


def process_file(full_file_path, cur,conn):
    """
    insertquery = "INSERT INTO nds.meta_source_files(file_name) VALUES('{0}') ON CONFLICT (file_name) DO NOTHING"
    """
    insert_query = "Insert into nds.tmp_load(loan_code,value) values('{0}','{1}')"
    book = xlrd.open_workbook(full_file_path)

    # print sheet names
    # print ("Total Sheets",book.nsheets,"Sheet Names",book.sheet_names())

    # get the first worksheet
    data_sheet = book.sheet_by_index(1)
    max_rows = data_sheet.nrows
    max_cols = data_sheet.ncols

    # read a row
    # print ("Sheet Header (aka 1st Row)",data_sheet.row_values(0))
    totals = [0, max_rows]

    # read a row slice
    for ii in range(0, max_rows):
        # print ii;
        rows = data_sheet.row_slice(rowx=ii, start_colx=0, end_colx=max_cols)
        a=rows[0].value
        b=str(rows[3].value)
        data=[a,b]
       # print insert_query,data
        try:
            cur.execute(insert_query.format(a,b))
            totals[0] = totals[0] + 1

        except:
            pass


        # print ("Load row into TempTable",rows,ii)
        # for cell in rows:
        #    print cell.value

    conn.commit()
    return totals


def xls_to_csv(xls_filename, csv_filename):
    wb = xlrd.open_workbook(xls_filename)
    sh = wb.sheet_by_index(1)

    fh = open(csv_filename, "wb")
    csv_out = unicodecsv.writer(fh, encoding='utf-8', delimiter=';')

    for row_number in range(1, sh.nrows):
        csv_out.writerow(sh.row_values(row_number))

    fh.close()
