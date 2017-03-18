import unicodecsv
import xlrd


def flag_bad_file(cur, file_name):
    update_query = "Update nds.meta_source_files " \
                   "set file_process_state='failed'," \
                   "    processed_dtm = now() " \
                   "where file_name='{0}'"
    cur.execute(update_query.format(file_name))
    print ("Import Faild:", file_name, update_query)


def flag_completed_file(cur, file_name):
    update_query = "Update nds.meta_source_files " \
                   "set file_process_state='completed'," \
                   "    processed_dtm = now() " \
                   "where file_name='{0}'"
    cur.execute(update_query.format(file_name))
    print ("Import Completion:", file_name)


def open_file(full_file_path):
    """
    Open and read an Excel file
    """
    book = xlrd.open_workbook(full_file_path)

    # print sheet names
    # print ("Total Sheets",book.nsheets,"Sheet Names",book.sheet_names())

    # get the first worksheet
    data_sheet = book.sheet_by_index(1)
    max_rows = data_sheet.nrows
    max_cols = data_sheet.ncols

    # read a row
    # print ("Sheet Header (aka 1st Row)",data_sheet.row_values(0))

    # read a row slice
    for ii in range(0, max_rows):
        # print ii;
        rows = data_sheet.row_slice(rowx=ii, start_colx=0, end_colx=max_cols)
        print rows[1]
        # print ("Load row into TempTable",rows,ii)
        # for cell in rows:
        #    print cell.value


def xls_to_csv(xls_filename, csv_filename):
    wb = xlrd.open_workbook(xls_filename)
    sh = wb.sheet_by_index(1)

    fh = open(csv_filename, "wb")
    csv_out = unicodecsv.writer(fh, encoding='utf-8', delimiter=';')

    for row_number in range(1, sh.nrows):
        csv_out.writerow(sh.row_values(row_number))

    fh.close()
