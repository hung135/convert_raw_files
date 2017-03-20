

def flag_bad_file(cur, file_name):
    update_query = "Update nds.meta_source_files " \
                   "set file_process_state='failed'," \
                   "    processed_dtm = now() " \
                   "where file_name='{0}'"
    cur.execute(update_query.format(file_name))
    print ("Import Faild:", file_name, update_query)


def flag_completed_file(conn,cur, file_name, rows_inserted, total_rows):
    update_query = "Update nds.meta_source_files " \
                   "set file_process_state='completed'," \
                   "    rows_inserted={1}," \
                   "    total_rows={2}," \
                   "    processed_dtm = now()" \
                   "where file_name='{0}'"
    cur.execute(update_query.format(file_name, rows_inserted, total_rows))
    print ("Import Completion:", file_name,"Rows Inserted",rows_inserted,"Total Rows:",total_rows)
    conn.commit()


