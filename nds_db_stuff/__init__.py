import glob


def reset_meta_db(cur):
    query = "Truncate table nds.meta_source_files"
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
