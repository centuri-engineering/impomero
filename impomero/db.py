import sqlite3


def init_db(db_name):
    """Creates the import database with the correct tables
    and columns
    """
    with sqlite3.connect(db_name) as sql_con:
        sql_con.execute("CREATE TABLE IF NOT EXISTS monitored (date, base_dir)")
        sql_con.execute(
            """CREATE TABLE IF NOT EXISTS annotated ('index', title, created,
                project, user, comment, tags, accessed, target, fileset, file_path,
                'group', organism, sample, channel_0, id, base_dir)"""
        )
