import logging

import pandas as pd
import pyodbc as sql

from func import get_conf


def custom(src, delim):
    qry_text = f"""
SELECT
ISNULL(LastName, '') + '{delim}' + ISNULL(FirstName, '') AS PlayerName,
Username

FROM UsernameXRef

WHERE Source = '{src}'
AND DownloadFlag = 1
"""
    return qry_text


def username(src, delim, usr):
    qry_text = f"""
SELECT
ISNULL(LastName, '') + '{delim}' + ISNULL(FirstName, '') AS PlayerName,
Username

FROM UsernameXRef

WHERE Source = '{src}'
AND Username = '{usr}'
"""
    return qry_text


def person(src, delim, lname, fname):
    qry_text = f"""
SELECT
ISNULL(LastName, '') + '{delim}' + ISNULL(FirstName, '') AS PlayerName,
Username

FROM UsernameXRef

WHERE Source = '{src}'
AND LastName = '{lname}'
AND FirstName = '{fname}'
"""
    return qry_text


def write_log(wr_type, player, site, timecontrol, color, startdate, enddate, outpath, dl_time, game_ct):
    conn_str = get_conf('SqlServerConnectionStringTrusted')
    conn = sql.connect(conn_str)

    # possible null handling
    player = f"'{player}'"
    site = 'NULL' if site is None else f"'{site}'"
    timecontrol = 'NULL' if timecontrol is None else f"'{timecontrol}'"
    color = 'NULL' if color is None else f"'{color}'"
    startdate = 'NULL' if startdate is None else f"'{startdate}'"
    enddate = 'NULL' if enddate is None else f"'{enddate}'"
    outpath = 'NULL' if outpath is None else f"'{outpath}'"

    csr = conn.cursor()
    sql_cmd = ''
    if wr_type == 'New':
        sql_cmd = 'INSERT INTO DownloadLog (Player, Site, TimeControl, Color, StartDate, EndDate, OutPath) VALUES '
        sql_cmd = sql_cmd + f"({player}, {site}, {timecontrol}, {color}, {startdate}, {enddate}, {outpath})"
    elif wr_type == 'Update':
        qry_text = 'SELECT MAX(DownloadID) FROM DownloadLog'
        qry_rec = pd.read_sql(qry_text, conn).values.tolist()
        curr_id = int(qry_rec[0][0])

        sql_cmd = f"UPDATE DownloadLog SET DownloadStatus = 'Complete', DownloadSeconds = {dl_time}, DownloadGames = {game_ct} WHERE DownloadID = {curr_id}"

    if sql_cmd != '':
        logging.debug(sql_cmd)
        csr.execute(sql_cmd)
        conn.commit()
    conn.close()
