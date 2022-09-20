import datetime as dt
import logging
import os
import re

import pandas as pd
import pyodbc as sql
import requests

import func
import queries as qry
import validation as v


def lichess_games(name, basepath):
    # download Lichess user games
    nd = func.get_config(os.path.dirname(os.path.dirname(__file__)), 'nameDelimiter')
    dload_path = os.path.join(basepath, 'Lichess')
    if not os.path.isdir(dload_path):
        os.mkdir(dload_path)

    conn_str = func.get_conf('SqlServerConnectionStringTrusted')
    conn = sql.connect(conn_str)
    if len(name) == 1:
        if name[0].upper() == 'CUSTOM':  # backdoor to allow me to download custom datasets based on the original Excel selection process
            qry_text = qry.custom(src='Lichess', delim=nd)
        else:
            qry_text = qry.username(src='Lichess', delim=nd, usr=name[0])
    else:
        qry_text = qry.person(src='Lichess', delim=nd, lname=name[0], fname=name[1])
    logging.debug(qry_text.replace('\n', ' '))
    users = pd.read_sql(qry_text, conn).values.tolist()
    rec_ct = len(users)
    conn.close()

    repl_nm = 1
    if len(name) == 1 and name[0].upper() != 'CUSTOM' and rec_ct == 0:  # username was passed, not in SQL table
        yn = v.yn_prompt('A username was passed but not found in the SQL reference table. Force download and continue? Y or N ===> ')
        if yn == 'N':
            logging.critical('Process terminated by user!')
            raise SystemExit
        users = [[name[0], name[0]]]
        rec_ct = len(users)
        repl_nm = 0

    if rec_ct > 0:
        logging.info('Lichess game download started')
        token_value = func.get_conf('LichessAPIToken')

        # get pgns
        for i in users:
            dte_val = dt.datetime.now().strftime('%Y%m%d%H%M%S')
            dload_url = f'https://lichess.org/api/games/user/{i[1]}?perfType=bullet,blitz,rapid,classical,correspondence&clocks=true&evals=true&sort=dateAsc'
            dload_name = f'{i[1]}_{dte_val}.pgn'
            dload_file = os.path.join(dload_path, dload_name)
            hdr = {'Authorization': f'Bearer {token_value}'}
            with requests.get(dload_url, headers=hdr, stream=True) as resp:
                if resp.status_code != 200:
                    logging.warning(f'Unable to complete request to {dload_url}! Request returned code {resp.status_code}')
                else:
                    with open(dload_file, 'wb') as f:
                        for chunk in resp.iter_content(chunk_size=8196):
                            f.write(chunk)
                    with open(dload_file, mode='r', encoding='utf-8', errors='ignore') as dl:
                        lines = dl.read()
                    if repl_nm:
                        txt_old = f'"{i[1]}"'
                        txt_new = '"' + i[0].replace(nd, ', ') + '"'
                        lines = re.sub(txt_old, txt_new, lines, flags=re.IGNORECASE)
                        with open(dload_file, mode='w', encoding='utf-8', errors='ignore') as dl:
                            dl.write(lines)

        if os.path.isfile(dload_file):  # file was created earlier
            # merge and clean pgns
            dte_val = dt.datetime.now().strftime('%Y%m%d%H%M%S')
            if rec_ct == 1:
                merge_name = dload_name
                clean_name = f'Lichess_{users[0][0]}_AllGames_{dte_val}.pgn'
            else:
                merge_name = f'Lichess_Multiple_Merged_{dte_val}.pgn'
                clean_name = f'Lichess_Multiple_AllGames_{dte_val}.pgn'
                cmd_text = f'copy /B *.pgn {merge_name} >nul'
                logging.debug(cmd_text)
                if os.getcwd != dload_path:
                    os.chdir(dload_path)
                os.system('cmd /C ' + cmd_text)

            cmd_text = f'pgn-extract -N -V -D -pl2 --quiet --nosetuptags --output {clean_name} {merge_name} >nul'
            logging.debug(cmd_text)
            if os.getcwd != dload_path:
                os.chdir(dload_path)
            os.system('cmd /C ' + cmd_text)

            # delete old files
            dir_files = [f for f in os.listdir(dload_path) if os.path.isfile(os.path.join(dload_path, f))]
            for filename in dir_files:
                if filename != clean_name:
                    fname_relpath = os.path.join(dload_path, filename)
                    os.remove(fname_relpath)

            # move to new folder
            output_path = os.path.join(basepath, 'output')
            if not os.path.isdir(output_path):
                os.mkdir(output_path)
            old_loc = os.path.join(dload_path, clean_name)
            new_loc = os.path.join(output_path, clean_name)
            os.rename(old_loc, new_loc)
            logging.info('Lichess game download complete')
        else:
            logging.info('Lichess game download complete')
    else:
        logging.info('No Lichess games to download')
