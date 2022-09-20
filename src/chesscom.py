import datetime as dt
import json
import logging
import os
import re

import chess
import chess.pgn
import pandas as pd
import pyodbc as sql
import requests

import func
import queries as qry
import validation as v


def chesscom_games(name, basepath):
    # download Chess.com user games
    nd = func.get_config(os.path.dirname(os.path.dirname(__file__)), 'nameDelimiter')
    dload_path = os.path.join(basepath, 'ChessCom')
    if not os.path.isdir(dload_path):
        os.mkdir(dload_path)
    dte = dt.datetime.now().strftime('%Y%m%d%H%M%S')

    conn_str = func.get_conf('SqlServerConnectionStringTrusted')
    conn = sql.connect(conn_str)
    if len(name) == 1:
        if name[0].upper() == 'CUSTOM':  # backdoor to allow me to download custom datasets based on the original Excel selection process
            qry_text = qry.custom(src='Chess.com', delim=nd)
        else:
            qry_text = qry.username(src='Chess.com', delim=nd, usr=name[0])
    else:
        qry_text = qry.person(src='Chess.com', delim=nd, lname=name[0], fname=name[1])
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
        # get pgns
        for i in users:
            archive_url = f'https://api.chess.com/pub/player/{i[1]}/games/archives'
            with requests.get(archive_url) as resp:
                if resp.status_code != 200:
                    logging.warning(f'Unable to complete request to {archive_url}! Request returned code {resp.status_code}')
                    chk = 0
                else:
                    json_data = resp.content
                    archive_list = json.loads(json_data)
                    chk = 1
            if chk == 1:
                url_ct = len(archive_list['archives'])
                ct = 1
                for url in archive_list['archives']:
                    logging.info(f'Currently downloading Chess.com game file {ct} of {url_ct}')
                    dload_url = f'{url}/pgn'
                    search_start = '/games/'
                    start = url.find(search_start) + len(search_start)
                    yyyy = url[start:start+4]
                    mm = url[-2:]
                    dload_name = f'{i[1]}_{yyyy}{mm}.pgn'
                    dload_file = os.path.join(dload_path, dload_name)
                    with requests.get(dload_url, stream=True) as resp:
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
                    ct = ct + 1

        if os.path.isfile(dload_file):
            # merge and clean pgns
            if rec_ct == 1:
                merge_name = f'ChessCom_{users[0][0]}_Merged_{dte}.pgn'
                clean_name = f'ChessCom_{users[0][0]}_AllGames_{dte}.pgn'
            else:
                merge_name = f'ChessCom_Multiple_Merged_{dte}.pgn'
                clean_name = f'ChessCom_Multiple_AllGames_{dte}.pgn'

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

            # remove any non-standard games remaining; Chess.com standard games omit the Variant tag
            pgn = open(os.path.join(dload_path, clean_name), mode='r', encoding='utf-8', errors='replace')
            updated_clean_name = os.path.splitext(clean_name)[0] + '_NoVariant' + os.path.splitext(clean_name)[1]
            pgn_new = open(os.path.join(dload_path, updated_clean_name), 'w', encoding='utf-8')
            gm_txt = chess.pgn.read_game(pgn)
            while gm_txt is not None:
                variant_tag = gm_txt.headers.get('Variant') if gm_txt.headers.get('Variant') else 'Standard'
                if variant_tag == 'Standard':
                    pgn_new.write(str(gm_txt) + '\n\n')
                gm_txt = chess.pgn.read_game(pgn)
            pgn.close()
            pgn_new.close()

            # # delete old files
            dir_files = [f for f in os.listdir(dload_path) if os.path.isfile(os.path.join(dload_path, f))]
            for filename in dir_files:
                if filename != updated_clean_name:
                    fname_relpath = os.path.join(dload_path, filename)
                    os.remove(fname_relpath)

            # move to new folder
            output_path = os.path.join(basepath, 'output')
            if not os.path.isdir(output_path):
                os.mkdir(output_path)
            old_loc = os.path.join(dload_path, updated_clean_name)
            new_loc = os.path.join(output_path, updated_clean_name)
            os.rename(old_loc, new_loc)
            logging.info('Chess.com game download complete')
    else:
        logging.info('No Chess.com games to download')
