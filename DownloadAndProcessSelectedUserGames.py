import datetime as dt
import pyodbc as sql
import pandas as pd
from urllib import request, error
import json
import os
import re
import fileinput
import shutil as sh
import chess
import chess.pgn

def lichessgames():
    dload_path = r'C:\Users\eehunt\Documents\Chess\Scripts\Lichess'
    dte = dt.datetime.now()
    utc_monthstart = str(int(dte.replace(tzinfo=dt.timezone.utc).timestamp())) + '000' # because I'm lazy I'll hard-code the milli/micro/nanoseconds

    conn = sql.connect('Driver={ODBC Driver 17 for SQL Server};Server=HUNT-PC1;Database=ChessAnalysis;Trusted_Connection=yes;')        
    qry_text = "SELECT ISNULL(LastName, '') + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE EEHFlag = 0 AND Source = 'Lichess' AND DownloadFlag = 1"
    users = pd.read_sql(qry_text, conn).values.tolist()
    rec_ct = len(users)
    conn.close()

    if rec_ct > 0:
        # get pgns
        for i in users:
            dte_val = dt.datetime.now().strftime('%Y%m%d%H%M%S')
            """TODO Look into using authentication to speed up a bit, iterating over URL's is slow"""
            dload_url = 'https://lichess.org/api/games/user/' + i[1] + '?until=' + utc_monthstart
            dload_name = i[1] + '_' + dte_val + '.pgn'
            dload_file = os.path.join(dload_path, dload_name)
            try:
                request.urlretrieve(dload_url, dload_file)
                with open(dload_file, mode='r', encoding='utf-8', errors='ignore') as dl:
                    lines = dl.read()
                txt_old = '"' + i[1] + '"'
                txt_new = '"' + i[0] + '"'
                lines = re.sub(txt_old, txt_new, lines, flags=re.IGNORECASE)
                with open(dload_file, mode='w', encoding='utf-8', errors='ignore') as dl:
                    dl.write(lines)
            except error.HTTPError.code as e:
                err = e.getcode()
                print(str(err) + ' error on ' + i[1])

        # merge and clean pgns
        dte_val = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        if rec_ct == 1:
            merge_name = dload_name
            clean_name = 'Lichess_' + users[0][0] + '_AllGames_' + dte_val + '.pgn'
        else:
            merge_name = 'Lichess_Multiple_Merged_' + dte_val + '.pgn'
            clean_name = 'Lichess_Multiple_AllGames' + dte_val + '.pgn'
            cmd_text = 'copy /B *.pgn ' + merge_name
            if os.getcwd != dload_path:
                os.chdir(dload_path)
            os.system('cmd /C ' + cmd_text)

        cmd_text = 'pgn-extract -C -N -V -D -pl2 --quiet --nosetuptags --output ' + clean_name + ' ' + merge_name
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
        output_path = r'C:\Users\eehunt\Documents\Chess\Scripts\output'
        old_loc = os.path.join(dload_path, clean_name)
        new_loc = os.path.join(output_path, clean_name)
        os.rename(old_loc, new_loc)
    
    print('Lichess game download complete')

def chesscomgames():
    dload_path = r'C:\Users\eehunt\Documents\Chess\Scripts\ChessCom'
    dte = dt.datetime.now().strftime("%Y%m%d%H%M%S")

    conn = sql.connect('Driver={ODBC Driver 17 for SQL Server};Server=HUNT-PC1;Database=ChessAnalysis;Trusted_Connection=yes;')        
    qry_text = "SELECT ISNULL(LastName, '') + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE EEHFlag = 0 AND Source = 'Chess.com' AND DownloadFlag = 1"
    users = pd.read_sql(qry_text, conn).values.tolist()
    rec_ct = len(users)
    conn.close()

    if rec_ct > 0:
        # get pgns
        for i in users:
            archive_url = 'https://api.chess.com/pub/player/' + i[1] + '/games/archives'
            try:
                json_data = request.urlopen(archive_url).read()
                archive_list = json.loads(json_data)
                chk = 1
            except error.HTTPError.code as e:
                err = e.getcode()
                print(str(err) + ' error on ' + i[1])
                chk = 0
            if chk == 1:
                for url in archive_list['archives']:
                    dload_url = url + '/pgn'
                    search_start = '/games/'
                    start = url.find(search_start) + len(search_start)
                    yyyy = url[start:start+4]
                    mm = url[-2:]
                    dload_name = i[1] + '_' + yyyy + mm + '.pgn'
                    dload_file = os.path.join(dload_path, dload_name)
                    request.urlretrieve(dload_url, dload_file)
                    with open(dload_file, mode='r', encoding='utf-8', errors='ignore') as dl:
                        lines = dl.read()
                    txt_old = '"' + i[1] + '"'
                    txt_new = '"' + i[0] + '"'
                    lines = re.sub(txt_old, txt_new, lines, flags=re.IGNORECASE)
                    with open(dload_file, mode='w', encoding='utf-8', errors='ignore') as dl:
                        dl.write(lines)

        # merge and clean pgns
        if rec_ct == 1:
            merge_name = 'ChessCom_' + users[0][0] + '_Merged_' + dte + '.pgn'
            clean_name = 'ChessCom_' + users[0][0] + '_AllGames_' + dte + '.pgn'
        else:
            merge_name = 'ChessCom_Multiple_Merged_' + dte + '.pgn'
            clean_name = 'ChessCom_Multiple_AllGames_' + dte + '.pgn'

        cmd_text = 'copy /B *.pgn ' + merge_name
        if os.getcwd != dload_path:
            os.chdir(dload_path)
        os.system('cmd /C ' + cmd_text)

        cmd_text = 'pgn-extract -C -N -V -D -pl2 --quiet --nosetuptags --output ' + clean_name + ' ' + merge_name
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
        output_path = r'C:\Users\eehunt\Documents\Chess\Scripts\output'
        old_loc = os.path.join(dload_path, clean_name)
        new_loc = os.path.join(output_path, clean_name)
        os.rename(old_loc, new_loc)
    
    print('Chess.com game download complete')

def processfiles():
    dte = dt.datetime.now().strftime("%Y%m%d%H%M%S")
    output_path = r'C:\Users\eehunt\Documents\Chess\Scripts\output'
    file_list = [f for f in os.listdir(output_path) if os.path.isfile(os.path.join(output_path, f))]

    name_set = set()
    for f in file_list:
        # this allows to extract names/usernames that might have an "_" character in them
        s_idx = f.index('_') + 1
        e_idx = f.index('_AllGames_')
        nm = f[s_idx:e_idx]
        name_set.add(nm)

    player_name = list(name_set)[0]
    merge_name = player_name + '_AllGames_' + dte + '.pgn'
    
    cmd_text = 'copy /B *.pgn ' + merge_name
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    # delete old files
    dir_files = [f for f in os.listdir(output_path) if os.path.isfile(os.path.join(output_path, f))]
    for filename in dir_files:
        if filename != merge_name:
            fname_relpath = os.path.join(output_path, filename)
            os.remove(fname_relpath)
    
    # sort game file
    pgn = open(merge_name, mode='r', encoding='utf-8', errors='ignore')

    idx = []
    game_date = []
    game_text = []
    gm_idx = 0
    gm_txt = chess.pgn.read_game(pgn)
    while gm_txt is not None:
        idx.append(gm_idx)
        game_date.append(gm_txt.headers['Date'])
        game_text.append(gm_txt)
        gm_txt = chess.pgn.read_game(pgn)
        gm_idx = gm_idx + 1

    sort_name = os.path.splitext(merge_name)[0] + '_Sorted' + os.path.splitext(merge_name)[1]
    sort_file = open(os.path.join(output_path, sort_name), 'w')
    idx_sort = [x for _, x in sorted(zip(game_date, idx))]
    for i in idx_sort:
        txt = str(game_text[i]).encode(encoding='utf-8', errors='replace')
        sort_file.write(str(txt) + '\n\n')
    sort_file.close()  
    pgn.close()

    # create tag files
    tc_tag_file = 'TimeControlTag.txt'
    cmd_text = 'echo "TimeControl >= 180" >> ' + tc_tag_file
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    wh_tag_file = 'WhiteTag.txt'
    cmd_text = 'echo White "' + player_name + '" >> ' + wh_tag_file
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    bl_tag_file = 'BlackTag.txt'
    cmd_text = 'echo Black "' + player_name + '" >> ' + bl_tag_file
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)
    
    # update correspondence game TimeControl tag; missing from Lichess games
    updated_tc_name = os.path.splitext(sort_name)[0] + '_TimeControlFixed' + os.path.splitext(sort_name)[1]
    ofile = os.path.join(output_path, sort_name)
    nfile = os.path.join(output_path, updated_tc_name)
    searchExp = '[TimeControl "-"]'
    replaceExp = '[TimeControl "1/86400"]'
    wfile = open(nfile, 'w')
    for line in fileinput.input(ofile):
        if searchExp in line:
            line = line.replace(searchExp, replaceExp)
        wfile.write(line)
    wfile.close()
    
    """ If I want to remove bullet games, need to ensure all Daily/Correspondence games have time controls otherwise they are removed as well
    # remove bullet games
    new_file = 'NoBullet_' + merge_name
    cmd_text = 'pgn-extract --quiet -t' + tc_tag_file + ' --output ' + new_file + ' ' + updated_tc_name
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)
    """

    new_file = updated_tc_name #uncomment this line and comment out above bullet block if leaving bullet games in

    # create White file
    new_white = 'White_' + new_file
    cmd_text = 'pgn-extract --quiet -t' + wh_tag_file + ' --output ' + new_white + ' ' + new_file
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    # create Black file
    new_black = 'Black_' + new_file
    cmd_text = 'pgn-extract --quiet -t' + bl_tag_file + ' --output ' + new_black + ' ' + new_file
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    # clean up
    os.remove(os.path.join(output_path, tc_tag_file))
    os.remove(os.path.join(output_path, new_file))
    os.remove(os.path.join(output_path, wh_tag_file))
    os.remove(os.path.join(output_path, bl_tag_file))
    os.remove(os.path.join(output_path, merge_name))
    os.remove(os.path.join(output_path, sort_name))

def archiveold():
    output_path = r'C:\Users\eehunt\Documents\Chess\Scripts\output'
    archive_path = r'C:\Users\eehunt\Documents\Chess\Scripts\output\old'

    file_list = [f for f in os.listdir(output_path) if os.path.isfile(os.path.join(output_path, f))] # lists only files in directory, no subfolders
    if len(file_list) > 0:
        for file in file_list:
            old_name = os.path.join(output_path, file)
            new_name = os.path.join(archive_path, file)
            sh.move(old_name, new_name)

def main():
    archiveold()
    lichessgames()
    chesscomgames()
    processfiles()


if __name__ == '__main__':
    main()