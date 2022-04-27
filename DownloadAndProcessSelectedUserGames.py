import datetime as dt
import pyodbc as sql
import pandas as pd
import requests
import json
import os
import re
import fileinput
import shutil as sh
import chess
import chess.pgn

# TODO: Consider adding command line arguments to specify user directly, rather than through Excel file
# Also could consider parameters like games between/since certain dates, time controls, etc

def lichessgames():
    dload_path = r'C:\Users\eehunt\Documents\Chess\Scripts\Lichess'

    conn = sql.connect('Driver={ODBC Driver 17 for SQL Server};Server=HUNT-PC1;Database=ChessAnalysis;Trusted_Connection=yes;')        
    qry_text = "SELECT ISNULL(LastName, '') + '-' + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE EEHFlag = 0 AND Source = 'Lichess' AND DownloadFlag = 1"
    users = pd.read_sql(qry_text, conn).values.tolist()
    rec_ct = len(users)
    conn.close()

    if rec_ct > 0:
        # get auth token
        fpath = r'C:\Users\eehunt\Repository'
        fname = 'keys.json'
        with open(os.path.join(fpath, fname), 'r') as f:
            json_data = json.load(f)
        token_value = json_data.get('LichessAPIToken')

        # get pgns
        for i in users:
            dte_val = dt.datetime.now().strftime('%Y%m%d%H%M%S')
            dload_url = 'https://lichess.org/api/games/user/' + i[1] + '?perfType=bullet,blitz,rapid,classical,correspondence&sort=dateAsc'
            dload_name = i[1] + '_' + dte_val + '.pgn'
            dload_file = os.path.join(dload_path, dload_name)
            hdr = {'Authorization': 'Bearer ' + token_value}
            with requests.get(dload_url, headers=hdr, stream=True) as resp:
                if resp.status_code != 200:
                    print('Unable to complete request! Request returned code ' + str(resp.status_code))
                else:
                    with open(dload_file, 'wb') as f:
                        for chunk in resp.iter_content(chunk_size=8196):
                            f.write(chunk)
                    with open(dload_file, mode='r', encoding='utf-8', errors='ignore') as dl:
                        lines = dl.read()
                    txt_old = '"' + i[1] + '"'
                    txt_new = '"' + i[0].replace('-', ', ') + '"'
                    lines = re.sub(txt_old, txt_new, lines, flags=re.IGNORECASE)
                    with open(dload_file, mode='w', encoding='utf-8', errors='ignore') as dl:
                        dl.write(lines)

        # merge and clean pgns
        dte_val = dt.datetime.now().strftime('%Y%m%d%H%M%S')
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
    dte = dt.datetime.now().strftime('%Y%m%d%H%M%S')

    conn = sql.connect('Driver={ODBC Driver 17 for SQL Server};Server=HUNT-PC1;Database=ChessAnalysis;Trusted_Connection=yes;')        
    qry_text = "SELECT ISNULL(LastName, '') + '-' + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE EEHFlag = 0 AND Source = 'Chess.com' AND DownloadFlag = 1"
    users = pd.read_sql(qry_text, conn).values.tolist()
    rec_ct = len(users)
    conn.close()
    #users = [['mohamad_kk7','mohamad_kk7']]

    if rec_ct > 0:
        # get pgns
        for i in users:
            archive_url = 'https://api.chess.com/pub/player/' + i[1] + '/games/archives'
            with requests.get(archive_url) as resp:
                if resp.status_code != 200:
                    print('Unable to complete request! Request returned code ' + resp.status_code)
                    chk = 0
                else:
                    json_data = resp.content
                    archive_list = json.loads(json_data)
                    chk = 1
            if chk == 1:
                for url in archive_list['archives']:
                    dload_url = url + '/pgn'
                    search_start = '/games/'
                    start = url.find(search_start) + len(search_start)
                    yyyy = url[start:start+4]
                    mm = url[-2:]
                    dload_name = i[1] + '_' + yyyy + mm + '.pgn'
                    dload_file = os.path.join(dload_path, dload_name)
                    with requests.get(dload_url, stream=True) as resp:
                        if resp.status_code != 200:
                            print('Unable to complete request! Request returned code ' + resp.status_code)
                        else:
                            with open(dload_file, 'wb') as f:
                                for chunk in resp.iter_content(chunk_size=8196):
                                    f.write(chunk)
                            with open(dload_file, mode='r', encoding='utf-8', errors='ignore') as dl:
                                lines = dl.read()
                            txt_old = '"' + i[1] + '"'
                            txt_new = '"' + i[0].replace('-', ', ') + '"'
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

        # remove any non-standard games remaining; Chess.com standard games omit the Variant tag
        pgn = open(os.path.join(dload_path, clean_name), mode='r', encoding='utf-8', errors='replace')
        updated_clean_name = os.path.splitext(clean_name)[0] + '_NoVariant' + os.path.splitext(clean_name)[1]
        pgn_new = open(os.path.join(dload_path, updated_clean_name), 'w')
        gm_txt = chess.pgn.read_game(pgn)
        while gm_txt is not None:
            try:
                variant_tag = gm_txt.headers["Variant"]
            except:
                variant_tag = 'Standard'
            if variant_tag == 'Standard':
                #pgn_new.write(str(gm_txt) + '\n\n')
                txt = str(gm_txt).encode(encoding='utf-8', errors='replace')
                pgn_new.write(str(txt) + '\n\n')
            gm_txt = chess.pgn.read_game(pgn)
        pgn.close()
        pgn_new.close()

        #""" should be irrelevant now that I am replacing errors when I open/sort above, instead of ignoring
        # need to rerun a dummy pgn-extract basically to reformat file from bytes to standard pgn
        updated_clean_name2 = os.path.splitext(updated_clean_name)[0] + 's' + os.path.splitext(updated_clean_name)[1]
        cmd_text = 'pgn-extract -C -N -V -D -pl2 --quiet --nosetuptags --output ' + updated_clean_name2 + ' ' + updated_clean_name
        if os.getcwd != dload_path:
            os.chdir(dload_path)
        os.system('cmd /C ' + cmd_text)
        #"""

        # delete old files
        dir_files = [f for f in os.listdir(dload_path) if os.path.isfile(os.path.join(dload_path, f))]
        for filename in dir_files:
            #if filename != updated_clean_name:
            if filename != updated_clean_name2:
                fname_relpath = os.path.join(dload_path, filename)
                os.remove(fname_relpath)
        
        # move to new folder
        output_path = r'C:\Users\eehunt\Documents\Chess\Scripts\output'
        #old_loc = os.path.join(dload_path, updated_clean_name)
        #new_loc = os.path.join(output_path, updated_clean_name)
        old_loc = os.path.join(dload_path, updated_clean_name2)
        new_loc = os.path.join(output_path, updated_clean_name2)
        os.rename(old_loc, new_loc)
    
    print('Chess.com game download complete')

def processfiles():
    dte = dt.datetime.now().strftime('%Y%m%d%H%M%S')
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
    merge_name = player_name.replace('-', '') + '_AllGames_' + dte + '.pgn'
    
    cmd_text = 'copy /B *.pgn ' + merge_name
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)
    
    # delete original files
    dir_files = [f for f in os.listdir(output_path) if os.path.isfile(os.path.join(output_path, f))]
    for filename in dir_files:
        if filename != merge_name:
            fname_relpath = os.path.join(output_path, filename)
            os.remove(fname_relpath)

    # create tag files
    wh_tag_file = 'WhiteTag.txt'
    cmd_text = 'echo White "' + player_name.replace('-', ', ') + '" >> ' + wh_tag_file
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    bl_tag_file = 'BlackTag.txt'
    cmd_text = 'echo Black "' + player_name.replace('-', ', ') + '" >> ' + bl_tag_file
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    # update correspondence game TimeControl tag; missing from Lichess games
    updated_tc_name = os.path.splitext(merge_name)[0] + '_TimeControlFixed' + os.path.splitext(merge_name)[1]
    ofile = os.path.join(output_path, merge_name)
    nfile = os.path.join(output_path, updated_tc_name)
    searchExp = '[TimeControl "-"]'
    replaceExp = '[TimeControl "1/86400"]'
    wfile = open(nfile, 'w')
    for line in fileinput.input(ofile):
        if searchExp in line:
            line = line.replace(searchExp, replaceExp)
        wfile.write(line)
    wfile.close()

    # option for filtering to certain time controls
    tc_filter_option = False
    tc_type = 'Blitz'
    tc_options = ['Bullet', 'Blitz', 'Rapid', 'Classical', 'Correspondence']
    # range for each time control, in seconds; values taken after reviewing Chess.com and Lichess criteria
    tc_min_list = ['60', '180', '601', '1800', '86400']
    tc_max_list = ['179', '600', '1799', '86399', '1209600']
    if tc_filter_option and tc_type in tc_options:
        i = 0
        for t in tc_options:
            if t == tc_type:
                break # exit for loop; i will be the index needed
            i = i + 1
        tc_min = tc_min_list[i]
        tc_max = tc_max_list[i]

        # create time control tag files
        tc_tag_file_min = 'TimeControlTagMin.txt'
        tc_tag_file_min_full = os.path.join(output_path, tc_tag_file_min)
        tc_txt = 'TimeControl >= "' + tc_min + '"'
        with open(tc_tag_file_min_full, 'w') as mn:
            mn.write(tc_txt)
        
        tc_tag_file_max = 'TimeControlTagMax.txt'
        tc_tag_file_max_full = os.path.join(output_path, tc_tag_file_max)
        tc_txt = 'TimeControl <= "' + tc_max + '"'
        with open(tc_tag_file_max_full, 'w') as mx:
            mx.write(tc_txt)
      
        # filter min time control
        tmp_file = 'temp' + tc_type + '_' + merge_name
        cmd_text = 'pgn-extract --quiet -t' + tc_tag_file_min + ' --output ' + tmp_file + ' ' + updated_tc_name
        if os.getcwd != output_path:
            os.chdir(output_path)
        os.system('cmd /C ' + cmd_text)

        # filter max time control
        new_file = tc_type + '_' + merge_name
        cmd_text = 'pgn-extract --quiet -t' + tc_tag_file_max + ' --output ' + new_file + ' ' + tmp_file
        if os.getcwd != output_path:
            os.chdir(output_path)
        os.system('cmd /C ' + cmd_text)
    else:
        new_file = updated_tc_name

    # sort game file
    pgn = open(os.path.join(output_path, new_file), mode='r', encoding='utf-8', errors='replace')

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
    
    sort_name = os.path.splitext(updated_tc_name)[0] + '_Sorted' + os.path.splitext(updated_tc_name)[1]
    sort_file = open(os.path.join(output_path, sort_name), 'w')
    idx_sort = [x for _, x in sorted(zip(game_date, idx))]
    for i in idx_sort:
        #txt = str(game_text[i])
        txt = str(game_text[i]).encode(encoding='utf-8', errors='replace') # need to serious review codec issues, Jordan Timm has some funky characters in his games
        sort_file.write(str(txt) + '\n\n')
    sort_file.close()  
    pgn.close()

    # create White file
    if tc_filter_option and tc_type in tc_options:
        new_white = 'White_' + player_name.replace('-', '') + '_' + tc_type + '_' + dte + '.pgn'
        new_black = 'Black_' + player_name.replace('-', '') + '_' + tc_type + '_' + dte + '.pgn'
    else:
        new_white = 'White_' + player_name.replace('-', '') + '_All_' + dte + '.pgn'
        new_black = 'Black_' + player_name.replace('-', '') + '_All_' + dte + '.pgn'

    cmd_text = 'pgn-extract --quiet -t' + wh_tag_file + ' --output ' + new_white + ' ' + sort_name
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    # create Black file
    cmd_text = 'pgn-extract --quiet -t' + bl_tag_file + ' --output ' + new_black + ' ' + sort_name
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)
    
    # clean up
    if tc_filter_option and tc_type in tc_options:
        os.remove(os.path.join(output_path, updated_tc_name))
        os.remove(os.path.join(output_path, tc_tag_file_min))
        os.remove(os.path.join(output_path, tc_tag_file_max))
        os.remove(os.path.join(output_path, tmp_file))
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