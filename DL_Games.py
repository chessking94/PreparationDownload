import argparse
import datetime as dt
import fileinput
import json
import os
import re
import shutil as sh

import chess
import chess.pgn
import dateutil.parser as dtp
import pandas as pd
import pyodbc as sql
import requests

NM_DELIM = '$$' # hard to find a good delimiter that might not be used in a username or is an escape character in Windows

def archive_old(outpath):
    # move any files sitting in the output folder to an archive
    output_path = os.path.join(outpath, 'output')
    archive_path = os.path.join(output_path, 'archive')

    if os.path.isdir(output_path):
        file_list = [f for f in os.listdir(output_path) if os.path.isfile(os.path.join(output_path, f))]
        if len(file_list) > 0:
            if not os.path.isdir(archive_path):
                os.mkdir(archive_path)
            for file in file_list:
                old_name = os.path.join(output_path, file)
                new_name = os.path.join(archive_path, file)
                sh.move(old_name, new_name)
            
            print('Old files archived to ' + archive_path)

def check_backdoor(player, site):
    # validate and verify if custom dataset is to be downloaded
    if len(player) == 1:
        if player[0] == 'CUSTOM':
            yn = yn_prompt('You are about to download a custom dataset. Continue? Y or N ===> ')
            if yn == 'N':
                print('Process terminated by user!')
                quit()
        else:
            if site is None:
                raise RuntimeError('Player username ' + player[0] + ' was provided but no site specified')

def chesscom_games(name, basepath):
    # download Chess.com user games
    dload_path = os.path.join(basepath, 'ChessCom')
    if not os.path.isdir(dload_path):
        os.mkdir(dload_path)
    dte = dt.datetime.now().strftime('%Y%m%d%H%M%S')

    conn_str = get_connstr()
    conn = sql.connect(conn_str)
    if len(name) == 1:
        if name[0].upper() == 'CUSTOM': # backdoor to allow me to download custom datasets based on the original Excel selection process
            qry_text = "SELECT ISNULL(LastName, '') + '" + NM_DELIM + "' + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE Source = 'Chess.com' AND DownloadFlag = 1"
        else:
            qry_text = "SELECT ISNULL(LastName, '') + '" + NM_DELIM + "' + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE Source = 'Chess.com' AND Username = '" + name[0] + "'"
    else:
        qry_text = "SELECT ISNULL(LastName, '') + '" + NM_DELIM + "' + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE Source = 'Chess.com' AND LastName = '" + name[0] + "' AND FirstName = '" + name[1] + "'"
    users = pd.read_sql(qry_text, conn).values.tolist()
    rec_ct = len(users)
    conn.close()

    repl_nm = 1
    if len(name) == 1 and name[0].upper() != 'CUSTOM' and rec_ct == 0: # username was passed, not in SQL table
        yn = yn_prompt('A username was passed but not found in the SQL reference table. Force download and continue? Y or N ===> ')
        if yn == 'N':
            print('Process terminated by user!')
            quit()
        users = [[name[0], name[0]]]
        rec_ct = len(users)
        repl_nm = 0

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
                            if repl_nm:
                                txt_old = '"' + i[1] + '"'
                                txt_new = '"' + i[0].replace(NM_DELIM, ', ') + '"'
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

        cmd_text = 'copy /B *.pgn ' + merge_name + ' >nul'
        if os.getcwd != dload_path:
            os.chdir(dload_path)
        os.system('cmd /C ' + cmd_text)

        # seems like pgn-extract is still writing parsing errors to stdout, can I suppress? if so, would need to do it here, below, and in LIchess block
        cmd_text = 'pgn-extract -C -N -V -D -pl2 --quiet --nosetuptags --output ' + clean_name + ' ' + merge_name + ' >nul'
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
                variant_tag = gm_txt.headers['Variant']
            except:
                variant_tag = 'Standard'
            if variant_tag == 'Standard':
                txt = str(gm_txt).encode(encoding='utf-8', errors='replace')
                pgn_new.write(str(txt) + '\n\n')
            gm_txt = chess.pgn.read_game(pgn)
        pgn.close()
        pgn_new.close()

        #""" should be irrelevant now that I am replacing errors when I open/sort above, instead of ignoring
        # need to rerun a dummy pgn-extract basically to reformat file from bytes to standard pgn
        updated_clean_name2 = os.path.splitext(updated_clean_name)[0] + 's' + os.path.splitext(updated_clean_name)[1]
        cmd_text = 'pgn-extract -C -N -V -D -pl2 --quiet --nosetuptags --output ' + updated_clean_name2 + ' ' + updated_clean_name + ' >nul'
        if os.getcwd != dload_path:
            os.chdir(dload_path)
        os.system('cmd /C ' + cmd_text)
        #"""

        # delete old files
        dir_files = [f for f in os.listdir(dload_path) if os.path.isfile(os.path.join(dload_path, f))]
        for filename in dir_files:
            if filename != updated_clean_name2:
                fname_relpath = os.path.join(dload_path, filename)
                os.remove(fname_relpath)
        
        # move to new folder
        output_path = os.path.join(basepath, 'output')
        if not os.path.isdir(output_path):
            os.mkdir(output_path)
        old_loc = os.path.join(dload_path, updated_clean_name2)
        new_loc = os.path.join(output_path, updated_clean_name2)
        os.rename(old_loc, new_loc)
        print('Chess.com game download complete')
    else:
        print('No Chess.com games to download')

def format_date(date_string):
    # format dates in the PGN standard yyyy.mm.dd format
    try:
        dte = dt.datetime.strftime(dtp.parse(date_string), '%Y.%m.%d') if date_string is not None else None
    except:
        dte = None
        print('Unable to parse ' + date_string + ' as date, ignoring parameter')
    return dte

def get_connstr():
    # get SQL Server connection string from private file
    fpath = r'C:\Users\eehunt\Repository'
    fname = 'confidential.json'
    with open(os.path.join(fpath, fname), 'r') as t:
        key_data = json.load(t)
    conn_str = key_data.get('SqlServerConnectionStringTrusted')
    return conn_str

def get_lichesstoken():
    # get Lichess API token from private file
    fpath = r'C:\Users\eehunt\Repository'
    fname = 'confidential.json'
    with open(os.path.join(fpath, fname), 'r') as t:
        key_data = json.load(t)
    token_value = key_data.get('LichessAPIToken')
    return token_value

def get_timecontrollimits(timecontrol, limit):
    # get minimum or maximum number of seconds to be referenced for a given time control
    # ranges in seconds determined after reviewing Chess.com and Lichess criteria
    tc_dict = {}
    tc_dict['Bullet'] = {'Min': '60', 'Max': '179'}
    tc_dict['Blitz'] = {'Min': '180', 'Max': '600'}
    tc_dict['Rapid'] = {'Min': '601', 'Max': '1799'}
    tc_dict['Classical'] = {'Min': '1800', 'Max': '86399'}
    tc_dict['Correspondence'] = {'Min': '86400', 'Max': '1209600'}
    return tc_dict[timecontrol].get(limit)

def lichess_games(name, basepath):
    # download Lichess user games
    dload_path = os.path.join(basepath, 'Lichess')
    if not os.path.isdir(dload_path):
        os.mkdir(dload_path)

    conn_str = get_connstr()
    conn = sql.connect(conn_str)
    if len(name) == 1:
        if name[0].upper() == 'CUSTOM': # backdoor to allow me to download custom datasets based on the original Excel selection process
            qry_text = "SELECT ISNULL(LastName, '') + '" + NM_DELIM + "' + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE Source = 'Lichess' AND DownloadFlag = 1"
        else:
            qry_text = "SELECT ISNULL(LastName, '') + '" + NM_DELIM + "' + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE Source = 'Lichess' AND Username = '" + name[0] + "'"
    else:
        qry_text = "SELECT ISNULL(LastName, '') + '" + NM_DELIM + "' + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE Source = 'Lichess' AND LastName = '" + name[0] + "' AND FirstName = '" + name[1] + "'"
    users = pd.read_sql(qry_text, conn).values.tolist()
    rec_ct = len(users)
    conn.close()

    repl_nm = 1
    if len(name) == 1 and name[0].upper() != 'CUSTOM' and rec_ct == 0: # username was passed, not in SQL table
        yn = yn_prompt('A username was passed but not found in the SQL reference table. Force download and continue? Y or N ===> ')
        if yn == 'N':
            print('Process terminated by user!')
            quit()
        users = [[name[0], name[0]]]
        rec_ct = len(users)
        repl_nm = 0

    if rec_ct > 0:
        token_value = get_lichesstoken()

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
                    if repl_nm:
                        txt_old = '"' + i[1] + '"'
                        txt_new = '"' + i[0].replace(NM_DELIM, ', ') + '"'
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
            cmd_text = 'copy /B *.pgn ' + merge_name + ' >nul'
            if os.getcwd != dload_path:
                os.chdir(dload_path)
            os.system('cmd /C ' + cmd_text)

        cmd_text = 'pgn-extract -C -N -V -D -pl2 --quiet --nosetuptags --output ' + clean_name + ' ' + merge_name + ' >nul'
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
        print('Lichess game download complete')
    else:
        print('No Lichess games to download')

def process_games(basepath, timecontrol, startdate, enddate, color):
    # process downloaded games per specifications
    output_path = os.path.join(basepath, 'output')
    file_list = [f for f in os.listdir(output_path) if os.path.isfile(os.path.join(output_path, f))]
    
    name_set = set()
    for f in file_list:
        s_idx = f.index('_') + 1
        e_idx = f.index('_AllGames_') # this allows to extract names/usernames that might have an "_" character in them
        nm = f[s_idx:e_idx]
        name_set.add(nm)
    player_name = list(name_set)[0]

    # combine or rename file(s) downloaded
    merge_name = player_name.replace(NM_DELIM, '') + '_AllGames.pgn'
    cmd_text = 'copy /B *.pgn ' + merge_name + ' >nul'
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    # update correspondence game TimeControl tag; missing from Lichess games
    updated_tc_name = os.path.splitext(merge_name)[0] + '_tcfix' + os.path.splitext(merge_name)[1]
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

    # time control extract
    if timecontrol is not None:
        tc_min = get_timecontrollimits(timecontrol, 'Min')
        tc_max = get_timecontrollimits(timecontrol, 'Max')

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
        tmp_file = 'temp' + timecontrol + '_' + merge_name
        cmd_text = 'pgn-extract --quiet -t' + tc_tag_file_min + ' --output ' + tmp_file + ' ' + updated_tc_name + ' >nul'
        if os.getcwd != output_path:
            os.chdir(output_path)
        os.system('cmd /C ' + cmd_text)

        # filter max time control
        new_file = timecontrol + '_' + merge_name
        cmd_text = 'pgn-extract --quiet -t' + tc_tag_file_max + ' --output ' + new_file + ' ' + tmp_file + ' >nul'
        if os.getcwd != output_path:
            os.chdir(output_path)
        os.system('cmd /C ' + cmd_text)
    else:
        new_file = updated_tc_name

    # start date extract
    if startdate is not None:
        # create start date tag file
        sd_tag_file = 'StartDateTag.txt'
        sd_tag_file_full = os.path.join(output_path, sd_tag_file)
        sd_txt = 'Date >= "' + startdate + '"'
        with open(sd_tag_file_full, 'w') as sdt:
            sdt.write(sd_txt)

        # filter start date
        sd_file = 'SD_' + new_file
        cmd_text = 'pgn-extract --quiet -t' + sd_tag_file + ' --output ' + sd_file + ' ' + new_file + ' >nul'
        if os.getcwd != output_path:
            os.chdir(output_path)
        os.system('cmd /C ' + cmd_text)
    else:
        sd_file = new_file

    # end date extract
    if enddate is not None:
        # create end date tag file
        ed_tag_file = 'EndDateTag.txt'
        ed_tag_file_full = os.path.join(output_path, ed_tag_file)
        ed_txt = 'Date <= "' + enddate + '"'
        with open(ed_tag_file_full, 'w') as edt:
            edt.write(ed_txt)

        # filter end date
        ed_file = 'ED_' + sd_file
        cmd_text = 'pgn-extract --quiet -t' + ed_tag_file + ' --output ' + ed_file + ' ' + sd_file + ' >nul'
        if os.getcwd != output_path:
            os.chdir(output_path)
        os.system('cmd /C ' + cmd_text)
    else:
        ed_file = sd_file

    # sort game file
    pgn = open(os.path.join(output_path, ed_file), mode='r', encoding='utf-8', errors='replace')

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
    min_dte = game_date[idx_sort[0]].replace('.', '') if len(idx_sort) > 0 else '19000101'
    for i in idx_sort:
        txt = str(game_text[i]).encode(encoding='utf-8', errors='replace')
        sort_file.write(str(txt) + '\n\n')
    sort_file.close()  
    pgn.close()
   
    # set file names based on parameters set and split into White/Black files
    base_name = player_name.replace(NM_DELIM, '')
    if timecontrol is not None:
        base_name = base_name + '_' + timecontrol
    else:
        base_name = base_name + '_All'
    if startdate is not None:
        base_name = base_name + '_' + startdate.replace('.', '')
    else:
        base_name = base_name + '_' + min_dte
    if enddate is not None:
        base_name = base_name + '_' + enddate.replace('.', '')
    else:
        base_name = base_name + '_' + dt.datetime.now().strftime('%Y%m%d')

    new_white = base_name + '_White.pgn'
    new_black = base_name + '_Black.pgn'
    new_combined = base_name + '_Combined.pgn'

    # create white/black tag files
    wh_tag_file = 'WhiteTag.txt'
    cmd_text = 'echo White "' + player_name.replace(NM_DELIM, ', ') + '" >> ' + wh_tag_file
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    bl_tag_file = 'BlackTag.txt'
    cmd_text = 'echo Black "' + player_name.replace(NM_DELIM, ', ') + '" >> ' + bl_tag_file
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    # split into applicable color files
    if color in ['White', None]:
        cmd_text = 'pgn-extract --quiet -t' + wh_tag_file + ' --output ' + new_white + ' ' + sort_name + ' >nul'
        if os.getcwd != output_path:
            os.chdir(output_path)
        os.system('cmd /C ' + cmd_text)

    if color in ['Black', None]:
        cmd_text = 'pgn-extract --quiet -t' + bl_tag_file + ' --output ' + new_black + ' ' + sort_name + ' >nul'
        if os.getcwd != output_path:
            os.chdir(output_path)
        os.system('cmd /C ' + cmd_text)
    
    # need to run a dummy pgn-extract on new_combined if being kept, it's formatted as bytes
    if color == 'Combined':
        cmd_text = 'pgn-extract --quiet --output ' + new_combined + ' ' + sort_name + ' >nul'
        if os.getcwd != output_path:
            os.chdir(output_path)
        os.system('cmd /C ' + cmd_text)
    
    # clean up
    files_to_keep = []
    if color in ['White', None]:
        files_to_keep.append(new_white)
    if color in ['Black', None]:
        files_to_keep.append(new_black)
    if color == 'Combined':
        files_to_keep.append(new_combined)
    dir_files = [f for f in os.listdir(output_path) if os.path.isfile(os.path.join(output_path, f))]
    for filename in dir_files:
        if filename not in files_to_keep:
            fname_relpath = os.path.join(output_path, filename)
            os.remove(fname_relpath)

    print('PGN processing complete, files located at ' + output_path)

def parse_name(name):
    # return array ['Last', 'First']; otherwise ['name']
    parsed_name = []
    if ',' in name:
        name = re.sub('\,\,+', ',', name) # remove double commas
        parsed_name = [x.strip() for x in name.split(',')]
        return parsed_name
    elif ' ' in name:
        name = re.sub('\ \ +', ' ', name) # remove double spaces
        parsed_name = [x.strip() for x in name.split(' ')]
        parsed_name.reverse()
        return parsed_name
    else: # no comma, no space, must be a username
        parsed_name.append(name)
        return parsed_name

def validate_path(path, def_path):
    # verifiy path exists for game output
    ret = path
    if not os.path.isdir(path):
        yn = yn_prompt('Do you want to create the new path ' + path + ' ? Y or N ===> ')
        if yn == 'Y':
            os.mkdir(path)
        else:
            ret = def_path
    return ret

def yn_prompt(prompt):
    # general Yes/No prompt
    yn = ''
    yn_val = ['Y', 'N']
    while yn not in yn_val:
        yn = input(prompt)
        yn = yn.upper()
        if yn not in yn_val:
            print('Invalid parameter passed, please try again!')
    return yn

def main():
    # set up CLI parser
    def_path = r'C:\Users\eehunt\Documents\Chess\Scripts'
    vrs_num = '1.8'
    parser = argparse.ArgumentParser(
        description = 'Chess.com and Lichess Game Downloader',
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
        usage = argparse.SUPPRESS
    )
    parser.add_argument(
        '-v', '--version',
        action = 'version',
        version = '%(prog)s ' + vrs_num
    )
    parser.add_argument(
        '-p', '--player',
        default = 'CUSTOM',
        help = 'Player name'
    )
    parser.add_argument(
        '-s', '--site',
        default = None,
        nargs = '?',
        choices = ['Chess.com', 'Lichess'],
        help = 'Website to download games from'
    )
    parser.add_argument(
        '-t', '--timecontrol',
        default = None,
        nargs = '?',
        choices = ['Bullet', 'Blitz', 'Rapid', 'Classical', 'Correspondence'],
        help = 'Time control of games to download'
    )
    parser.add_argument(
        '-c', '--color',
        default = None,
        nargs = '?',
        choices = ['White', 'Black', 'Combined'],
        help = 'Color of player games'
    )
    parser.add_argument(
        '--startdate',
        nargs = '?',
        help = 'Do not include games before this date'
    )
    parser.add_argument(
        '--enddate',
        nargs = '?',
        help = 'Do not include games after this date'
    )
    parser.add_argument(
        '--outpath',
        default = def_path,
        help = 'Root path to output files to'
    )
    """
    Future Arguments:
    game type (variants) - Would be nice to have support for variants, but that would take more thought and be useless for my purposes
    finer details like ECO, minimum number of moves - might need some kind of pgn-extract loop thing for this, lower priority
    """
    args = parser.parse_args()
    config = vars(args)
    player = parse_name(config['player'])
    site = config['site']
    timecontrol = config['timecontrol']
    color = config['color']
    startdate = format_date(config['startdate'])
    enddate = format_date(config['enddate'])
    outpath = validate_path(config['outpath'], def_path)

    # check backdoor and validate username-only entry
    check_backdoor(player, site)

    # process request
    archive_old(outpath)
    if site in ['Lichess', None]:
        lichess_games(player, outpath)
    if site in ['Chess.com', None]:
        chesscom_games(player, outpath)
    process_games(outpath, timecontrol, startdate, enddate, color)


if __name__ == '__main__':
    main()