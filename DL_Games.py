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
import argparse
import dateutil.parser as dtp

def lichessgames(name):
    dload_path = r'C:\Users\eehunt\Documents\Chess\Scripts\Lichess'

    conn = sql.connect('Driver={ODBC Driver 17 for SQL Server};Server=HUNT-PC1;Database=ChessAnalysis;Trusted_Connection=yes;')
    if len(name) == 1:
        if name[0].upper() == 'CUSTOM': # backdoor to allow me to download custom datasets based on the original Excel selection process
            qry_text = "SELECT ISNULL(LastName, '') + '-' + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE Source = 'Lichess' AND DownloadFlag = 1"
        else:
            qry_text = "SELECT ISNULL(LastName, '') + '-' + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE Source = 'Lichess' AND Username = '" + name[0] + "'"
    else:
        qry_text = "SELECT ISNULL(LastName, '') + '-' + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE Source = 'Lichess' AND LastName = '" + name[0] + "' AND FirstName = '" + name[1] + "'"
    users = pd.read_sql(qry_text, conn).values.tolist()
    rec_ct = len(users)
    conn.close()

    repl_nm = 1
    if len(name) == 1 and name[0].upper() != 'CUSTOM' and rec_ct == 0: # username was passed, not in SQL table
        yn = yn_prompt('A username was passed but not found in the SQL reference table. Force download and continue? Y or N ===> ')
        if yn == 'Y':
            users = [[name[0], name[0]]]
            rec_ct = len(users)
            repl_nm = 0

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
                    if repl_nm:
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
        output_path = r'C:\Users\eehunt\Documents\Chess\Scripts\output'
        old_loc = os.path.join(dload_path, clean_name)
        new_loc = os.path.join(output_path, clean_name)
        os.rename(old_loc, new_loc)
        print('Lichess game download complete')
    else:
        print('No Lichess games to download')

def chesscomgames(name):
    dload_path = r'C:\Users\eehunt\Documents\Chess\Scripts\ChessCom'
    dte = dt.datetime.now().strftime('%Y%m%d%H%M%S')

    conn = sql.connect('Driver={ODBC Driver 17 for SQL Server};Server=HUNT-PC1;Database=ChessAnalysis;Trusted_Connection=yes;')        
    if len(name) == 1:
        if name[0].upper() == 'CUSTOM': # backdoor to allow me to download custom datasets based on the original Excel selection process
            qry_text = "SELECT ISNULL(LastName, '') + '-' + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE Source = 'Chess.com' AND DownloadFlag = 1"
        else:
            qry_text = "SELECT ISNULL(LastName, '') + '-' + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE Source = 'Chess.com' AND Username = '" + name[0] + "'"
    else:
        qry_text = "SELECT ISNULL(LastName, '') + '-' + ISNULL(FirstName, '') AS PlayerName, Username FROM UsernameXRef WHERE Source = 'Chess.com' AND LastName = '" + name[0] + "' AND FirstName = '" + name[1] + "'"
    users = pd.read_sql(qry_text, conn).values.tolist()
    rec_ct = len(users)
    conn.close()

    repl_nm = 1
    if len(name) == 1 and name[0].upper() != 'CUSTOM' and rec_ct == 0: # username was passed, not in SQL table
        yn = yn_prompt('A username was passed but not found in the SQL reference table. Force download and continue? Y or N ===> ')
        if yn == 'Y':
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
                #pgn_new.write(str(gm_txt) + '\n\n')
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
        output_path = r'C:\Users\eehunt\Documents\Chess\Scripts\output'
        old_loc = os.path.join(dload_path, updated_clean_name2)
        new_loc = os.path.join(output_path, updated_clean_name2)
        os.rename(old_loc, new_loc)
        print('Chess.com game download complete')
    else:
        print('No Chess.com games to download')

def processfiles(timecontrol):
    dte = dt.datetime.now().strftime('%Y%m%d%H%M%S')
    output_path = r'C:\Users\eehunt\Documents\Chess\Scripts\output'
    file_list = [f for f in os.listdir(output_path) if os.path.isfile(os.path.join(output_path, f))]
    
    name_set = set()
    for f in file_list: # this allows to extract names/usernames that might have an "_" character in them
        s_idx = f.index('_') + 1
        e_idx = f.index('_AllGames_') # may need to revist this part, if a time control is specified if might not read this way
        nm = f[s_idx:e_idx]
        name_set.add(nm)

    player_name = list(name_set)[0]
    merge_name = player_name.replace('-', '') + '_AllGames_' + dte + '.pgn'
    
    cmd_text = 'copy /B *.pgn ' + merge_name + ' >nul'
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

    tc_type = timecontrol
    tc_options = ['Bullet', 'Blitz', 'Rapid', 'Classical', 'Correspondence']
    # range for each time control, in seconds; values taken after reviewing Chess.com and Lichess criteria
    tc_min_list = ['60', '180', '601', '1800', '86400']
    tc_max_list = ['179', '600', '1799', '86399', '1209600']
    if tc_type in tc_options:
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
        cmd_text = 'pgn-extract --quiet -t' + tc_tag_file_min + ' --output ' + tmp_file + ' ' + updated_tc_name + ' >nul'
        if os.getcwd != output_path:
            os.chdir(output_path)
        os.system('cmd /C ' + cmd_text)

        # filter max time control
        new_file = tc_type + '_' + merge_name
        cmd_text = 'pgn-extract --quiet -t' + tc_tag_file_max + ' --output ' + new_file + ' ' + tmp_file + ' >nul'
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
    if tc_type in tc_options:
        new_white = 'White_' + player_name.replace('-', '') + '_' + tc_type + '_' + dte + '.pgn'
        new_black = 'Black_' + player_name.replace('-', '') + '_' + tc_type + '_' + dte + '.pgn'
    else:
        new_white = 'White_' + player_name.replace('-', '') + '_All_' + dte + '.pgn'
        new_black = 'Black_' + player_name.replace('-', '') + '_All_' + dte + '.pgn'

    cmd_text = 'pgn-extract --quiet -t' + wh_tag_file + ' --output ' + new_white + ' ' + sort_name + ' >nul'
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    # create Black file
    cmd_text = 'pgn-extract --quiet -t' + bl_tag_file + ' --output ' + new_black + ' ' + sort_name + ' >nul'
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)
    
    # clean up
    if tc_type in tc_options:
        os.remove(os.path.join(output_path, updated_tc_name))
        os.remove(os.path.join(output_path, tc_tag_file_min))
        os.remove(os.path.join(output_path, tc_tag_file_max))
        os.remove(os.path.join(output_path, tmp_file))
    os.remove(os.path.join(output_path, new_file))
    os.remove(os.path.join(output_path, wh_tag_file))
    os.remove(os.path.join(output_path, bl_tag_file))
    os.remove(os.path.join(output_path, merge_name))
    os.remove(os.path.join(output_path, sort_name))

    print('PGN processing complete, files located at ' + output_path)

def archiveold():
    output_path = r'C:\Users\eehunt\Documents\Chess\Scripts\output'
    archive_path = os.path.join(output_path, 'archive')

    if os.path.isdir(output_path):
        file_list = [f for f in os.listdir(output_path) if os.path.isfile(os.path.join(output_path, f))] # lists only files in directory, no subfolders
        if len(file_list) > 0:
            if not os.path.isdir(archive_path):
                os.mkdir(archive_path)
            for file in file_list:
                old_name = os.path.join(output_path, file)
                new_name = os.path.join(archive_path, file)
                sh.move(old_name, new_name)
            
            print('Old files archived to ' + archive_path)
    else: # TODO - Add better error handling here
        print('Output path does not exist!')
        quit()

def parse_name(name): # return array ['Last', 'First']; otherwise ['name']
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

def validate_site(site):
    site_array = ['Chess.Com', 'Lichess']
    ret = None
    if site is not None:
        site_val = site.title()
        if site_val not in site_array:
            ret = None
            print('Unable to validate ' + site + ' to a site, ignoring parameter')
        else:
            ret = site_val
    return ret

def yn_prompt(prompt):
    yn = ''
    yn_val = ['Y', 'N']
    while yn not in yn_val:
        yn = input(prompt)
        yn = yn.upper()
        if yn not in yn_val:
            print('Invalid parameter passed, please try again!')
    if yn == 'N':
        print('Process terminated by user')
    return yn

def validate_timecontrol(tc):
    tc_array = ['Bullet', 'Blitz', 'Rapid', 'Classical', 'Correspondence']
    ret = None
    if tc is not None:
        tc_val = tc.title()
        if tc_val not in tc_array:
            print('Unable to validate ' + tc + ' to a time control, ignoring parameter')
        else:
            ret = tc_val
    return ret

def format_date(date_string):
    try:
        dte = dt.datetime.strftime(dtp.parse(date_string), '%Y.%m.%d') if date_string is not None else None
    except:
        dte = None
        print('Unable to parse ' + date_string + ' as date, ignoring parameter')
    return dte

def validate_color(color):
    color_array = ['White', 'Black']
    ret = None
    if color is not None:
        col = color.title()
        if col not in color_array:
            print('Unable to validate ' + color + ' to a color, ignoring parameter')
        else:
            ret = col
    return ret

def main():
    # set up CLI parser
    parser = argparse.ArgumentParser(description = 'Chess.com and Lichess Game Downloader', formatter_class = argparse.ArgumentDefaultsHelpFormatter, usage = argparse.SUPPRESS)
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 1.0')
    parser.add_argument('-p', '--player', default = 'CUSTOM', help = 'Player name')
    parser.add_argument('-s', '--site', nargs = '?', help = 'Game website: Chess.com|Lichess')
    parser.add_argument('-t', '--timecontrol', nargs = '?', help = 'Time control: Bullet|Blitz|Rapid|Classical|Correspondence')
    #parser.add_argument('--startdate', nargs = '?', help = 'Start date')
    #parser.add_argument('--enddate', nargs = '?', help = 'End date')
    #parser.add_argument('-c', '--color', nargs = '?', help = 'Color: White|Black')
    """
    Future Arguments:
    game type (variants) - Would be nice to have support for variants, but that would take more thought and be useless for my purposes
    finer details like ECO, minimum number of moves - might need some kind of pgn-extract loop thing for this, lower priority
    specific output folder - Want to modularize as much as possible and construct all paths in script, rather than relying on existing paths
    """
    args = parser.parse_args()
    config = vars(args)
    player = parse_name(config['player'])
    site = validate_site(config['site'])
    timecontrol = validate_timecontrol(config['timecontrol'])
    #startdate = format_date(config['startdate'])
    #enddate = format_date(config['enddate'])
    #color = validate_timecontrol(config['color'])

    # check backdoor and validate username-only entry
    if len(player) == 1:
        if player[0] == 'CUSTOM':
            yn = yn_prompt('You are about to download a custom dataset. Continue? Y or N ===> ')
            if yn == 'N':
                quit()
        else:
            if site is None:
                raise RuntimeError('Player username ' + player[0] + ' was provided but no site specified')

    # process request
    archiveold()
    if site == 'Lichess':
        lichessgames(player)
    elif site == 'Chess.Com':
        chesscomgames(player)
    else:
        lichessgames(player)
        chesscomgames(player)
    processfiles(timecontrol) # the "right" way to do this would be to pass the dates to the download step, but would complicate the process


if __name__ == '__main__':
    main()