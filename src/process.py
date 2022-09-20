import datetime as dt
import fileinput
import logging
import os

import chess
import chess.pgn

import func


def process_games(basepath, timecontrol, startdate, enddate, color):
    # process downloaded games per specifications
    nd = func.get_config(os.path.dirname(os.path.dirname(__file__)), 'nameDelimiter')
    output_path = os.path.join(basepath, 'output')
    file_list = [f for f in os.listdir(output_path) if os.path.isfile(os.path.join(output_path, f))]

    name_set = set()
    for f in file_list:
        s_idx = f.index('_') + 1
        e_idx = f.index('_AllGames_')  # this allows to extract names/usernames that might have an "_" character in them
        nm = f[s_idx:e_idx]
        name_set.add(nm)
    player_name = list(name_set)[0]

    # combine or rename file(s) downloaded
    merge_name = f"{player_name.replace(nd, '')}_AllGames.pgn"
    cmd_text = f'copy /B *.pgn {merge_name} >nul'
    logging.debug(cmd_text)
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    # update correspondence game TimeControl tag; missing from Lichess games
    updated_tc_name = os.path.splitext(merge_name)[0] + '_tcfix' + os.path.splitext(merge_name)[1]
    ofile = os.path.join(output_path, merge_name)
    nfile = os.path.join(output_path, updated_tc_name)
    searchExp = '[TimeControl "-"]'
    replaceExp = '[TimeControl "1/86400"]'
    wfile = open(nfile, mode='w', encoding='utf-8', errors='replace')
    for line in fileinput.input(ofile, openhook=fileinput.hook_encoded('utf-8')):
        if searchExp in line:
            line = line.replace(searchExp, replaceExp)
        wfile.write(line)
    wfile.close()

    # time control extract
    if timecontrol is not None:
        logging.debug(f'TimeControl: {timecontrol}')
        tc_min = func.get_timecontrollimits(timecontrol, 'Min')
        tc_max = func.get_timecontrollimits(timecontrol, 'Max')

        # create time control tag files
        tc_tag_file_min = 'TimeControlTagMin.txt'
        tc_tag_file_min_full = os.path.join(output_path, tc_tag_file_min)
        tc_txt = 'TimeControl >= "' + tc_min + '"'
        with open(tc_tag_file_min_full, 'w') as mn:
            mn.write(tc_txt)

        tc_tag_file_max = 'TimeControlTagMax.txt'
        tc_tag_file_max_full = os.path.join(output_path, tc_tag_file_max)
        tc_txt = f'TimeControl <= "{tc_max}"'
        with open(tc_tag_file_max_full, 'w') as mx:
            mx.write(tc_txt)

        # filter min time control
        tmp_file = f'temp{timecontrol}_{merge_name}'
        cmd_text = f'pgn-extract --quiet -t{tc_tag_file_min} --output {tmp_file} {updated_tc_name} >nul'
        logging.debug(cmd_text)
        if os.getcwd != output_path:
            os.chdir(output_path)
        os.system('cmd /C ' + cmd_text)

        # filter max time control
        new_file = f'{timecontrol}_{merge_name}'
        cmd_text = f'pgn-extract --quiet -t{tc_tag_file_max} --output {new_file} {tmp_file} >nul'
        logging.debug(cmd_text)
        if os.getcwd != output_path:
            os.chdir(output_path)
        os.system('cmd /C ' + cmd_text)
    else:
        new_file = updated_tc_name

    # start date extract
    if startdate is not None:
        logging.debug(f'Start date: {startdate}')
        # create start date tag file
        sd_tag_file = 'StartDateTag.txt'
        sd_tag_file_full = os.path.join(output_path, sd_tag_file)
        sd_txt = f'Date >= "{startdate}"'
        with open(sd_tag_file_full, 'w') as sdt:
            sdt.write(sd_txt)

        # filter start date
        sd_file = f'SD_{new_file}'
        cmd_text = f'pgn-extract --quiet -t{sd_tag_file} --output {sd_file} {new_file} >nul'
        logging.debug(cmd_text)
        if os.getcwd != output_path:
            os.chdir(output_path)
        os.system('cmd /C ' + cmd_text)
    else:
        sd_file = new_file

    # end date extract
    if enddate is not None:
        logging.debug(f'End date: {enddate}')
        # create end date tag file
        ed_tag_file = 'EndDateTag.txt'
        ed_tag_file_full = os.path.join(output_path, ed_tag_file)
        ed_txt = f'Date <= "{enddate}"'
        with open(ed_tag_file_full, 'w') as edt:
            edt.write(ed_txt)

        # filter end date
        ed_file = f'ED_{sd_file}'
        cmd_text = f'pgn-extract --quiet -t{ed_tag_file} --output {ed_file} {sd_file} >nul'
        logging.debug(cmd_text)
        if os.getcwd != output_path:
            os.chdir(output_path)
        os.system('cmd /C ' + cmd_text)
    else:
        ed_file = sd_file

    # sort game file
    pgn = open(os.path.join(output_path, ed_file), mode='r', encoding='utf-8')

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
    sort_file = open(os.path.join(output_path, sort_name), 'w', encoding='utf-8')
    idx_sort = [x for _, x in sorted(zip(game_date, idx))]
    min_dte = game_date[idx_sort[0]].replace('.', '') if len(idx_sort) > 0 else '19000101'
    for i in idx_sort:
        sort_file.write(str(game_text[i]) + '\n\n')
    sort_file.close()
    pgn.close()

    # set file names based on parameters set and split into White/Black files
    base_name = player_name.replace(nd, '')
    if timecontrol is not None:
        base_name = f'{base_name}_{timecontrol}'
    else:
        base_name = f'{base_name}_All'
    if startdate is not None:
        base_name = f"{base_name}_{startdate.replace('.', '')}"
    else:
        base_name = f'{base_name}_{min_dte}'
    if enddate is not None:
        base_name = f"{base_name}_{enddate.replace('.', '')}"
    else:
        base_name = f"{base_name}_{dt.datetime.now().strftime('%Y%m%d')}"

    new_white = f'{base_name}_White.pgn'
    new_black = f'{base_name}_Black.pgn'
    new_combined = f'{base_name}_Combined.pgn'

    # count games in sort_name for return value
    game_ct = 0
    search_text = '[Event "'
    with open(os.path.join(output_path, sort_name), 'r', encoding='utf-8') as f:
        for line in f:
            if search_text in line:
                game_ct = game_ct + 1

    # create white/black tag files
    wh_tag_file = 'WhiteTag.txt'
    cmd_text = 'echo White "' + player_name.replace(nd, ', ') + '" >> ' + wh_tag_file
    logging.debug(cmd_text)
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    bl_tag_file = 'BlackTag.txt'
    cmd_text = 'echo Black "' + player_name.replace(nd, ', ') + '" >> ' + bl_tag_file
    logging.debug(cmd_text)
    if os.getcwd != output_path:
        os.chdir(output_path)
    os.system('cmd /C ' + cmd_text)

    # split into applicable color files
    if color in ['White', None]:
        cmd_text = f'pgn-extract --quiet -t{wh_tag_file} --output {new_white} {sort_name} >nul'
        logging.debug(cmd_text)
        if os.getcwd != output_path:
            os.chdir(output_path)
        os.system('cmd /C ' + cmd_text)

    if color in ['Black', None]:
        cmd_text = f'pgn-extract --quiet -t{bl_tag_file} --output {new_black} {sort_name} >nul'
        logging.debug(cmd_text)
        if os.getcwd != output_path:
            os.chdir(output_path)
        os.system('cmd /C ' + cmd_text)

    # rename combined file
    os.rename(os.path.join(output_path, sort_name), os.path.join(output_path, new_combined))

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

    logging.info(f'PGN processing complete, files located at {os.path.normpath(output_path)}')

    return game_ct
