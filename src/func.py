import json
import logging
import os
import shutil


def get_config(filepath, key):
    filename = os.path.join(filepath, 'config.json')
    with open(filename, 'r') as t:
        key_data = json.load(t)
    val = key_data.get(key)
    return val


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
                shutil.move(old_name, new_name)

            logging.info(f'Old files archived to {os.path.normpath(archive_path)}')


def check_backdoor(player, site):
    # validate and verify if custom dataset is to be downloaded
    if len(player) == 1:
        if player[0] == 'CUSTOM':
            yn = yn_prompt('You are about to download a custom dataset. Continue? Y or N ===> ')
            if yn == 'N':
                logging.critical('Process terminated by user!')
                raise SystemExit
        else:
            if site is None:
                logging.critical(f'Player username {player[0]} was provided but no site specified')
                raise SystemExit


def yn_prompt(prompt):
    # general Yes/No prompt
    yn = ''
    yn_val = ['Y', 'N']
    ct = 0
    while yn not in yn_val:
        yn = input(prompt)
        yn = yn.upper()
        if yn not in yn_val:
            ct = ct + 1
            if ct < 3:
                logging.warning(f'Parameter "{yn}" is invalid, please try again!')
            else:
                logging.critical('Three consecutive bad parameters, process terminated!')
                raise SystemExit
    return yn
