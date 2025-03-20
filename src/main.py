import argparse
import datetime as dt
import logging
import os

import func
import queries
import validation as v
from chesscom import chesscom_games
from lichess import lichess_games
from process import process_games

# TODO: Support for variants
# TODO: Additional arguments for ECO, minimum number of moves, etc. Might require pgn-extract loops
# TODO: Casing; pgn-extract apparently needs exact upper/lower case for parsing usernames. Look into returning proper casing from API call if successful


def main():
    logging.basicConfig(
        format='%(asctime)s\t%(funcName)s\t%(levelname)s\t%(message)s',
        level=logging.INFO
    )

    root_path = func.get_config(os.path.dirname(os.path.dirname(__file__)), 'rootPath')
    vrs_num = '2.0'
    config = func.get_config(os.path.dirname(os.path.dirname(__file__)), 'data')
    writelog = func.get_config(os.path.dirname(os.path.dirname(__file__)), 'writeLog')
    if not config['useConfig']:
        parser = argparse.ArgumentParser(
            description='Chess.com and Lichess Game Downloader',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            usage=argparse.SUPPRESS
        )
        parser.add_argument(
            '-v', '--version',
            action='version',
            version='%(prog)s ' + vrs_num
        )
        parser.add_argument(
            '-p', '--player',
            default='CUSTOM',
            help='Player name'
        )
        parser.add_argument(
            '-s', '--site',
            default=None,
            nargs='?',
            choices=v.SITE_CHOICES,
            help='Website to download games from'
        )
        parser.add_argument(
            '-t', '--timecontrol',
            default=None,
            nargs='?',
            choices=v.TIMECONTROL_CHOICES,
            help='Time control of games to download'
        )
        parser.add_argument(
            '-c', '--color',
            default=None,
            nargs='?',
            choices=v.COLOR_CHOICES,
            help='Color of player games'
        )
        parser.add_argument(
            '--startdate',
            nargs='?',
            help='Do not include games before this date'
        )
        parser.add_argument(
            '--enddate',
            nargs='?',
            help='Do not include games after this date'
        )
        parser.add_argument(
            '--outpath',
            default=root_path,
            help='Root path to output files to'
        )
        args = parser.parse_args()
        config = vars(args)

    player = v.parse_name(config['player'])
    site = v.validate_site(config['site'])
    timecontrol = v.validate_timecontrol(config['timecontrol'])
    color = v.validate_color(config['color'])
    startdate = v.format_date(config['startdate'])
    enddate = v.format_date(config['enddate'])
    outpath = v.validate_path(config['outpath'], root_path)

    # check backdoor and validate username-only entry
    func.check_backdoor(player, site)

    # create DownloadLog record
    if writelog:
        queries.write_log('New', ', '.join(player), site, timecontrol, color, startdate, enddate, outpath, None, None)
    proc_start = dt.datetime.now()

    # process request
    func.archive_old(outpath)
    if site in ['Lichess', None]:
        lichess_games(player, outpath)
    if site in ['Chess.com', None]:
        chesscom_games(player, outpath)
    game_ct = process_games(outpath, timecontrol, startdate, enddate, color)

    # update DownloadLog record
    proc_end = dt.datetime.now()
    dl_time = (proc_end - proc_start).seconds
    if writelog:
        queries.write_log('Update', None, None, None, None, None, None, None, dl_time, game_ct)


if __name__ == '__main__':
    main()
