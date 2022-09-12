import datetime as dt
import logging
import os
import re

import dateutil.parser as dtp

from func import yn_prompt


SITE_CHOICES = ['Chess.com', 'Lichess']
TIMECONTROL_CHOICES = ['Bullet', 'Blitz', 'Rapid', 'Classical', 'Correspondence']
COLOR_CHOICES = ['White', 'Black', 'Combined']


def format_date(date_string):
    # format dates in the PGN standard yyyy.mm.dd format
    if date_string != '':
        try:
            dte = dt.datetime.strftime(dtp.parse(date_string), '%Y.%m.%d') if date_string is not None else None
        except dtp.ParserError:
            dte = None
            logging.warning(f'Unable to parse {date_string} as date, ignoring parameter')
    else:
        dte = None
    return dte


def parse_name(name):
    # return array ['Last', 'First']; otherwise ['name']
    if not name or name.strip() == '':
        logging.critical('Blank name!')
        raise SystemExit
    parsed_name = []
    if ',' in name:
        name = re.sub(r'\,\,+', ',', name)  # remove double commas
        parsed_name = [x.strip() for x in name.split(',')]
        return parsed_name
    elif ' ' in name:
        name = re.sub(r'\ \ +', ' ', name)  # remove double spaces
        parsed_name = [x.strip() for x in name.split(' ')]
        parsed_name.reverse()
        return parsed_name
    else:  # no comma, no space, must be a username
        parsed_name.append(name)
        return parsed_name


def validate_color(color):
    color = color.lower().capitalize()
    if color not in COLOR_CHOICES:
        if color != '':
            logging.warning(f'Invalid color provided, ignoring|{color}')
        color = None
    return color


def validate_site(site):
    site = site.lower().capitalize()
    if site not in SITE_CHOICES:
        if site != '':
            logging.warning(f'Invalid site provided, ignoring|{site}')
        site = None
    return site


def validate_timecontrol(timecontrol):
    timecontrol = timecontrol.lower().capitalize()
    if timecontrol not in TIMECONTROL_CHOICES:
        if timecontrol != '':
            logging.warning(f'Invalid timecontrol provided, ignoring|{timecontrol}')
        timecontrol = None
    return timecontrol


def validate_path(path, root_path):
    # verifiy path exists for game output
    ret = path if path != '' else root_path
    if not os.path.isdir(ret):
        yn = yn_prompt(f'Do you want to create the new path {path} ? Y or N ===> ')
        if yn == 'Y':
            os.mkdir(path)
        else:
            logging.info(f'User chose not to create new path, using default path {root_path}')
            ret = root_path
    return ret
