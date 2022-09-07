from __future__ import absolute_import
import argparse
import sys
from datetime import datetime
from os import path

import gspread
import pandas as pd
from fuzzywuzzy import fuzz
from gspread import WorksheetNotFound
from oauth2client.service_account import ServiceAccountCredentials

CORE_COLUMNS = set([u'QuestionID', u'Date', u'Points', u'Round #', u'Question #', u'Variance Limit', u'Question', u'Answer'])


def get_client():
    u"""
    Gets the client for all of the google api communications
    Note :: assumes that the permissions json is in the same folder as this script

    :return: The client to use to interact with google sheets/drive
    """
    scope = [u'https://spreadsheets.google.com/feeds', u'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(path.join(path.dirname(path.abspath(__file__)),
                                                                       u'triviagradingproject-48344dd02957.json'), scope)
    return gspread.authorize(creds)


def read_spreadsheets(client, round, sheetname=u"Trivia_Statistics", date=None,
                      wksheet=u"Detail"):
    u"""
    read in the full sheet and filter for round

    :param client: the client for the google api
    :param round: the round number
    :param sheetname: the name of the sheet with the information to be read
    :param date:  the date of the trivia night
    :return: a dataframe of the answers and inputs for the round
    """
    if date is None:
        date = datetime.today().strftime(u'%-m/%-d/%Y')

    sheet = client.open(sheetname)
    data = sheet.worksheet(wksheet).get_all_records()
    df = pd.DataFrame(data)
    filtered = df[(df[u"Round #"] == round) & (df[u"Date"] == date)]
    if filtered.empty:
        print u"No trivia was done on {0}".format(date)
        exit()

    return sheet, filtered


def grade_entries(data):
    u"""
    Creates a new dataframe of the grades

    :param data: The original dataframe with the answers and the teams' responses
    :return: The dataframe with the grades to be posted on the sheet
    """
    teams = [name for name in list(data.columns) if name not in CORE_COLUMNS]
    correct = pd.DataFrame()
    correct[u"QuestionID"] = data[u"QuestionID"]
    correct[u"Answer"] = data[u"Answer"]
    correct[u"Round #"] = data[u"Round #"]

    for team in teams:
        correct[team] = data.apply(lambda x: rate_match(x[team], x[u"Answer"]), axis = 1)
    return correct


def rate_match(entry, correct):
    u"""
    Takes the team's response and compares it to the correct answer.

    :param entry: The team's answer
    :param correct: The correct answer
    :return: a string with the verdict (1 --yes, 0--no, CHECK--maybe) and either the score (if 1 or 0) or the team's answers, if maybe.
    """
    if entry is u"":
        return f"0, empty"
    verdict = fuzz.token_set_ratio(entry, correct)
    if verdict >= 90:
        return u"1, {0}".format(verdict)
    elif verdict >= 50:
        return u"CHECK, {0}".format(entry)
    else:
        return u"0, {0}".format(verdict)


# TODO:: figure out how to send report for points
def report(data, date, sheet):
    u"""
    Posts the grades on the spreadsheet

    :param data: The dataframe of the grades
    :param date: The date of the trivia night
    :param sheet: The name of the sheet with the trivia information
    :return: None
    """
    name = u"Results_{0}".format(date)
    try:
        wksheet = sheet.worksheet(name)
        wksheet.append_rows(data.values.tolist())

    except WorksheetNotFound:
        wksheet = sheet.add_worksheet(name, rows = 100, cols = len(list(data.columns)))
        wksheet.update([data.columns.values.tolist()] + data.values.tolist())

    print u"Upload Complete"


def main(argv):
    parser = argparse.ArgumentParser(formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(u"-s, --sheet",
                        help = u"The name of the spreadsheet with all of the data, including the output sheets. Note :: Must be shared with trivanightadmin@triviagradingproject.iam.gserviceaccount.com",
                        action = u"store", nargs = 1, default = u"Trivia Statistics", type = unicode, dest = u"sheetname")
    parser.add_argument(u"-w, --worksheet",
                        help = u"The name of the sheet inside the spreadsheet where the data is stored",
                        action = u"store", nargs = 1, type = unicode, default = u"Detail", dest = u"wksheet")
    parser.add_argument(u"-r, --round", help = u"The round of trivia to be graded", action = u"store", nargs = 1,
                        type = int, required = True, dest = u"round")
    parser.add_argument(u"-d, --date", help = u"the date of the trivia round to be graded", action = u"store", nargs = 1,
                        type = unicode, required = False, dest = u"date")
    args = vars(parser.parse_args(argv))
    print u"args: ", args

    date = args[u"date"]
    if date is not None:
        date = date[0]

    client = get_client()
    sheet, data = read_spreadsheets(client, round = args[u"round"][0], sheetname = args[u"sheetname"][0],
                                    wksheet = args[u"wksheet"], date = date)
    results = grade_entries(data)
    report(results, args[u"date"][0], sheet)


if __name__ == u"__main__":
    main(sys.argv[1:])
