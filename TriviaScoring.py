from os import path

import argparse
import gspread
import pandas as pd
import sys
from datetime import datetime
from fuzzywuzzy import fuzz
from gspread import WorksheetNotFound
from oauth2client.service_account import ServiceAccountCredentials
from typing import Union

CORE_COLUMNS = {'QuestionID', 'Date', 'Points', 'Round #', 'Question #', 'Variance Limit', 'Question', 'Answer'}


def get_client() -> object:
    """
    Gets the client for all the Google api communications
    Note :: assumes that the permissions json is in the same folder as this script

    :return: The client to use to interact with Google sheets/drive
    """
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(path.join(path.dirname(path.abspath(__file__)),
                                                                       'triviagradingproject-48344dd02957.json'), scope)
    return gspread.authorize(creds)


def read_spreadsheets(client: object, round: str, sheetname: str = "Trivia_Statistics",
                      date: Union[None, str] = None, wksheet: str = "Detail"):
    """
    read in the full sheet and filter for round

    :param client: the client for the google api
    :param round: the round number (as a string)
    :param sheetname: the name of the sheet with the information to be read
    :param date:  the date of the trivia night
    :return: a dataframe of the answers and inputs for the round
    """
    if date is None:
        date = datetime.today().strftime('%-m/%-d/%Y')

    sheet = client.open(sheetname)
    data = sheet.worksheet(wksheet).get_all_records()
    df = pd.DataFrame(data)
    filtered = df[(df["Round #"] == round) & (df["Date"] == date)]
    if filtered.empty:
        print("No trivia was done on {0}".format(date))
        exit()

    return sheet, filtered


def grade_entries(data: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a new dataframe of the grades

    :param data: The original dataframe with the answers and the teams' responses
    :return: The dataframe with the grades to be posted on the sheet
    """
    teams = [name for name in list(data.columns) if name not in CORE_COLUMNS]
    correct = pd.DataFrame()
    correct["QuestionID"] = data["QuestionID"]
    correct["Answer"] = data["Answer"]
    correct["Round #"] = data["Round #"]

    for team in teams:
        correct[team] = data.apply(lambda x: rate_match(x[team], x["Answer"]), axis=1)
    return correct


def rate_match(entry: str, correct: str) -> str:
    """
    Takes the team's response and compares it to the correct answer.

    :param entry: The team's answer
    :param correct: The correct answer
    :return: a string with the verdict (1 --yes, 0--no, CHECK--maybe) and either the score (if 1 or 0) or the team's answers, if potentially correct.
    """
    if entry is "":
        return f"0, empty"
    verdict = fuzz.token_set_ratio(entry, correct)
    if verdict >= 90:
        return f"1, {verdict}"
    elif verdict >= 50:
        return f"CHECK, {entry}"
    else:
        return f"0, {verdict}"


# TODO:: figure out how to send report for points
def report(data: pd.DataFrame, date: str, sheet: str) -> None:
    """
    Posts the grades on the spreadsheet

    :param data: The dataframe of the grades
    :param date: The date of the trivia night
    :param sheet: The name of the sheet with the trivia information
    :return: None
    """
    name = f"Results_{date}"
    try:
        wksheet = sheet.worksheet(name)
        wksheet.append_rows(data.values.tolist())

    except WorksheetNotFound:
        wksheet = sheet.add_worksheet(name, rows=100, cols=len(list(data.columns)))
        wksheet.update([data.columns.values.tolist()] + data.values.tolist())

    print("Upload Complete")


def main(argv):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-s, --sheet",
                        help="The name of the spreadsheet with all of the data, including the output sheets. Note :: Must be shared with trivanightadmin@triviagradingproject.iam.gserviceaccount.com",
                        action="store", nargs=1, default="Trivia Statistics", type=str, dest="sheetname")
    parser.add_argument("-w, --worksheet",
                        help="The name of the sheet inside the spreadsheet where the data is stored",
                        action="store", nargs=1, type=str, default="Detail", dest="wksheet")
    parser.add_argument("-r, --round", help="The round of trivia to be graded", action="store", nargs=1,
                        type=int, required=True, dest="round")
    parser.add_argument("-d, --date", help="the date of the trivia round to be graded", action="store", nargs=1,
                        type=str, required=False, dest="date")
    args = vars(parser.parse_args(argv))
    print("args: ", args)

    date = args["date"]
    if date is not None:
        date = date[0]

    client = get_client()
    sheet, data = read_spreadsheets(client, round=args["round"][0], sheetname=args["sheetname"][0],
                                    wksheet=args["wksheet"], date=date)
    results = grade_entries(data)
    report(results, args["date"][0], sheet)


if __name__ == "__main__":
    main(sys.argv[1:])
