import csv

import pandas as pd


def check_date_format(value):
    if len(value.split("-")) == 3:
        return True
    else:
        return False


with open("data.csv", "r") as f:
    reader = csv.reader(f)
    for row in reader:
        is_correct = check_date_format(row[1])
        if not is_correct:
            print(row[1])

        print(row)

# df = pd.read_csv("data.csv", header=None)
# print(df.head())
# df.info()

