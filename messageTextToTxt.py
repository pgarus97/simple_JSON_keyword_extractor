import os
import sys

import pandas as pd
from pandas import json_normalize
import json


def messages_to_txt_pd(path):
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', -1)

    with open(path,
              "r") as read_file:
        jsondata = json.loads(read_file.read())

        # prepare filename
        filefullname = path.split('\\')
        filename = filefullname[-1]
        filename = filename.replace('.json', '')

        # gets datagram
        pandanorm = json_normalize(jsondata['messages'])

        # get only specific rows
        text_subtype = pandanorm[["text", "subtype"]]
        messageframe = text_subtype[pd.isna(text_subtype['subtype'])]

        # save to file
        messageframe.to_csv('messagetext_dataset/' + filename + '.txt', sep='\t', index=False, header=False)


if __name__ == '__main__':

    dataset_path = sys.argv[1]
    # create directory for txt files
    if not os.path.exists('messagetext_dataset'):
        os.mkdir('messagetext_dataset')

    # iterate through all json files in dataset
    for filename in os.listdir(dataset_path):
        if filename.endswith(".json"):
            messages_to_txt_pd(dataset_path+filename)
