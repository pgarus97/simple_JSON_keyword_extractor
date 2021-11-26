import os
import sys

import pandas as pd
from pandas import json_normalize
import json

def getDataFrame(path):
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    with open(path,
              "r") as read_file:
        jsondata = json.loads(read_file.read())

        # gets datagram
        dataframe = json_normalize(jsondata['messages'])
        return dataframe

def print_dataframe_csv(dataframe, save_path, filename):
    dataframe = dataframe.replace(r'\n', ' ', regex=True)
    dataframe.to_csv(save_path + filename + '.csv')

def messages_to_txt(dataframe, save_path, filename):
    text_subtype = dataframe[["text", "subtype"]]
    messageframe = text_subtype[pd.isna(text_subtype['subtype'])]
    messageframe.to_csv(save_path + filename + '.txt', sep='\t', index=False, header=False)

if __name__ == '__main__':

    dataset_path = sys.argv[1]
    #create directory for txt files
    if not os.path.exists('datasets/mainframe_dataset'):
        os.mkdir('datasets/mainframe_dataset')

    if not os.path.exists('datasets/messagetext_dataset'):
        os.mkdir('datasets/messagetext_dataset')

    #iterate through all json files in dataset
    for filename in os.listdir(dataset_path):
        if filename.endswith(".json"):
            dataframe = getDataFrame(dataset_path+filename)
            print_dataframe_csv(dataframe, "datasets/mainframe_dataset/", filename.replace('.json', ''))
            messages_to_txt(dataframe, "datasets/messagetext_dataset/", filename.replace('.json', ''))



