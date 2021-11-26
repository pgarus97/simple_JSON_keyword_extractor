import os
import sys

import pandas as pd
from pandas import json_normalize
import json
from glom import glom
from flatten_json import flatten

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

def extract_information(dataframe, save_path, filename):

    count_channeljoin = len(dataframe.loc[dataframe['subtype'] == 'channel_join'])
    count_channelpurpose = len(dataframe.loc[dataframe['subtype'] == 'channel_purpose'])
    count_messages = len(dataframe[pd.isna(dataframe['subtype'])])
    count_active_user = len(dataframe['user'].unique())
    count_team = len(dataframe['team'].dropna().unique())
    most_active_user = dataframe['user'].value_counts().idxmax()
    count_reactions = len(dataframe['reactions'].dropna())


    # members = pd.json_normalize(data=json_data['results'][0]['members']
    glomdata = glom(dataframe, (['blocks']))
    #blocks = json_normalize(dataframe['blocks'])
    #elements = json_normalize(blocks['elements'])

    print(glomdata)



    #elements1 = json_normalize(blocks,  meta=['properties'], record_path=['elements'], errors='ignore')
    #print(blocks)
    #print(elements)



    #count_emoji_messages = dataframe.loc[dataframe['subtype'] == 'channel_purpose']


    #print(count_active_user)
    #count_messages2 = len(dataframe.loc[(dataframe['subtype'] != 'channel_join') & (dataframe['subtype'] != 'channel_purpose')])

    #blocks, elements, elements , type='emoji'

    #dict with all needed information
    info = {

    }

    #with open(filename+".json", "w") as out:
        #json.dump(info, out)

if __name__ == '__main__':

    dataset_path = sys.argv[1]
    #create directory for txt files
    if not os.path.exists('datasets/mainframe_dataset'):
        os.mkdir('datasets/mainframe_dataset')

    if not os.path.exists('datasets/messagetext_dataset'):
        os.mkdir('datasets/messagetext_dataset')

    if not os.path.exists('datasets/information_dataset'):
        os.mkdir('datasets/information_dataset')

    dataframe = getDataFrame(dataset_path+"mentors_healthcare.json")
    extract_information(dataframe, "datasets/information_dataset/", "mentors_healthcare")

    #iterate through all json files in dataset
    #for filename in os.listdir(dataset_path):
        #if filename.endswith(".json"):
            #dataframe = getDataFrame(dataset_path+filename)
            #print_dataframe_csv(dataframe, "datasets/mainframe_dataset/", filename.replace('.json', ''))
            #messages_to_txt(dataframe, "datasets/messagetext_dataset/", filename.replace('.json', ''))
            #extract_information(dataframe, "datasets/information_dataset/", filename.replace('.json', ''))


