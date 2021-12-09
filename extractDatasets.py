import os
import sys

import pandas as pd
from keybert import KeyBERT
from pandas import json_normalize
import json
import logging
import re


def get_emotelist(obj, save_path, filename):
    # iterate arrays
    if isinstance(obj, list):
        for item in obj:
            if json.dumps(item).startswith("{\"elements\": [{") and not \
                    json.dumps(item).startswith("{\"elements\": [{\"elements\": [{"):
                if 'emoji' in json.dumps(item):
                    emotedata = json_normalize(item['elements'])
                    # in case .json is needed
                    with open(save_path + filename + ".json", "a+") as out:
                        json.dump(emotedata.to_dict(orient='records'), out, indent=2)

            get_emotelist(item, save_path, filename)

    # iterate objects
    elif isinstance(obj, dict):
        for key in obj:
            get_emotelist(obj[key], save_path, filename)


def count_keys(selected_key, val, obj):
    count = 0

    # iterate arrays
    if isinstance(obj, list):
        for item in obj:
            count += count_keys(selected_key, val, item)
    # iterate objects
    elif isinstance(obj, dict):
        for key in obj:

            if key == selected_key:
                value = val

                if value in str(obj[key]):
                    count += 1

            count += count_keys(selected_key, val, obj[key])

    return count


def get_dataframe(path):
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
    messageframe['text'] = messageframe['text'].apply(lambda x: re.sub('<.*>', '', x))
    messageframe['text'] = messageframe['text'].apply(lambda x: re.sub('(?<=:)\\S*?(?=:)', '', x))
    messageframe.to_csv(save_path + filename + '.txt', sep='\t', index=False, header=False)


def get_emoji_txt(save_path, filename, obj):
    # iterate arrays
    if isinstance(obj, list):
        for item in obj:
            get_emoji_txt(save_path, filename, item)
    # iterate objects
    elif isinstance(obj, dict):
        for key in obj:

            if key == 'type':
                if 'emoji' in str(obj[key]):
                    with open(save_path + filename + ".txt", "a+") as out:
                        out.write(obj['name'] + '\n')

            get_emoji_txt(save_path, filename, obj[key])


def iterate_txt(inputpath,outputpath,case):
    logging.info("Iterate: " + inputpath + "case: " + case)
    if os.path.exists(outputpath):
        logging.info("Iterated data already exists => will be deleted and redone...")
        os.remove(outputpath)
    for filename in os.listdir(inputpath):
        if case == "project":
            if filename.endswith(".txt") and filename[0].isdigit():

                with open(inputpath + filename,
                          "r", encoding='utf-8') as read_file:
                                with open(outputpath, "a+", encoding='utf-8') as out:
                                    out.write(read_file.read() + '\n')

        if case == "general":
            if filename.endswith(".txt") and not filename[0].isdigit():
                with open(inputpath + filename,
                          "r", encoding='utf-8') as read_file:
                    with open(outputpath, "a+", encoding='utf-8') as out:
                        out.write(read_file.read() + '\n')

def iterate_projects():
    if not os.path.exists('datasets/project-data'):
        os.mkdir('datasets/project-data')

    if not os.path.exists('datasets/general-data'):
        os.mkdir('datasets/general-data')

    # get messagedatasets
    iterate_txt("datasets\\messagetext_dataset\\","datasets\\project-data\\project_messagetext.txt","project")
    iterate_txt("datasets\\messagetext_dataset\\","datasets\\general-data\\general_messagetext.txt","general")
    # get emojidatasets
    iterate_txt("datasets\\emoji_dataset\\","datasets\\project-data\\project_emoji.txt","project")
    iterate_txt("datasets\\emoji_dataset\\","datasets\\general-data\\general_emoji.txt","general")


def extract_information(dataframe, save_path, filename, kw_model):
    dictframe = dataframe.to_dict(orient='records')
    count_channeljoin = len(dataframe.loc[dataframe['subtype'] == 'channel_join'])
    count_channelpurpose = len(dataframe.loc[dataframe['subtype'] == 'channel_purpose'])
    count_messages = len(dataframe[pd.isna(dataframe['subtype'])])
    count_active_user = len(dataframe['user'].unique())
    # count_team = len(dataframe['team'].dropna().unique())
    most_active_user = dataframe['user'].value_counts().idxmax()
    # count_reactions = len(dataframe['reactions'].dropna())
    count_emoji = count_keys('type', 'emoji', dictframe)
    count_link = count_keys('type', 'link', dictframe)
    count_mentions = count_keys('type', 'user', dictframe)

    logging.info("Reading txt files for keywords")
    with open("datasets/messagetext_dataset/" + filename + ".txt",
              "r", encoding="utf8") as txt_file:
        messagetxt = txt_file.read()

    logging.info("Processing keywords of: " + filename)
    keywords = kw_model.extract_keywords(messagetxt, keyphrase_ngram_range=(1, 1), stop_words=None)
    logging.info("Processing keypairs of: " + filename)
    keypairs = kw_model.extract_keywords(messagetxt, keyphrase_ngram_range=(1, 2), stop_words=None)

    # dict with all needed information
    info = {
        "count_channeljoin": count_channeljoin,
        "count_channelpurpose": count_channelpurpose,
        "count_messages": count_messages,
        "count_active_user": count_active_user,
        # "count_team" : count_team,
        "most_active_user": most_active_user,
        # "count_reactions" : count_reactions,
        "count_emoji": count_emoji,
        "count_link": count_link,
        "count_mentions": count_mentions,
        "keywords": keywords,
        "keypairs": keypairs
    }

    with open(save_path + filename + ".json", "w") as out:
        json.dump(info, out, indent=2)


if __name__ == '__main__':

    dataset_path = sys.argv[1]
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Initializing Keybert-Model...")
    kw_model = KeyBERT()
    logging.info("Initializing Keybert-Model finished!")

    # create directory for txt files
    if not os.path.exists('datasets'):
        os.mkdir('datasets')

    if not os.path.exists('datasets/mainframe_dataset'):
        os.mkdir('datasets/mainframe_dataset')

    if not os.path.exists('datasets/messagetext_dataset'):
        os.mkdir('datasets/messagetext_dataset')

    if not os.path.exists('datasets/information_dataset'):
        os.mkdir('datasets/information_dataset')

    if not os.path.exists('datasets/emojitext_dataset'):
        os.mkdir('datasets/emojitext_dataset')

    if not os.path.exists('datasets/emoji_dataset'):
        os.mkdir('datasets/emoji_dataset')

    # iterate through all json files in dataset
    for filename in os.listdir(dataset_path):
        if filename.endswith(".json"):

            with open(dataset_path + filename,
                      "r") as read_file:
                jsondata = json.loads(read_file.read())

            dataframe = json_normalize(jsondata['messages'])

            if not os.path.exists('datasets/mainframe_dataset/' + filename.replace('.json', '.csv')):
                logging.info("Processing main dataframe of: " + filename)
                print_dataframe_csv(dataframe, "datasets/mainframe_dataset/", filename.replace('.json', ''))
            if not os.path.exists('datasets/messagetext_dataset/' + filename.replace('.json', '.txt')):
                logging.info("Processing text messages of: " + filename)
                messages_to_txt(dataframe, "datasets/messagetext_dataset/", filename.replace('.json', ''))
            if not os.path.exists('datasets/information_dataset/' + filename):
                logging.info("Processing meta information of: " + filename)
                extract_information(dataframe, "datasets/information_dataset/", filename.replace('.json', ''), kw_model)
            if not os.path.exists('datasets/emojitext_dataset/' + filename):
                logging.info("Processing emote-text occurences of: " + filename)
                get_emotelist(jsondata, "datasets/emojitext_dataset/", filename.replace('.json', ''))
            if not os.path.exists('datasets/emoji_dataset/' + filename.replace('.json', '.txt')):
                logging.info("Processing emote occurences of: " + filename)
                get_emoji_txt("datasets/emoji_dataset/", filename.replace('.json', ''), jsondata)

    logging.info("Processing project/general info:")
    iterate_projects()
