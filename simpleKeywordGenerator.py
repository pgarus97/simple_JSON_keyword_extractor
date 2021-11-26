import sys

from keybert import KeyBERT
import pandas as pd
from pandas import json_normalize
from glom import glom
import json


def count_keys(selected_key, obj):
    count = 0

    # iterate arrays
    if isinstance(obj, list):
        for item in obj:
            count += count_keys(selected_key, item)
    # iterate objects
    elif isinstance(obj, dict):
        for key in obj:

            if key == selected_key:
                #channeljoinmsg = "> has joined the channel"
                #emote = "emoji"

              # if channeljoinmsg not in str(obj[key]):
                #if emote in str(obj[key]):
                    count += 1

            count += count_keys(selected_key, obj[key])

    return count


def get_all_attributes(selected_key, obj):
    all_messages = ''

    if isinstance(obj, list):
        for item in obj:
            all_messages += get_all_attributes(selected_key, item)

    elif isinstance(obj, dict):
        for key in obj:

            if key == selected_key:
                channeljoinmsg = "> has joined the channel"

                if channeljoinmsg not in str(obj[key]):
                    all_messages += ' ' + obj[key]

            all_messages += get_all_attributes(selected_key, obj[key])

    return all_messages

def pandatest(count,attribute, path):
    #sets pandas output
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', -1)

    with open(path,
              "r") as read_file:
        jsondata= json.loads(read_file.read())

        #gets datagram
        pandanorm = json_normalize(jsondata['messages'])

        #get only specific rows
        text_subtype = pandanorm[["text", "subtype"]]
        print(text_subtype)

        #gets specific value as condition
        #print(pandanorm.loc[pandanorm['subtype'] == 'channel_join'])
        #print(pandanorm)

        #gets all messages via glom
        glomdata = glom(jsondata, ('messages',['text']))

        #goes over datalist and removes elements with given string via glom
        glomdata[:] = [x for x in glomdata if "> has joined the channel" not in x]

        #count messages
        print(len(glomdata))

        #clean data from uninteded characters etc.
        for i,x in enumerate(glomdata):
            glomdata[i] = x.replace("\n"," ").replace("\\xa0", " ")
        print(glomdata)

# main function
def main(count, attribute, path):
    with open(path,
              "r") as read_file:
        jsondata = json.loads(read_file.read())
        print(count_keys(count, jsondata))
        # TODO need to filter out joined channel messages

        data = get_all_attributes(attribute, jsondata)

        kw_model = KeyBERT()
        # change parameters here to change keyword to key-sentence and stopwords
        keywords = kw_model.extract_keywords(data, keyphrase_ngram_range=(1, 2), stop_words=None)
        print(keywords)


if __name__ == '__main__':
    pandatest(sys.argv[1], sys.argv[2], sys.argv[3])

