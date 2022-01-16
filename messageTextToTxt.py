import os
import pandas as pd
from keybert import KeyBERT
from pandas import json_normalize
import json
import re



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
        #glomdata = glom(jsondata, ('messages', [('blocks',[('elements', [('elements', ['type'])])])]))
        return dataframe

def print_dataframe_csv(dataframe, save_path, filename):
    dataframe = dataframe.replace(r'\n', ' ', regex=True)
    dataframe.to_csv(save_path + filename + '.csv')



def extract_information(dataframe, save_path, filename):
    dictframe = dataframe.to_dict(orient='records')

    count_channeljoin = len(dataframe.loc[dataframe['subtype'] == 'channel_join'])
    count_channelpurpose = len(dataframe.loc[dataframe['subtype'] == 'channel_purpose'])
    count_messages = len(dataframe[pd.isna(dataframe['subtype'])])
    count_active_user = len(dataframe['user'].unique())
    count_team = len(dataframe['team'].dropna().unique())
    most_active_user = dataframe['user'].value_counts().idxmax()
    #count_reactions = len(dataframe['reactions'].dropna())
    count_emoji = count_keys('type','emoji',dictframe)
    count_link = count_keys('type','link',dictframe)
    count_mentions = count_keys('type','user', dictframe)

    with open("datasets/messagetext_dataset/"+filename+".txt",
              "r") as txt_file:
        messagetxt = txt_file.read()

    kw_model = KeyBERT()
    print("Processing keywords of: " + filename)
    keywords = kw_model.extract_keywords(messagetxt, keyphrase_ngram_range=(1, 1), stop_words=None)
    print("Processing keypairs of: " + filename)
    keypairs = kw_model.extract_keywords(messagetxt, keyphrase_ngram_range=(1, 2), stop_words=None)


    #dict with all needed information
    info = {
        "count_channeljoin" : count_channeljoin,
        "count_channelpurpose" : count_channelpurpose,
        "count_messages" : count_messages,
        "count_active_user" : count_active_user,
        "count_team" : count_team,
        "most_active_user" : most_active_user,
        #"count_reactions" : count_reactions,
        "count_emoji" : count_emoji,
        "count_link" : count_link,
        "count_mentions" :count_mentions,
        "keywords" : keywords,
        "keypairs" : keypairs
    }

    with open(save_path+filename+".json", "w") as out:
        json.dump(info, out, indent=2)




def get_emotelist(obj):

    # iterate arrays
    if isinstance(obj, list):
        for item in obj:
            if json.dumps(item).startswith("{\"elements\": [{") and not \
                    json.dumps(item).startswith("{\"elements\": [{\"elements\": [{"):
                if 'emoji' in json.dumps(item):
                    print(item['elements'])
                    emotedata = json_normalize(item['elements'])
                    testdata = emotedata.loc[emotedata['type'] == 'emoji']
                    print(testdata['name'])
                    # in case .json is needed
                    #with open("test.json", "a") as out:
                        #json.dump(emotedata.to_dict(orient='records'), out, indent=2)
                    #print(emotedata)

            get_emotelist(item)

    # iterate objects
    elif isinstance(obj, dict):
        for key in obj:
             get_emotelist(obj[key])


def get_emoji_txt(obj):

    # iterate arrays
    if isinstance(obj, list):
        for item in obj:
            get_emoji_txt(item)
    # iterate objects
    elif isinstance(obj, dict):
        for key in obj:

            if key == 'type':
                if 'emoji' in str(obj[key]):
                    with open("resulttest.txt", "a+") as out:
                        print("write")
                        out.write(obj['name'] + '\n')

            get_emoji_txt(obj[key])

def iterate_emojidataset():
    for filename in os.listdir("C:\\Users\\phili\\Desktop\\Praxisproject\\pp21-hack-the-crisis\\DataExtraction"
                               "\\datasets\\emoji_dataset\\"):
        if filename.endswith(".txt") and not filename[0].isdigit():

            with open("C:\\Users\\phili\\Desktop\\Praxisproject\\pp21-hack-the-crisis\\DataExtraction\\datasets"
                      "\\emoji_dataset\\" + filename,
                      "r", encoding='utf-8') as read_file:
                            with open("C:\\Users\\phili\\Desktop\\Praxisproject\\pp21-hack-the-crisis\\DataExtraction"
                                      "\\datasets\\project-data\\project_emoji.txt", "a+", encoding='utf-8') as out:
                                print("write")
                                out.write(read_file.read() + '\n')

        if filename.endswith(".txt") and filename[0].isdigit():
            with open("C:\\Users\\phili\\Desktop\\Praxisproject\\pp21-hack-the-crisis\\DataExtraction\\datasets"
                      "\\emoji_dataset\\" + filename,
                      "r", encoding='utf-8') as read_file:
                with open("C:\\Users\\phili\\Desktop\\Praxisproject\\pp21-hack-the-crisis\\DataExtraction"
                          "\\datasets\\general-data\\general_emoji.txt", "a+", encoding='utf-8') as out:
                    print("write")
                    out.write(read_file.read() + '\n')

def iterate_txt(inputpath,outputpath,case):
    if os.path.exists(outputpath):
        os.remove(outputpath)
    for filename in os.listdir(inputpath):
        if case == "project":
            if filename.endswith(".txt") and filename[0].isdigit():

                with open(inputpath + filename,
                          "r", encoding='utf-8') as read_file:
                                with open(outputpath, "a+", encoding='utf-8') as out:
                                    print("write")
                                    out.write(read_file.read() + '\n')

        if case == "general":
            if filename.endswith(".txt") and not filename[0].isdigit():
                with open(inputpath + filename,
                          "r", encoding='utf-8') as read_file:
                    with open(outputpath, "a+", encoding='utf-8') as out:
                        print("write")
                        out.write(read_file.read() + '\n')


def iterate_info(inputpath,outputpath,case):
    if os.path.exists(outputpath):
        os.remove(outputpath)
    iteratedict = {
        "count_channeljoin": 0,
        "count_channelpurpose": 0,
        "count_messages": 0,
        "count_active_user": 0,
        "count_emoji": 0,
        "count_link": 0,
        "count_mentions": 0,
    }
    for filename in os.listdir(inputpath):
        if case == "project":
            if filename.endswith(".json") and filename[0].isdigit():
                with open(inputpath + filename,
                          "r", encoding='utf-8') as read_file:
                    tempDict = json.load(read_file)
                    iteratedict['count_channeljoin'] += tempDict['count_channeljoin']
                    iteratedict['count_channelpurpose'] += tempDict['count_channelpurpose']
                    iteratedict['count_messages'] += tempDict['count_messages']
                    iteratedict['count_active_user'] += tempDict['count_active_user']
                    iteratedict['count_emoji'] += tempDict['count_emoji']
                    iteratedict['count_link'] += tempDict['count_link']
                    iteratedict['count_mentions'] += tempDict['count_mentions']

        if case == "general":
            if filename.endswith(".json") and not filename[0].isdigit():
                with open(inputpath + filename,
                          "r", encoding='utf-8') as read_file:
                    tempDict = json.load(read_file)
                    iteratedict['count_channeljoin'] += tempDict['count_channeljoin']
                    iteratedict['count_channelpurpose'] += tempDict['count_channelpurpose']
                    iteratedict['count_messages'] += tempDict['count_messages']
                    iteratedict['count_active_user'] += tempDict['count_active_user']
                    iteratedict['count_emoji'] += tempDict['count_emoji']
                    iteratedict['count_link'] += tempDict['count_link']
                    iteratedict['count_mentions'] += tempDict['count_mentions']

    with open(outputpath, "w", encoding='utf-8') as out:
        json.dump(iteratedict, out, indent=2)

def iterate_projects():
    if not os.path.exists('datasets/project-data'):
        os.mkdir('datasets/project-data')

    if not os.path.exists('datasets/general-data'):
        os.mkdir('datasets/general-data')

    #get emoji data
    #get messagedataset
    iterate_txt("datasets\\messagetext_dataset\\","datasets\\project-data\\project_messagetext.txt","project")
    iterate_txt("datasets\\messagetext_dataset\\","datasets\\general-data\\general_messagetext.txt","general")
    iterate_txt("datasets\\emoji_dataset\\","datasets\\project-data\\project_emoji.txt","project")
    iterate_txt("datasets\\emoji_dataset\\","datasets\\general-data\\general_emoji.txt","general")
    iterate_info("datasets\\information_dataset\\","datasets\\general-data\\general_information.txt","general")
    iterate_info("datasets\\information_dataset\\","datasets\\project-data\\project_information.txt","project")








def join_dataframes():

    frames = [pd.read_csv("C:\\Users\\phili\\Desktop\\Praxisproject\\pp21-hack-the-crisis\\DataExtraction\\datasets\\mainframe_dataset\\"+f, index_col=0) for f in os.listdir("C:\\Users\\phili\\Desktop\\Praxisproject\\pp21-hack-the-crisis\\DataExtraction\\datasets\\mainframe_dataset\\")]
    result = pd.concat(frames)
    print(result)
    result.to_json('test.json', orient='split')


    #for filename in os.listdir("C:\\Users\\phili\\Desktop\\Praxisproject\\pp21-hack-the-crisis\\DataExtraction\\datasets\\mainframe_dataset\\"):
       # if filename.endswith(".csv") and filename.startswith("1"):

            #with open("C:\\Users\\phili\\Desktop\\Praxisproject\\pp21-hack-the-crisis\\DataExtraction\\datasets\\mainframe_dataset\\" + filename,
               #       "r") as read_file:
               # jsondata = json.loads(read_file.read())

           # dataframe = json_normalize(jsondata['messages'])

def messages_to_txt(dataframe, save_path, filename):
    print("inmethod")
    text_subtype = dataframe[["text", "subtype"]]
    messageframe = text_subtype[pd.isna(text_subtype['subtype'])]
    #messageframe['text'] = messageframe['text'].apply(lambda x: x.strip().capitalize())
    #messageframe['text'] = messageframe['text'].str.replace('as','beef')
    #messageframe['text'] = re.sub('<.*>', '', messageframe['text'].str)
    messageframe['text'] = messageframe['text'].apply(lambda x: re.sub('<.*>', '', x))
    messageframe['text'] = messageframe['text'].apply(lambda x: re.sub( ' :.*?:', '', x))



    #<@U011B1DRCBG>
    print(messageframe)
    messageframe.to_csv(save_path + filename + '.txt', sep='\t', index=False, header=False)

def test():
    with open("C:\\Users\\phili\\Desktop\\Praxisproject\\pp21-hack-the-crisis\\dataset\\5_391_centralize_tracking_inspection.json",
              "r") as read_file:
        jsondata = json.loads(read_file.read())

    dataframe = json_normalize(jsondata['messages'])

    get_emoticon_txt("C:\\Users\\phili\\Desktop\\Praxisproject\\pp21-hack-the-crisis\\DataExtraction\\","test", dataframe)


def get_emoticon_txt(save_path, filename, dataframe):
    text_subtype = dataframe[["text", "subtype"]]
    messageframe = text_subtype[pd.isna(text_subtype['subtype'])]

    with open("convertEmoticons.txt", 'r') as file:
        for line in file:
            emoticon = line.split()
            print(emoticon)
            emoticon_count = messageframe['text'].str.count(emoticon[0]).sum()
            print(emoticon_count)
            if(emoticon_count.item() > 0):
                with open(save_path + filename + ".txt", "a+") as out:
                    out.write(emoticon[0].replace("\\","") + ':' + str(emoticon_count.item()) + "(" + emoticon[1] +")"+'\n')

if __name__ == '__main__':
    kw_model = KeyBERT()

    with open("datasets/project-data/project_messagetext.txt",
              "r", encoding="utf8") as txt_file:
        messagetxt = txt_file.read()

    keywords = kw_model.extract_keywords(messagetxt, keyphrase_ngram_range=(1, 1), stop_words=None)
    keypairs = kw_model.extract_keywords(messagetxt, keyphrase_ngram_range=(1, 2), stop_words=None)

    info = {
        "keywords": keywords,
        "keypairs": keypairs
    }
    with open("datasets/project-data/project_keywords.txt", "w") as out:
        json.dump(info, out, indent=2)

    with open("datasets/general-data/general_messagetext.txt",
              "r", encoding="utf8") as txt_file:
        messagetxt2 = txt_file.read()

    keywords = kw_model.extract_keywords(messagetxt2, keyphrase_ngram_range=(1, 1), stop_words=None)
    keypairs = kw_model.extract_keywords(messagetxt2, keyphrase_ngram_range=(1, 2), stop_words=None)

    info2 = {
        "keywords": keywords,
        "keypairs": keypairs
    }
    with open("datasets/general-data/general_keywords.txt", "w") as out:
        json.dump(info2, out, indent=2)
