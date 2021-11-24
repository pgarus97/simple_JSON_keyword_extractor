import sys

from keybert import KeyBERT
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
                channeljoinmsg = "> has joined the channel"

               if channeljoinmsg not in str(obj[key]):
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
    main(sys.argv[1], sys.argv[2], sys.argv[3])
