#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import json
import sys
import re
import os
import multiprocessing

import nltk.tokenize.casual as NLTK
from nltk.corpus import stopwords
import string

PUNCTUATION = list(string.punctuation)
STOP = stopwords.words('english') + ['rt', 'via']

##########################################
# These are the core regular expressions.
# Copied from nltk.tokenize.casual, but separate to different types.
##########################################

EMOTICONS = NLTK.EMOTICONS
EMOJIS = r"""[\U00010000-\U0010ffff]"""

URLS = NLTK.URLS

Phone_numbers = r"""
    (?:
      (?:            # (international)
        \+?[01]
        [\-\s.]*
      )?
      (?:            # (area code)
        [\(]?
        \d{3}
        [\-\s.\)]*
      )?
      \d{3}          # exchange
      [\-\s.]*
      \d{4}          # base
    )"""

Username = r"""
(?:@[\w_]+)"""

Hashtags = r"""
(?:\#+[\w_]+[\w\'_\-]*[\w_]+)"""

Email = r"""
[\w.+-]+@[\w-]+\.(?:[\w-]\.?)+[\w-]"""

# The components of the tokenizer:
REGEXPS = NLTK.REGEXPS
# Modified from nltk regexps. Separated different types.
NormWords = (
    # HTML tags:
    r"""<[^>\s]+>"""
    ,
    # ASCII Arrows
    r"""[\-]+>|<[\-]+"""
    ,
    # Remaining word types:
    r"""
    (?:[^\W\d_](?:[^\W\d_]|['\-_])+[^\W\d_]) # Words with apostrophes or dashes.
    |
    (?:[+\-]?\d+[,/.:-]\d+[+\-]?)  # Numbers, including fractions, decimals.
    |
    (?:[\w_]+)                     # Words without apostrophes or dashes.
    |
    (?:\.(?:\s*\.){1,})            # Ellipsis dots.
    |
    (?:\S)                         # Everything else that isn't whitespace.
    """
    )

######################################################################
# This is the core tokenizing regex:

WORD_RE = re.compile(r"""(%s)""" % "|".join(REGEXPS), re.VERBOSE | re.I
                     | re.UNICODE)

# WORD_RE performs poorly on these patterns:
HANG_RE = re.compile(r"""([^a-zA-Z0-9])\1{3,}""")

# The emoticon string gets its own regex so that we can preserve case for
# them as needed:
EMOTICON_RE = re.compile(EMOTICONS, re.VERBOSE | re.I | re.UNICODE)

# These regex are added
EMOJI_RE = re.compile(EMOJIS, re.UNICODE)
URLS_RE = re.compile(URLS, re.VERBOSE | re.I | re.UNICODE)
PHONUM_RE = re.compile(Phone_numbers, re.VERBOSE | re.I | re.UNICODE)
USERNAME_RE = re.compile(Username, re.VERBOSE | re.I | re.UNICODE)
HASHTAG_RE = re.compile(Hashtags, re.VERBOSE | re.I | re.UNICODE)
EMAIL_RE = re.compile(Email, re.VERBOSE | re.I | re.UNICODE)
NORMAL_RE = re.compile(r"""(%s)""" % "|".join(NormWords), re.VERBOSE | re.I | re.UNICODE)

class MyTweetTokenizer:
    r"""
    Modified from nltk TweetTokenizer
    If type argument is set to be True,
    Return a tuple for each token, with the token and its type.
    Otherwise,
    Return the original nltk TweetTokenizer results.
    Type codes:
    N: normal words
    E: emoticons or emojis
    U: urls or emails
    PN: phone number
    USR: user names
    H: hashtags
    S: stopwords
    PUNC: punctuations
    """

    def __init__(self, type_include=True):
        self.type_include = type_include

    def clean(self, text):
        if not self.type_include:
            tknzr = NLTK.TweetTokenizer()
            return tknzr.tokenize(text)

       # Fix HTML character entities:
        text = NLTK._replace_html_entities(text)
        # Shorten problematic sequences of characters
        safe_text = NLTK.HANG_RE.sub(r'\1\1\1', text)
        # Tokenize:
        words = WORD_RE.findall(safe_text)

        clean_text = text
       # # Possibly alter the case, but avoid changing emoticons like :D into :d:
        for i, x in enumerate(words[:]):

            # if EMOTICON_RE.match(x) or EMOJI_RE.match(x):
            # text.decode('utf8')
            
            
            if URLS_RE.match(x) or EMAIL_RE.match(x):
                # print "url"
                clean_text = clean_text.replace(x, '')
            elif USERNAME_RE.match(x):
                # print "Username"
                clean_text = clean_text.replace(x, '')
            elif HASHTAG_RE.match(x):
                # print "tag"
                clean_text = clean_text.replace(x, '')
            elif PHONUM_RE.match(x):
                # print "phone"
                clean_text = clean_text.replace(x, '')
            # elif x.lower() in STOP:
            #   print "stop"
            #     clean_text = clean_text.replace(x, '')
            # elif EMOJI_RE.match(x):
            #       clean_text = clean_text.replace(x, '')

                
            else:
                continue
        return clean_text



emoji_pattern = re.compile(
    u"(\ud83d[\ude00-\ude4f])|"  # emoticons
    u"(\ud83d[\u0000-\uddff])|"  # symbols & pictographs (2 of 2)
    u"(\ud83d[\ude80-\udeff])|"  # transport & map symbols
    u"(\uD83E[\uDD00-\uDDFF])|"
    u"(\ud83c[\udf00-\uffff])|"  # symbols & pictographs (1 of 2)
    u"(\ud83c[\udde0-\uddff])|"  # flags (iOS)
    u"([\u2934\u2935]\uFE0F?)|"
    u"([\u3030\u303D]\uFE0F?)|"
    u"([\u3297\u3299]\uFE0F?)|"
    u"([\u203C\u2049]\uFE0F?)|"
    u"([\u00A9\u00AE]\uFE0F?)|"
    u"([\u2122\u2139]\uFE0F?)|"
    u"(\uD83C\uDC04\uFE0F?)|"
    u"(\uD83C\uDCCF\uFE0F?)|"
    u"([\u0023\u002A\u0030-\u0039]\uFE0F?\u20E3)|"
    u"(\u24C2\uFE0F?|[\u2B05-\u2B07\u2B1B\u2B1C\u2B50\u2B55]\uFE0F?)|"
    u"([\u2600-\u26FF]\uFE0F?)|"
    u"([\u2700-\u27BF]\uFE0F?)"
    "+", flags=re.UNICODE) 

# input_file: str
# output_file: str
def read_tweets(input_file):
    happy_emoji = [' :‑) ', ' :) ', ' :))', ' :)))', ' :-] ', ' :]', ' :-3', ':->', ':>', ' 8-)', ':-}', ':}', ' :o)', ' :c)', ' :^)', ' =]', ' =)', 
                    ' :‑D', ' :D', ' 8‑D', ' 8D ', ' x‑D', ' xD ', ' X‑D ', ' XD ', ' =D ', ' =3', ':-))', ':-)))', ':\'‑)', ':\')', 
                    ' :-*', ':*', ' :×', ' ;‑)', ' ;)', ' *-)', ' *)', ';‑]', ' ;]', ' ;^)', ' :‑,', ' ;D', ' :‑P', ' :P', ' X‑P', ' x‑p',
                    ' :‑p', ' :p ', ' :b', ' d:', ' =p', '^_^', '^^', '^ ^', '(^_^)/', '(^O^)／', '(^o^)／', '(^^)/', '>^_^<', '^/^', '(*^_^*）',
                    '(^.^)', '(^·^)', '(^.^)', '(^_^)', '(^^)', '^_^', '(*^.^*)', '(#^.^#)', '(*^0^*)', '＼(^o^)／', '!(^^)!', '(＾ｖ＾)', 
                    '(＾ｕ＾)', '( ^)o(^ )', '(^O^)', '(^o^)', ')^o^(', 'ヽ(´▽`)/', '≧∇≦', '^ω^', '＾▽＾', 
                    '\xF0\x9F\x98\x81', '\xF0\x9F\x98\x82', '\xF0\x9F\x98\x83', '\xF0\x9F\x98\x84', '\xF0\x9F\x98\x86', '\xF0\x9F\x98\x89', 
                    '\xF0\x9F\x98\x8A', '\xF0\x9F\x98\x8B', '\xF0\x9F\x98\x8C', '\xF0\x9F\x98\x8D', '\xF0\x9F\x98\x8F', '\xF0\x9F\x98\x97', '\xF0\x9F\x98\x98',
                    '\xF0\x9F\x98\x99', '\xF0\x9F\x98\x9A', '\xF0\x9F\x98\x9B', '\xF0\x9F\x98\x9C', '\xF0\x9F\x98\x9D', '\xF0\x9F\x98\xB8', '\xF0\x9F\x98\xB9', '\xF0\x9F\x98\xBA',
                    '\xF0\x9F\x98\xBB', '\xF0\x9F\x98\xBD', '\xF0\x9F\x99\x8B', '\xF0\x9F\x99\x8C', '\xF0\x9F\x98\x80', '\xF0\x9F\x98\x87',
                    '\xF0\x9F\x98\x88', '\xF0\x9F\x98\x8E', 
                    '\xF0\x9F\x92\x8B', '\xF0\x9F\x92\x8F', '\xF0\x9F\x92\x91', '\xF0\x9F\x92\x95', '\xF0\x9F\x92\x96', '\xF0\x9F\x92\x97', '\xF0\x9F\x92\x98',
                    '\xF0\x9F\x92\x9D',  
                    '\xF0\x9F\x8C\xB9', '\xF0\x9F\x8E\x80', '\xF0\x9F\x8E\x81', '\xF0\x9F\x8E\x82', '\xF0\x9F\x8E\x86', '\xF0\x9F\x8E\x87', '\xF0\x9F\x8E\x89',
                    '\xF0\x9F\x8E\x8A', 
                    '\xE2\x9C\x8C', '\xE2\x9D\xA4', '\xE2\x98\xBA', '\xE2\x99\xA5', '\xE3\x8A\x97']

    sad_emoji = [' :‑(', ' :(', ' :‑c', ' :c', ' :‑<', ' :<', ' :‑[', ' :[', ' :-||', ' >:[', ' :{', ' :@', ' >:(', ' :\'‑(', ' :\'(',
                ' DX ', ' D= ', ' D; ', ' D: ', ' D:<', ' D‑\':', ' >:O', ' :-0', ' :‑o', ' :o ', ' :‑O', ' :O ', ' :‑/', ' :‑.',
                ' >:\\', ' >:/', ' :\\', ' =/', ' =\\', ' :L', ' =L', ' :S', ' :‑|', ' :|', ' :$ ', '(-_-;)', ' Q_Q', ' TnT', 'T.T', 'Q.Q', ';;', 
                ' ;n;', ' ;-;', ' ;_;', '(T_T)', '(;_:)', '(ToT)', '(ー_ー)!!', '(-.-)', '(-_-)', '(=_=)', '(-"-)', '(ーー;)', '(*￣m￣)', '(＃ﾟДﾟ)',
                '(´；ω；`)', 'ヽ(`Д´)ﾉ', '（ ´,_ゝ`)', 
                '\xF0\x9F\x98\x91', '\xF0\x9F\x98\x92', '\xF0\x9F\x98\x93', '\xF0\x9F\x98\x94', '\xF0\x9F\x98\x95', '\xF0\x9F\x98\x96', '\xF0\x9F\x98\x9E', '\xF0\x9F\x98\x9F', 
                '\xF0\x9F\x98\xA0', '\xF0\x9F\x98\xA1', '\xF0\x9F\x98\xA2', '\xF0\x9F\x98\xA3', '\xF0\x9F\x98\xA4', '\xF0\x9F\x98\xA5', '\xF0\x9F\x98\xA6', '\xF0\x9F\x98\xA7', '\xF0\x9F\x98\xA8', 
                '\xF0\x9F\x98\xA9', '\xF0\x9F\x98\xAB', '\xF0\x9F\x98\xAD', '\xF0\x9F\x98\xB1', '\xF0\x9F\x98\xBE', '\xF0\x9F\x99\x8D',
                '\xF0\x9F\x92\x94', '\xF0\x9F\x92\xA2'
                ]
    output = {}

    print("input_file: {}".format(input_file))
    with open(input_file, 'r') as inf:
        tweets = MyTweetTokenizer()
        output_file = "tagged/{}.txt".format(input_file.split("/")[-1]) 
        with open(output_file, 'w', 100) as outf:
            for line in inf:
                if is_json(line):
                    record = json.loads(line)
                    if u'text' in record:
                        unicode_text = record[u'text']
                        if len(unicode_text) < 20:
                            continue

                        utf8text = unicode_text.encode("utf-8")
                        is_happy, happy_tag, happy_clean_text = find_emoji(utf8text, happy_emoji)
                        is_sad, sad_tag, sad_clean_text = find_emoji(utf8text, sad_emoji)

                        if is_happy and not is_sad:
                            clean_emoji_text = remove_emoji(happy_clean_text)     # remove emoji
                            clean_text = tweets.clean(clean_emoji_text)          # remove others 
                            clean_text = clean_text.replace("http://","").replace("https://","")
                            if len(clean_text) >= 20:
                                output = {'text': clean_text, 'label': '1', 'emoji': happy_tag}
                                outf.write(json.dumps(output) + '\n')
                            else:
                                continue
                        elif not is_happy and is_sad:
                            clean_emoji_text = remove_emoji(sad_clean_text)
                            clean_text = tweets.clean(clean_emoji_text)
                            clean_text = clean_text.replace("http://","").replace("https://","")
                            if len(clean_text) >= 20:
                                output = {'text': clean_text, 'label': '0', 'emoji': sad_tag}
                                outf.write(json.dumps(output) + '\n')
                            else:
                                continue
                    else:
                        continue


# text: str
# emoji: list
# return: bool
def find_emoji(text, emoji):
    flag = False
    flag_emoji = []
    clean_text = text
    for e in emoji:
        if e in text:
            clean_text = clean_text.replace(e, '')
            flag = True
            flag_emoji.append(e)
    return flag, flag_emoji, clean_text


def remove_emoji(text):
    # print type(text), "first" # str
    text = text.decode('utf8')
    # print type(text), "second" # unicode
    text = emoji_pattern.sub(r'', text).encode('utf8')
    return 

def is_json(myjson):
  try:
    json_object = json.loads(myjson)
  except ValueError, e:
    return False
  return True

def run(data_dir):
    files = [os.path.join(data_dir, f) for f in os.listdir(data_dir)]
    start = time.time()
    pool = multiprocessing.Pool(4)
    results = pool.map_async(read_tweets, files)
    pool.close()
    pool.join()
    print(time.time()-start)

if __name__ == '__main__':
    run("/home/p8shi/tweets_english")

