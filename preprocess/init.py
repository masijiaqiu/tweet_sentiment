import multiprocessing
import os
import pandas as pd
import time

cleaned_dir = "/home/l8tan/clean_tweets"
data_dir = "/collections/TweetsCrawl/us-east/2015-07/"

def work(filename):

    try:
        reader = pd.read_json(filename, compression='gzip',
                          chunksize=5000, convert_axes=False, convert_dates=False,
                          orient='records', typ='frame', lines=True)

        filename = filename.split('/')[-1].split('.')[-2]
        print(filename)
        fout = open(os.path.join(cleaned_dir, filename), mode='w')
        for chunk in reader:
            # early data doesn't have 'timestamp_ms'
            df = chunk.dropna(subset=['text', 'id', 'lang', 'created_at'])

            english = df[['text', 'id', 'lang', 'created_at']][df.lang == 'en']

            fout.write(english.to_json(orient='records', lines=True))
        fout.close()
    except ValueError:
        pass
start = time.time()
pool = multiprocessing.Pool(10)
files = [os.path.join(data_dir, f) for f in os.listdir(data_dir)]

results = pool.map_async(work, files)
pool.close()
pool.join()
print(time.time()-start)
