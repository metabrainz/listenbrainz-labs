import listenbrainz_spark
import time
from listenbrainz_spark import config
from listenbrainz_spark.stats import run_query
from dateutil.relativedelta import relativedelta
from datetime import datetime 
from collections import defaultdict

def get_listener():
    result = run_query("""
        SELECT recording_msid
             , user_name
             , count(recording_msid) as cnt
          FROM listen
      GROUP BY recording_msid, user_name
      ORDER BY cnt DESC
    """)
    result.show()
    listener = defaultdict(list)
    for row in result.collect():
        listener[row.recording_msid].append({
            'user_name': row.user_name,
            'listen_count' : row.cnt,
        })
    return listener

def get_listen_count():
    result = run_query("""
        SELECT recording_msid
             , track_name
             , count(recording_msid) as cnt
          FROM listen
      GROUP BY recording_msid, track_name
      ORDER BY cnt DESC
    """)
    result.show()
    recording_count = defaultdict(dict)
    for row in result.collect():
        recording_count[row.recording_msid] = {
            'listen_count' : row.cnt,
            'track_name' : row.track_name,
        }
    return recording_count

def main(app_name):
    t0 = time.time()
    listenbrainz_spark.init_spark_session(app_name)
    df = None
    t = datetime.utcnow().replace(day=1)
    date = t + relativedelta(months=-2)
    for y in range(date.year, date.year + 1):
        for m in range(date.month, date.month + 1):
            try:
                month = listenbrainz_spark.sql_context.read.parquet('{}/data/listenbrainz/{}/{}.parquet'.format(config.HDFS_CLUSTER_URI, y, m))
                df = df.union(month) if df else month
            except:
                print("no data for %02d/%d" % (m, y))
    print("Dataframe loaded")
    print(df.count())
    df.registerTempTable('listen')
    print("querying...")
    data = defaultdict(dict)
    listeners = get_listener()
    for recording_msid, listener in listeners.items():
        data[recording_msid]['listener'] = listener
    recording = get_listen_count()
    for recording_msid, recording_data in recording.items():
        data[recording_msid]['recording'] = recording_data
    top_recording_msids = sorted(data, key=lambda x: data[x]['recording']['listen_count'], reverse=True)
    limit = min(100, len(top_recording_msids))
    for i in range(0, limit+1):
        print (data[top_recording_msids[i]])
    print("time = %.2f" % (time.time() - t0))