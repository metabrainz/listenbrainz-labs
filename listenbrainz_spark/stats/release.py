import listenbrainz_spark
import time
from listenbrainz_spark import config
from listenbrainz_spark.stats import run_query
from dateutil.relativedelta import relativedelta
from datetime import datetime 
from collections import defaultdict

def get_recordings():
    result = run_query("""
        SELECT release_msid
             , track_name
             , recording_mbid
             , recording_msid
             , count(recording_msid) as cnt
          FROM listen
      GROUP BY release_msid, track_name, recording_mbid, recording_msid
      ORDER bY cnt DESC
    """)
    result.show()
    recordings = defaultdict(list)
    for row in result.collect():
        recordings[row.release_msid].append({
            'track_name' : row.track_name,
            'recording_msid' : row.recording_msid,
            'recording_mbid' : row.recording_mbid,
            'cnt' : row.cnt,
        })
    return recordings

def get_listener():
    result = run_query("""
        SELECT release_msid
             , user_name
             , count(release_msid) as cnt
          FROM listen
      GROUP BY release_msid, user_name
      ORDER BY cnt DESC
    """)
    result.show()
    listener = defaultdict(list)
    for row in result.collect():
        listener[row.release_msid].append({
            'user_name': row.user_name,
            'listen_count' : row.cnt,
        })
    return listener

def get_listen_count():
    result = run_query("""
        SELECT release_msid
             , release_name
             , count(release_msid) as cnt
          FROM listen
      GROUP BY release_msid, release_name
      ORDER BY cnt DESC
    """)
    result.show()
    release_count = defaultdict(dict)
    for row in result.collect():
        release_count[row.release_msid] = {
            'listen_count' : row.cnt,
            'release_name' : row.release_name,
        }
    return release_count

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
    recordings = get_recordings()
    for release_msid, recording in recordings.items():
        data[release_msid]['recording'] = recording
    listeners = get_listener()
    for release_msid, listener in listeners.items():
        data[release_msid]['listener'] = listener
    release = get_listen_count()
    for release_msid, release_data in release.items():
        data[release_msid]['release'] = release_data
    top_release_msids = sorted(data, key=lambda x: data[x]['release']['listen_count'], reverse=True)
    limit = min(100, len(top_release_msids))
    for i in range(0, limit+1):
        print (data[top_release_msids[i]])
    print("time = %.2f" % (time.time() - t0))