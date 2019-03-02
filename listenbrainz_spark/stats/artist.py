import listenbrainz_spark
import time
from listenbrainz_spark import config
from listenbrainz_spark.stats import run_query
from dateutil.relativedelta import relativedelta
from datetime import datetime 
from collections import defaultdict

def get_releases():
    result = run_query("""
        SELECT artist_msid
             , release_name
             , release_mbid
             , release_msid
             , count(release_msid) as cnt
          FROM listen
      GROUP BY artist_msid, release_name, release_mbid, release_msid
      ORDER BY cnt DESC
    """)
    result.show()
    releases = defaultdict(list)
    for row in result.collect():
        releases[row.artist_msid].append({
            'release_name' : row.release_name,
            'release_msid' : row.release_msid,
            'release_mbid' : row.release_mbid,
            'count' : row.cnt,
        })
    return releases

def get_recordings():
    result = run_query("""
        SELECT artist_msid
             , track_name
             , recording_mbid
             , recording_msid
             , count(recording_msid) as cnt
          FROM listen
      GROUP BY artist_msid, track_name, recording_mbid, recording_msid
      ORDER bY cnt DESC
    """)
    result.show()
    recordings = defaultdict(list)
    for row in result.collect():
        recordings[row.artist_msid].append({
            'recording_name' : row.track_name,
            'recording_msid' : row.recording_msid,
            'recording_mbid' : row.recording_mbid,
            'cnt' : row.cnt,
        })
    return recordings

def get_listener():
    result = run_query("""
        SELECT artist_msid
             , user_name
             , count(artist_msid) as cnt
          FROM listen
      GROUP BY artist_msid, user_name
      ORDER BY cnt DESC
    """)
    result.show()
    listener = defaultdict(list)
    for row in result.collect():
        listener[row.artist_msid].append({
            'user_name': row.user_name,
            'listen_count' : row.cnt,
        })
    return listener

def get_listen_count():
    result = run_query("""
        SELECT artist_msid
             , artist_name
             , count(artist_msid) as cnt
          FROM listen
      GROUP BY artist_msid, artist_name
      ORDER BY cnt DESC
    """)
    result.show()
    artist_count = defaultdict(dict)
    for row in result.collect():
        artist_count[row.artist_msid] = {
            'listen_count' : row.cnt,
            'artist_name' : row.artist_name,
        }
    return artist_count
    
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
    releases = get_releases()
    for artist_mbid, release in releases.items():
        data[artist_mbid]['release'] = release
    recordings = get_recordings()
    for artist_mbid, recording in recordings.items():
        data[artist_mbid]['recording'] = recording
    listeners = get_listener()
    for artist_mbid, listener in listeners.items():
        data[artist_mbid]['listener'] = listener
    artist = get_listen_count()
    for artist_mbid, artist_data in artist.items():
        data[artist_mbid]['artist'] = artist_data
    top_artist_msids = sorted(data, key=lambda x: data[x]['artist']['listen_count'], reverse=True)
    limit = min(100, len(top_artist_msids))
    for i in range(0, limit+1):
        print (data[top_artist_msids[i]])
    print("time = %.2f" % (time.time() - t0))
