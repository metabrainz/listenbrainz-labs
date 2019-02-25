import listenbrainz_spark
import time
from listenbrainz_spark import config
from listenbrainz_spark.stats import run_query


def get_most_popular():
    result = run_query("""
        SELECT artist_msid
             , artist_name
             , release_msid
             , release_name
             , count(release_msid) as cnt
          FROM listen
      GROUP BY artist_msid, artist_name, release_msid, release_name
      ORDER BY cnt DESC
    """)
    result.show()



def main(app_name):
    t0 = time.time()
    listenbrainz_spark.init_spark_session(app_name)
    df = None
    for y in range(2018, 2019):
        for m in range(12, 13):
            try:
                month = listenbrainz_spark.sql_context.read.parquet('{}/data/listenbrainz/{}/{}.parquet'.format(config.HDFS_CLUSTER_URI, y, m))
                df = df.union(month) if df else month
            except:
                print("no data for %02d/%d" % (m, y))
    print("Dataframe loaded")
    print(df.count())
    df.registerTempTable('listen')
    print("querying...")
    get_most_popular()
    print("time = %.2f" % (time.time() - t0))
