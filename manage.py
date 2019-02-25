import sys
from listenbrainz_spark.stats import user
from listenbrainz_spark.stats import artist
from listenbrainz_spark.stats import release
from listenbrainz_spark.stats import recording


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: manage.py <module_name> <app_name>")
        sys.exit(-1)

    module_name = sys.argv[1]
    if module_name == 'user':
        user.main(app_name=sys.argv[2])
    elif module_name == 'artist':
        artist.main(app_name=sys.argv[2])
    elif module_name == 'release':
        release.main(app_name=sys.argv[2])
    elif module_name == 'recording':
        recording.main(app_name=sys.argv[2])
