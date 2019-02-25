import sys
from listenbrainz_spark.stats import user
from listenbrainz_spark.stats import artist


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: manage.py <module_name> <app_name>")
        sys.exit(-1)

    module_name = sys.argv[1]
    if module_name == 'user':
        user.main(app_name=sys.argv[2])
    elif module_name == 'artist':
        artist.main(app_name=sys.argv[2])
