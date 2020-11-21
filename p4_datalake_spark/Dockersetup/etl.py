#%%

import configparser
import p4src


def main(configpath, logpath, songpath):
    # Load config file
    config = configparser.ConfigParser()
    config.read(configpath)

    # Create spark session
    print('\n***\nstarting script and launching session\n')
    ss = p4src.get_spark(config)
    print('spark session created')
    # Read log data from s3
    print('read logs')
    staging_events = p4src.read_logs(ss, logpath)
    print('staging events {} lines'.format(staging_events.count()))



    # Read song data from S3
    print('read songs')
    staging_songs = p4src.read_songs(ss, songpath)
    print('staging songs {} lines'.format(staging_songs.count()))

    # Transform the dimension tables
    print('select tables')
    songs = p4src.select_songs_data(staging_songs)
    artists = p4src.select_artists_data(staging_songs)
    users = p4src.select_users_data(staging_events)
    time = p4src.select_time_data(staging_events)

    ## Cache the songs and artists table in order not to recalculate it
    songs = songs.cache()
    artists = artists.cache()
    # Print rows
    print('songs', songs.count())
    print('artists', artists.count())
    print('users', users.count())
    print('time', time.count())

    # Create the songplay fact table
    songplay = p4src.select_songplay(staging_events, songs, artists)

    # Check if the data performed correctly
    r = songplay.count()
    print('\n***\n***\nsongplay {} lines\n***\n***\n'.format(r))

    # Write the data to S3
    output_path = "s3a://dendpaulogieruswest2/p4csv/"
    print('write to s3')
    print('songs..')
    songs.write.partitionBy("year", "artist_id").parquet(output_path + 'songs/', mode='overwrite')
    print('artists..')
    artists.write.parquet(output_path + 'artists/', mode='overwrite')
    print('users..')
    users.write.parquet(output_path + 'users/', mode='overwrite')
    print('time..')
    time.write.partitionBy("year", "month").parquet(output_path + 'time/', mode='overwrite')
    print('songplay..')
    songplay.write.partitionBy("year", "month").parquet(output_path + 'songplay/', mode='overwrite')
    print('write finished')
    return None

if __name__ == '__main__':
    logpath = "s3a://udacity-dend/log_data/*/*/.json"
    songpath = "s3a://udacity-dend/song_data/*/*/*/*.json"
    configpath = '/home/jovyan/aws/config.cfg'
    print('Launching scrip etl')
    main(configpath, logpath, songpath)
    print('ETL script successfull')
    pass




