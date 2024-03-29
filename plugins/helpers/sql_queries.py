class SqlQueries:
    songplay_table_insert = """
        SELECT
            md5(events.session_id || events.start_time) songplay_id,
            events.start_time,
            events.user_id,
            events.level,
            songs.song_id,
            songs.artist_id,
            events.session_id,
            events.location,
            events.useragent
        FROM(
            SELECT
                TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
                *
            FROM staging_events
            WHERE page='NextSong'
        ) AS events
        LEFT JOIN staging_songs AS songs
        ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
    """

    user_table_insert = """
        SELECT DISTINCT
            user_id,
            firstname,
            lastname,
            gender,
            level
        FROM staging_events
        WHERE page='NextSong'
    """

    song_table_insert = """
        SELECT DISTINCT
            song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
    """

    artist_table_insert = """
        SELECT DISTINCT
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM staging_songs
    """

    time_table_insert = """
        SELECT
            start_time,
            extract(hour from start_time),
            extract(day from start_time),
            extract(week from start_time),
            extract(month from start_time),
            extract(year from start_time),
            extract(dayofweek from start_time)
        FROM songplays
    """
