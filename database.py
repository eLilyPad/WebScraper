# We'll need this to convert SQL responses into dictionaries
from psycopg2.extras import RealDictCursor
from psycopg2 import Error as psyError, DatabaseError, connect, sql

DB_NAME='social_news'
DB_USER='elizabeth' 
DB_HOST='localhost'
DB_PASSWORD='password'
DB_PORT=5432
SCHEMA_NAME='news'


def send_title(title: str):
    query = sql.SQL(
        """--sql
            insert into {schema_name}.stories (title) 
            values ({t}) 
            on conflict (title) 
            do nothing
        """
    ).format(t=sql.Composable(title), schema_name=sql.Composable(SCHEMA_NAME))

    result = _insert(query)
    
    storyID = get_story_id(title)

    send_log(storyID, "title has been added to this story")

    return result


def send_tag(tag):
    query = sql.SQL(
        """--sql
            insert into {schema_name}.tags (tag_name) 
            values ({t})
            on conflict (tag_name) 
            do nothing
        """
    ).format(t=sql.Composable(tag), schema_name=sql.Composable(SCHEMA_NAME))

    return _insert(query)


def send_link(story_id: int, link: str):
    query = sql.SQL(
        """--sql
            insert into {schema_name}.links (story_id, link) 
            values ({id}, {l}) 
            on conflict (link) 
            do nothing
        """
    ).format(id=sql.Composable(story_id), l=sql.Composable(link), schema_name=sql.Composable(SCHEMA_NAME))

    log = send_log(story_id, "link has been added to this story")

    return _insert(query), log


def send_metadata(story_id: int, tag_id: int):
    query = sql.SQL(
        """--sql
            insert into {schema_name}.metadata (story_id, tag_id) 
            values ({id}, {t_id})
        """
    ).format(id=sql.Composable(story_id), t_id=sql.Composable(tag_id), schema_name=sql.Composable(SCHEMA_NAME))

    log = send_log(story_id, "metadata has been added to this story")

    return _insert(query), log


def send_log(story_id, description):
    query = sql.SQL(
        """--sql
            insert into {schema_name}.logs (story_id, description)
            values ({id}, {d})
        """
    ).format(id=sql.Composable(story_id), d=sql.Composable(description), schema_name=sql.Composable(SCHEMA_NAME))

    return _insert(query)


def get_stories():
    query = f"""--sql
        select story_id, title, link from {SCHEMA_NAME}.stories
        join news.links 
        on stories.id = news.links.story_id 
    """
    return _retrieve_dict(query)


def get_story_id(title: str):
    query = sql.SQL(
        """--sql
            select id from {schema_name}.stories
            where title = {t}
        """
    ).format(t=sql.Composable(title), schema_name=sql.Composable(SCHEMA_NAME))

    print(query.__str__())

    return _retrieve(query)[0][0]


def get_tag_id(tag):
    query = sql.SQL(
        """--sql
            select id from {schema_name}.tags
            where tag_name = {t}
        """
    ).format(t=sql.Composable(tag), schema_name=sql.Composable(SCHEMA_NAME))
    return _retrieve(query)[0][0]


def stories_by_tag(tag: str):
    query = sql.SQL(
        """--sql
            select story_id, title, link from {schema_name}.stories
            join news.links
            on stories.id = news.links.story_id 
            where stories.id in ( 
                select story_id from news.metadata 
                where tag_id in (
                    select id from news.tags
                    where tag_name like '%u%'
                )
            )
        """
    ).format(t=sql.Composable(f"%{tag}%"), schema_name=sql.Composable(SCHEMA_NAME))

    return _retrieve(query)


def stories_by_title(title: str):
    query = sql.SQL(
        """--sql
            select * from {schema_name}.stories
            where title like {t}
        """
    ).format(t=sql.Composable(f"%{title}%"), schema_name=sql.Composable(SCHEMA_NAME))

    return _retrieve(query)


def _insert(sql):
    rows = 0
    with connection.cursor() as cursor:
        try:
            cursor.execute(sql)
            rows = cursor.rowcount
            connection.commit()
        except (Exception, DatabaseError) as error:
            print("something went wrong :(")
            print(error)
        finally:
            if cursor:
                cursor.close()
            return rows


def _retrieve(sql):
    data = []
    with connection.cursor() as cursor:
        try:
            cursor.execute(sql)
            data = cursor.fetchall()
            connection.commit()
        except (Exception, psyError) as error:
            print("Error while fetching data from PostgreSQL", error)
        finally:
            # closing database cursor
            if cursor:
                cursor.close()
            return data


def _retrieve_dict(sql):
    data = []
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        try:
            cursor.execute(sql)
            data = cursor.fetchall()
            connection.commit()
        except (Exception, psyError) as error:
            print("Error while fetching data from PostgreSQL", error)
        finally:
            # closing database cursor
            if cursor:
                cursor.close()
            return data


def _get_db_connection():
    try:
        # conn = connect(
        #     f'dbname={DB_NAME}, user={DB_USER}, password={DB_PASSWORD}, host={DB_HOST}'
        # )
        conn = connect(
            database=DB_NAME, 
            user=DB_USER, 
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except (Exception) as error:
        print("Error connecting to database.", error)


connection = _get_db_connection()
