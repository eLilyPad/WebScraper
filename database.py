# We'll need this to convert SQL responses into dictionaries
from psycopg2.extras import RealDictCursor
from psycopg2 import Error as psyError, DatabaseError, connect, sql

DB_NAME='social_news'
DB_USER='elizabeth' 
DB_HOST='localhost'
DB_PASSWORD='password'
DB_PORT=5432
SCHEMA_NAME='news'


def send_title(title: str) -> int:
    '''
    Sends the title of the article to the database

    Args:
        title (str): the title of the article

    Returns:
        int: returns 1 if the is successful sent to the database
    '''
    
    rows = 0
    
    query = """--sql
        insert into news.stories (title) 
        values (%(title)s) 
        on conflict (title) 
        do nothing
    """

    with connection.cursor() as cursor:
        try:
            cursor.execute(query, {'title': title})
            rows = cursor.rowcount
            connection.commit()
            
            # storyID = get_story_id(title)
            # send_log(storyID, "title has been added to this story")
        except DatabaseError as error:
            print("something went wrong :(")
            print(error)
        finally:
            if cursor:
                cursor.close()
            return rows


def send_tag(tag) -> int:
    query = sql.SQL(
        """--sql
            insert into news.tags (tag_name) 
            values ({t})
            on conflict (tag_name) 
            do nothing
        """
    ).format(t=sql.Composable(tag))

    return _insert(query)


def send_link(story_id: int, link: str) -> tuple[int, int]:
    rows = 0
    
    query = """--sql
        insert into news.links (link) 
        values (%(story_id)s, %(link)s) 
        on conflict (link) 
        do nothing
    """
    
    with connection.cursor() as cursor:
        try:
            cursor.execute(query, {'link': link, 'story_id': story_id})
            rows = cursor.rowcount
            connection.commit()
        except DatabaseError as error:
            print("something went wrong :(")
            print(error)
        finally:
            if cursor:
                cursor.close()
            return rows
    
    query = sql.SQL(
        """--sql
            insert into news.links (story_id, link) 
            values ({id}, {l}) 
            on conflict (link) 
            do nothing
        """
    ).format(id=sql.Composable(story_id), l=sql.Composable(link))

    # send_log(story_id, "link has been added to this story")

    return _insert(query)


def send_metadata(story_id: int, tag_id: int):
    query = sql.SQL(
        """--sql
            insert into news.metadata (story_id, tag_id) 
            values ({id}, {t_id})
        """
    ).format(id=sql.Composable(story_id), t_id=sql.Composable(tag_id))

    log = send_log(story_id, "metadata has been added to this story")

    return _insert(query), log


def send_log(story_id, description):
    query = sql.SQL(
        """--sql
            insert into news.logs (story_id, description)
            values ({id}, {d})
        """
    ).format(id=sql.Composable(story_id), d=sql.Composable(description))

    return _insert(query)


def get_stories():
    query = """--sql
        select s.story_id, title, link 
        from news.stories s 
        join news.links l 
        on s.story_id = l.story_id
    """
    return _retrieve_dict(query)


def get_story_id(title: str):
    query = sql.SQL(
        """--sql
            select id from news.stories
            where title = {t}
        """
    ).format(t=sql.Composable(title))

    return _retrieve(query)[0][0]


def get_tag_id(tag):
    query = sql.SQL(
        """--sql
            select id from news.tags
            where tag_name = {t}
        """
    ).format(t=sql.Composable(tag))
    return _retrieve(query)[0][0]


def stories_by_tag(tag: str):
    query = sql.SQL(
        """--sql
            select story_id, title, link from news.stories
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
    ).format(t=sql.Composable(f"%{tag}%"))

    return _retrieve(query)


def stories_by_title(title: str):
    query = sql.SQL(
        """--sql
            select * from news.stories
            where title like {t}
        """
    ).format(t=sql.Composable(f"%{title}%"))

    return _retrieve(query)


def _insert(query, value) -> int:
    rows = 0
    with connection.cursor() as cursor:
        try:
            cursor.execute(query, value)
            rows = cursor.rowcount
            connection.commit()
        except DatabaseError as error:
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
