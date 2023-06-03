# We'll need this to convert SQL responses into dictionaries
from psycopg2.extras import RealDictCursor
from psycopg2 import Error as psyError, DatabaseError, connect, sql

DB_NAME='social_news'
DB_USER='elizabeth' 
DB_HOST='localhost'
DB_PASSWORD='password'
DB_PORT=5432
SCHEMA_NAME='news'

def send_story(title:str, tag:str, link:str):
    send_title(title)
    send_tag(tag)
    
    story_id = get_story_id(title)
    tag_id = get_tag_id(tag)
    
    send_link(story_id, link)
    if tag_id != -1:
        send_metadata(story_id, tag_id)
    
def send_title(title: str) -> int:
    '''
    Sends the title of the article to the database

    Args:
        title (str): the title of the article

    Returns:
        int: returns 1 if the is successful sent to the database
    '''
    query = """--sql
        insert into news.stories (title) 
        values (%(title)s) 
        on conflict (title) 
        do nothing
        """
    
    return _insert(query, {"title": title}, error_message="Error while sending title to the database")

def send_tag(tag: str) -> int:
    query = """--sql
            insert into news.tags (tag_name) 
            values (%(tag)s)
            on conflict (tag_name) 
            do nothing
    """
    return _insert(query, {"tag": tag}, error_message="Error while sending tag to the database")

def send_link(story_id: int, link: str) -> tuple[int, int]:
    rows = 0
    
    query = """--sql
        insert into news.links (story_id, link) 
        values (%(story_id)s, %(link)s) 
        on conflict (link) 
        do nothing
    """
    return _insert(query, {'link': link, 'story_id': story_id}, error_message="Error while sending link to the database")

def send_metadata(story_id: int, tag_id: int):
    query = """--sql
        insert into news.metadata (story_id, tag_id) 
        values (%(story_id)s, %(tag_id)s)
    """

    return _insert(query, {'story_id': story_id, 'tag_id': tag_id})

def send_log(story_id, description):
    query = """--sql
        insert into news.logs (story_id, description)
        values ({id}, {d})
    """

    return _insert(query)

def get_stories():
    query = """--sql
        select s.id, title, link 
        from news.stories s 
        join news.links l 
        on s.id = l.story_id
    """
    return _retrieve_dict(query)

def get_story_id(title: str):
    data = []
    
    query = """--sql
        select id from news.stories
        where title = %(title)s
    """
    with connection.cursor() as cursor:
        try:
            cursor.execute(query, {'title': title})
            data = cursor.fetchone()
            connection.commit()
        except (Exception, psyError) as error:
            print("Error while fetching story id from database", error)
        finally:
            # closing database cursor
            if cursor:
                cursor.close()
    
    if len(data) > 0:
        return data[0]

def get_tag_id(tag: str) -> int:
    data = -1
    query = """--sql
        select id from news.tags
        where tag_name = %(tag)s
    """
    with connection.cursor() as cursor:
        try:
            cursor.execute(query, {'tag': tag})
            data = cursor.fetchone()
            connection.commit()
        except (Exception, psyError) as error:
            print("Error while fetching tag id from database", error)
        finally:
            # closing database cursor
            if cursor:
                cursor.close()
    if data is None: 
        return -1
    
    if len(data) > 0:
        return data[0]

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

def _insert(query:str, params: dict, error_message: str = "Error while sending data to database") -> int:
    rows = 0
    with connection.cursor() as cursor:
        try:
            cursor.execute(query, params)
            rows = cursor.rowcount
            connection.commit()
        except Exception as error:
            print(error_message, error)
            connection.rollback()
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
