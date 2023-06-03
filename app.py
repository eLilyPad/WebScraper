import api
import database as db
from scraper import BBC


def send_to_db(stories: list):
    for s in stories:
        title = s.get("title")
        link = s.get("link")
        tag = s.get("tag")
        
        db.send_story(title, tag, link)

def main():
    bbc = BBC()
    stories = bbc.parse_articles()
    send_to_db(stories)

if __name__ == "__main__":
    main()
    api.app.run(debug=True, host=api.IP, port=api.PORT)
