import api
import database as db
from scraper import BBC


def send_to_db(stories: list):
    for s in stories:
        title = s.get("title")
        link = s.get("link")
        tag = s.get("tag")

        db.send_title(title)

        # id = db.get_story_id(title)

        # db.send_tag(tag)

        # tag_id = db.get_tag_id(tag)

        # db.send_link(id, link)
        # db.send_metadata(id, tag_id)


def main():
    bbc = BBC()
    stories = bbc.parse_articles()
    send_to_db(stories)
    # print(db.get_stories())
    # print(stories)


if __name__ == "__main__":
    main()
    api.app.run(debug=True, host=api.IP, port=api.PORT)
