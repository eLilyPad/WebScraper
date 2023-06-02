from urllib.request import urlopen
from datetime import datetime as dt
from bs4 import BeautifulSoup as bs

URL = "http://bbc.co.uk"

class BBC:
    def __init__(self):
        html = urlopen(URL).read().decode("utf_8")
        self.soup = bs(html, "html.parser")

    cont_class = ".e1k195vp5"
    story_class = ".e1f5wbog1"
    metadata_class = ".ecn1o5v1"

    def parse_articles(self):
        stories = []
        articles = self.soup.select(BBC.cont_class)

        only1 = lambda v: len(v) == 1
        for article in articles:
            # print('Article found')
            foundStories = article.select(BBC.story_class)
            metadata = article.select(BBC.metadata_class)
            # if only1(story) and only1(metadata):
            for story in foundStories:
                try:
                    
                    title = story.get_text()
                    link = story.get("href")
                    tag = metadata[0].get_text()
                    # print(
                    #     f"""
                    #     --------------------------------
                    #     Title: {title} 
                    #     Link: {link}
                    #     Tag: {tag}
                    #     --------------------------------
                    #     """
                    #     )
                    stories.append({"title": title, "link": link, "tag": tag})
                except Exception as e:
                    print(e.with_traceback())
        return stories
