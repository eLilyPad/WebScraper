from flask import Flask, current_app, jsonify, request

import database as db

# from database import update_score, db_query
PORT='5001'
IP='0.0.0.0'

app = Flask(__name__)



@app.route("/", methods=["GET"])
def index():
    return current_app.send_static_file("index.html")


@app.route("/stories", methods=["GET"])
def stories():
    response = {
        "stories": {},
        "success": False,
        "total_stories": 0,
    }
    try:
        rec = db.get_stories()
        if rec:
            response["stories"] = rec
            response["success"] = True
            response["total_stories"] = len(rec)
    except TypeError:
        print("no stories was retrieved from ")

    return jsonify(response), 200


@app.route("/stories/<int:id>/votes", methods=["POST"])
def vote(id):
    data = request.json
    # update_score(id, data.get("direction"))
    return {"success": True}


@app.route("/search", methods=["GET"])
def search():
    results = []
    queries = []
    split_if_many = lambda x: x.split(",") if "," in x else x

    if "tags" in request.args:
        tags = split_if_many(request.args["tags"])
        queries.append({"tags": tags})

    if "titles" in request.args:
        titles = split_if_many(request.args["titles"])
        queries.append({"titles": titles})

    for query in queries:
        tags = query.get("tags")
        titles = query.get("titles")

        # if tags and titles:
        #     for tag, title in zip()
        if tags:
            for t in tags:
                results += db.stories_by_tag(tag=t)
        if titles:
            for t in titles:
                results += db.stories_by_title(title=t)

    return jsonify(results), 200
