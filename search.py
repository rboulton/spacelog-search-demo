from flask import Flask, render_template, request
from elasticsearch import Elasticsearch

app = Flask(__name__)
app.debug = True


def build_query(query_string):
    return {"function_score": {
        "boost_mode": "multiply",
        "query": text_query(query_string),
        "script_score": {
            "script": "2 * doc['weight'].value",
        }
    }}

def text_query(query_string):
    return {
        "bool": {
            "should": [
                {"match": { "text": query_string }},
                {"match": { "text.stemmed": query_string },},
                {"match": { "text.stemmed": query_string },},
                {"match": { "text.stemmed": query_string },},
                {"match": { "text.stemmed": query_string },},
            ]
        }
    }

@app.route('/')
def search():
    es = Elasticsearch()
    query_string = request.args.get('q', '')
    result = es.search(index="missions", body={
        "query": build_query(query_string),
        "size": 100,
    })
    return render_template("search.html", **{
        "hits": result["hits"]["hits"],
        "results": result["hits"]["total"],
        "query_string": query_string,
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0")
