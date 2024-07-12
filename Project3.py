from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient
import json
load_dotenv(find_dotenv())
password = os.environ.get("MONGODB_PWD")

connection_string = 

client = MongoClient(connection_string)

jeopardy_db = client.jeopardy_db
question = jeopardy_db.question
printer = pprint.PrettyPrinter()

def insert_json():
    with open('JEOPARDY_QUESTIONS1.json') as jp_file:
        jp_file = json.load(jp_file)

    questions.insert_many(jp_file)

def fuzzy_matching():
    result = question.aggregate([
        {
            "$search": {
                "index": "language_search",
                "text": {
                    "query": "computer",
                    "path": "category"
                }
            }
        }
    ])
    printer.pprint(list(result))

def autocomplete():
    result = question.aggregate([
        {
            "$search": {
                "index": "language_search",
                "autocomplete": {
                        "query": "computer programmer",
                        "path": "question",
                        "tokenOrder": "sequential",
                        "fuzzy": {}
                }
            }
        },
        {
                "$project": {
                    "_id": 0,
                    "question": 1
                }
        }
    ])
    printer.pprint(list(result))

def compound_queries():
    result = question.aggregate([
        {
            "$search": {
                "index": "language_search",
                "compound": {
                    "must": [
                        {
                            "text": {
                                "query": ["COMPUTER", "CODING"],
                                "path": "category"
                            }
                        }
                    ],
                    "mustNot": [{
                        "text": {
                            "query": "codes",
                            "path": "category"
                        }
                    }],
                    "should": [
                        {
                            "text": {
                                "query": "application",
                                "path": "answer"
                            }
                        }
                    ]
                }
            }
        },
        {
            "$project": {
                "question": 1,
                "answer": 1,
                "category": 1,
                "score": {"$meta": "searchScore"}
            }
        }
    ])

    printer.pprint(list(result))

def relevance():
    result = question.aggregate([
        {
            "$search": {
                "index": "language_search",
                "compound": {
                    "must": [
                        {
                            "text": {
                                "query": "geography",
                                "path": "category"
                            }
                        }
                    ],
                    "should": [
                        {
                            "text": {
                                "query": "Final Jeopardy",
                                "path": "round",
                                "score": {"boost": {"value": 3.0}}
                            }
                        },
                        {
                            "text": {
                                "query": "Double Jeopardy",
                                "path": "round",
                                "score": {"boost": {"value": 2.0}}
                            }
                        }
                    ]
                }
            }
        },
        {
            "$project": {
                "question": 1,
                "answer": 1,
                "category": 1,
                "round": 1,
                "score": {"$meta": "searchScore"}
            }
        },
        {
            "$limit": 10
        }
    ])

    printer.pprint(list(result))


        









