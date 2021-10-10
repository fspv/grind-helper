import logging
import os
import random
from functools import lru_cache
from typing import List, Tuple

import google.appengine.api
import leetcode
from flask import Flask, render_template, request
from google.appengine.api import memcache, taskqueue
from google.cloud import secretmanager

logging.getLogger().setLevel(logging.DEBUG)

app = Flask(__name__)
app.wsgi_app = google.appengine.api.wrap_wsgi_app(app.wsgi_app)
app.logger.setLevel(logging.DEBUG)


@lru_cache(None)
def get_leetcode_client() -> leetcode.DefaultApi:
    """
    Singleton to initialize leetcode client. Only one instance will be created
    """
    client = secretmanager.SecretManagerServiceClient()

    csrf_token = client.access_secret_version(
        request={"name": "projects/779116764331/secrets/LEETCODE_CSRF_TOKEN/versions/1"}
    ).payload.data.decode("UTF-8")

    leetcode_session = client.access_secret_version(
        request={"name": "projects/779116764331/secrets/LEETCODE_SESSION/versions/1"}
    ).payload.data.decode("UTF-8")

    configuration = leetcode.Configuration()

    configuration.api_key["x-csrftoken"] = csrf_token
    configuration.api_key["csrftoken"] = csrf_token
    configuration.api_key["LEETCODE_SESSION"] = leetcode_session
    configuration.api_key["Referer"] = "https://leetcode.com"
    configuration.debug = False

    return leetcode.DefaultApi(leetcode.ApiClient(configuration))


def get_problem_detail(slug: str) -> leetcode.GraphqlQuestionDetail:
    """
    Make a request to leetcode API to get more details about the question
    """
    graphql_request = leetcode.GraphqlQuery(
        query="""
            query getQuestionDetail($titleSlug: String!) {
              question(titleSlug: $titleSlug) {
                title
                topicTags {
                  name
                  slug
                }
              }
            }
        """,
        variables=leetcode.GraphqlQueryVariables(title_slug=slug),
        operation_name="getQuestionDetail",
    )

    api_instance = get_leetcode_client()

    api_response = api_instance.graphql_post(body=graphql_request)

    return api_response.data.question


def check_cache_tag(slug: str) -> bool:
    """
    Check the information about a tag is in the cache
    """
    return memcache.Client().get(f"tag_{slug}_name") is not None


def check_cache_problem(slug: str) -> bool:
    """
    Check we cached all the information about the problem
    """
    return (
        memcache.Client().get(f"{slug}_tags") is not None
        and memcache.Client().get(f"problem_{slug}_tags") is not None
        and all(
            check_cache_tag(slug)
            for slug in memcache.Client().get(f"problem_{slug}_tags")
        )
        is not None
        and memcache.Client().get(f"problem_{slug}_title") is not None
    )


@app.route("/invalidate_cache_schedule", methods=["GET"])
def invalidate_cache_schedule() -> Tuple[str, int]:
    """
    Create tasks to invalidate the information about all the leetcode
    problems. Those tasks will be later picked up by workers
    """

    # Authenticate appengine cron
    if not request.headers.get("X-AppEngine-Cron", False):
        logging.error("No cron header set")
        return "FAIL", 403

    api_instance = get_leetcode_client()

    response: List[str] = []

    for topic in ["algorithms", "shell", "databases", "concurrency"]:
        api_response = api_instance.api_problems_topic_get(topic=topic)

        for slug in (
            pair.stat.question__title_slug for pair in api_response.stat_status_pairs
        ):
            logging.info(f"Schedule invalidation for {slug}")
            if check_cache_problem(slug):
                logging.info(f"{slug} is already in the cache")
            else:
                task = taskqueue.add(
                    url="/invalidate_cache", target="worker", params={"slug": slug}
                )

                response.append(
                    f"Task {task.name} for slug {slug} enqueued, ETA {task.eta}."
                )

    return "<br/>".join(response), 200


@app.route("/invalidate_cache", methods=["POST"])
def invalidate_cache() -> Tuple[str, int]:
    """
    Method to cache the information about a particular problem
    """
    # Authenticate appengine queue
    if not request.headers.get("X-AppEngine-QueueName", False):
        logging.error("No task queue header set")
        return "FAIL", 403

    # Set randon TTL, so cache doesn't expire all at once
    cache_for = 86400 * 30 + random.randint(-86400, 86400)

    slug = request.form["slug"]
    if check_cache_problem(slug):
        logging.info(f"{slug} is already in the cache")
    else:
        logging.info(f"Updating slug {slug}")

        detail = get_problem_detail(slug)
        tags = [tag.slug for tag in detail.topic_tags]

        for tag in detail.topic_tags:
            logging.info(f"Updating tag {tag.slug} for problem {slug}")
            memcache.Client().set(
                f"tag_{tag.slug}_name",
                tag.name,
                time=cache_for,
            )

        memcache.Client().set(f"{slug}_tags", tags, time=cache_for)
        memcache.Client().set(
            f"problem_{slug}_tags",
            tags,
            time=cache_for,
        )
        memcache.Client().set(
            f"problem_{slug}_title",
            detail.title,
            time=cache_for,
        )

        logging.info(f"Finished updating slug {slug}")

    return "OK", 200


@app.route("/")
def main():
    return render_template("main.html")


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
