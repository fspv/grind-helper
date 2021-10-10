import asyncio
import json
import logging
import os
import random
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Set, Tuple

import google.appengine.api
import wtforms
from flask import Flask, render_template, request
from google.appengine.api import memcache

logging.getLogger().setLevel(logging.DEBUG)
app = Flask(__name__)
app.wsgi_app = google.appengine.api.wrap_wsgi_app(app.wsgi_app)
app.logger.setLevel(logging.DEBUG)


class LeetcodeProblemsForms(wtforms.Form):
    problems = wtforms.TextAreaField("problems", [wtforms.validators.InputRequired()])

    def validate_problems(self, form_field):
        """
        Validate json input of the form

        This is the data one can get from https://leetcode.com/api/problems/algorithms/

        Example of data you can get.

        {
            "user_name":"omgitspavel",
            "num_solved":770,
            "num_total":1840,
            "ac_easy":182,
            "ac_medium":469,
            "ac_hard":119,
            "stat_status_pairs":[
                {
                    "stat":{
                        "question_id":2162,
                        "question__article__live":null,
                        "question__article__slug":null,
                        "question__article__has_video_solution":null,
                        "question__title":"Partition Array Into Two Arrays to Minimize Sum Difference",
                        "question__title_slug":"partition-array-into-two-arrays-to-minimize-sum-difference",
                        "question__hide":false,
                        "total_acs":1237,
                        "total_submitted":6813,
                        "frontend_question_id":2035,
                        "is_new_question":false
                    },
                    "status":null,
                    "difficulty":{
                        "level":3
                    },
                    "paid_only":false,
                    "is_favor":false,
                    "frequency":0,
                    "progress":0.0
                }
            ]
        }
        """
        data = json.loads(form_field.data)

        if "stat_status_pairs" not in data:
            raise wtforms.ValidationError("No stat_status_pairs in request")

        stat_status_pairs = data["stat_status_pairs"]

        if not isinstance(stat_status_pairs, list):
            raise wtforms.ValidationError("stat_status_pairs must be a list")

        for stat in stat_status_pairs:
            if "stat" not in stat:
                raise wtforms.ValidationError("Provided JSON is not correct")

            question_data = stat["stat"]

            if "question__title_slug" not in question_data:
                raise wtforms.ValidationError("Provided JSON is not correct")

            slug = question_data["question__title_slug"]

            if not isinstance(slug, str):
                raise wtforms.ValidationError("Provided JSON is not correct")

            if not re.match("[a-z0-9][a-z0-9-]*[a-z0-9]", slug):
                raise wtforms.ValidationError("Provided JSON is not correct")


async def get_tags(slug: str) -> List[str]:
    """
    Get tags for problem slug from the cache
    """
    tags: List[str] = []

    try:
        tags = memcache.Client().get(f"{slug}_tags") or []
    except Exception:
        logging.exception("Failed to get tags for %s", slug)

    return tags


@dataclass
class TagInfo:
    solved: Set[str] = field(default_factory=set)
    left: Set[str] = field(default_factory=set)


async def interpret(
    problems: Dict[str, Any], form: LeetcodeProblemsForms
) -> Tuple[str, int]:
    """
    Interpret problems data got from the form.

    Two main outcomes of this method:
        1. Calculate solved/total ratio per tag
        2. Pick a random problem to solve bases on this ratio
           (least touched tags are prioritized)
    """
    slug_to_solved_status = {
        pair["stat"]["question__title_slug"]: True if pair["status"] == "ac" else False
        for pair in problems["stat_status_pairs"]
    }

    all_problems = list(slug_to_solved_status.keys())

    # Get the data of all tags for the problems
    tasks = [asyncio.create_task(get_tags(slug)) for slug in all_problems]
    results = await asyncio.gather(*tasks)
    problem_to_tags: Dict[str, List[str]] = {
        problem: tags for problem, tags in zip(all_problems, results)
    }

    # For each tag aggregate statistics of solved/left problems
    tag_info_map: Dict[str, TagInfo] = {}

    for slug, solved in list(slug_to_solved_status.items()):
        tags = problem_to_tags[slug]

        for tag in tags:
            tag_info_map.setdefault(tag, TagInfo())
            if solved:
                tag_info_map[tag].solved.add(slug)
            else:
                tag_info_map[tag].left.add(slug)

    # Start with "to solve" list equal to all problems possible
    to_solve_tasks: Set[str] = {slug for slug in slug_to_solved_status.keys()}
    to_solve = random.choice(list(to_solve_tasks))
    tag_progress: Dict[str, float] = {}

    # Go over the tags in the descending solved/total ratio order
    # (so the tags with the lowest ratio go first)
    for name, tag_info in sorted(
        tag_info_map.items(),
        key=lambda x: len(x[1].solved) / (len(x[1].solved) + len(x[1].left)),
    ):
        # Each step filter out the problems that are not related to this tag
        to_solve_tasks &= tag_info.left

        # And pick a new problem out of this reduced set (if there are any left)
        to_solve = random.choice(list(to_solve_tasks)) if to_solve_tasks else to_solve

        # Calculate the solved/total ratio
        solved_ratio = (
            len(tag_info.solved) / (len(tag_info.solved) + len(tag_info.left)) * 100
        )

        tag_progress[name] = solved_ratio

    # Resolve the human name for the problem we picked
    to_solve_name = memcache.Client().get(f"problem_{to_solve}_title") or to_solve

    # Resolve the human name for each tag
    tag_to_name = {
        slug: (memcache.Client().get(f"tag_{slug}_name") or slug)
        for slug, _ in tag_progress.items()
    }

    return (
        render_template(
            "interpret.html",
            to_solve=to_solve,
            to_solve_name=to_solve_name,
            tag_progress=tag_progress,
            tag_to_name=tag_to_name,
            form=form,
        ),
        200,
    )


@app.route("/", methods=["GET", "POST"])
async def main():
    form = LeetcodeProblemsForms(request.form)

    if request.method == "POST" and form.validate():
        # Form has been submitted and valid
        problems = json.loads(request.form["problems"])
        return await interpret(problems, form)
    else:
        # Nothing submitted return empty form
        return render_template("main.html", form=form)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
