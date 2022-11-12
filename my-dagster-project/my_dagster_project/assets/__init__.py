from github import InputFileContent
import nbformat
import jupytext
import pickle
from nbconvert.preprocessors import ExecutePreprocessor
import pandas as pd
from datetime import datetime, timedelta
from dagster import asset
from github import Github
import concurrent.futures
from tqdm import tqdm
import duckdb
import os


@asset(required_resource_keys={"github_api"})
def github_stargazers(context):
    last_year = datetime.now() - timedelta(weeks=57)
    return [
        stargazer
        for stargazer in context.resources.github_api.get_repo(
            os.getenv("GITHUB_REPO")
        ).get_stargazers_with_dates()
        if stargazer.starred_at >= last_year
    ]


@asset(required_resource_keys={"github_api"})
def github_stargazer_users(context, github_stargazers):
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                lambda gh, login: gh.get_user(login),
                context.resources.github_api,
                stargazer.user.login,
            )
            for stargazer in github_stargazers
        ]
        users = []
        created_ats = []
        for future in tqdm(
            concurrent.futures.as_completed(futures), total=len(github_stargazers)
        ):
            user = future.result()
            users.append(user.login)
            created_ats.append(user.created_at)
        return pd.DataFrame({"user": users, "created_at": created_ats})


@asset
def github_stargazers_by_week(github_stargazers, github_stargazer_users):
    stargazers = pd.DataFrame(
        [
            {
                "login": stargazer.user.login,
                "starred_at": stargazer.starred_at,
            }
            for stargazer in github_stargazers
        ]
    )
    users = github_stargazer_users

    is_fake = """ date_diff('hour', created_at, starred_at) < 48 """
    return duckdb.query(
        f"""

with joined as (
    select stargazers.login, stargazers.starred_at, users.created_at
    from stargazers
    inner join users on users.user = stargazers.login
)
select
    date_trunc('week', starred_at) as week,
    sum(if({is_fake}, 1, 0)) as fake_users,
    sum(if({is_fake}, 0, 1)) as real_users,
    count(*) as total_users
from joined
group by 1
order by 1 asc

    """
    ).df()


@asset
def github_stars_notebook(github_stargazers_by_week):
    markdown = f"""
# Github Stars

```python
import pickle
github_stargazers_by_week = pickle.loads({pickle.dumps(github_stargazers_by_week)!r})
```

## Github Stars by Week, last 52 weeks
```python
github_stargazers_by_week.tail(52).reset_index().plot.bar(x="week", y="total_users")
```

## FAKE Github Stars by Week, last 52 weeks
```python
github_stargazers_by_week.tail(52).reset_index().plot.bar(x="week", y="fake_users")
```
    """
    nb = jupytext.reads(markdown, "md")
    ExecutePreprocessor().preprocess(nb)
    return nbformat.writes(nb)


@asset(required_resource_keys={"github_api"})
def github_stars_notebook_gist(context, github_stars_notebook):
    gist = context.resources.github_api.get_gist(os.getenv("GITHUB_GIST_ID"))
    gist.edit(
        files={
            "notebook.ipynb": InputFileContent(github_stars_notebook),
        },
    )
    context.log.info(f"Notebook created at {gist.html_url}")
    return gist.html_url
