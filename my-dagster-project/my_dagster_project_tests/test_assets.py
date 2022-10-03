
from dagster import materialize_to_memory
from unittest.mock import MagicMock
from my_dagster_project.assets import (
    github_stars_notebook_gist,
    github_stars_notebook,
    github_stargazers_by_week,
    github_stargazers,
)
from datetime import date, datetime
import pandas as pd


def test_smoke():
    mock_stargazers = [
        ("user1", datetime(2021, 1, 1)),
        ("user2", datetime(2021, 1, 1)),
        ("user3", datetime(2021, 2, 1)),
    ]

    github_api = MagicMock()
    github_api.get_repo("dagster-io/dagster").get_stargazers_with_dates.return_value = [
        MagicMock(
            user=MagicMock(login=login),
            starred_at=starred_at,
        )
        for (login, starred_at) in mock_stargazers
    ]

    github_api.get_user().create_gist.return_value = MagicMock(
        html_url="https://gist.github.com/test_id"
    )

    result = materialize_to_memory(
        [
            github_stars_notebook_gist,
            github_stars_notebook,
            github_stargazers_by_week,
            github_stargazers,
        ],
        resources={"github_api": github_api},
    )

    assert result.success
    assert result.output_for_node("github_stargazers_by_week").reset_index().to_dict("records") == [
        {"users": 2, "week": date(2021, 1, 3)},
        {"users": 1, "week": date(2021, 2, 7)},
    ]
    assert result.output_for_node(
        "github_stars_notebook_gist") == "https://gist.github.com/test_id"
    assert "# Github Stars" in result.output_for_node("github_stars_notebook")
    assert github_api.get_user().create_gist.call_args[1]["public"] is False
