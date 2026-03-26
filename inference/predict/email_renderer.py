"""Render prediction email using Jinja2."""

from pathlib import Path

from jinja2 import Environment, FileSystemLoader


TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "templates"


def render_prediction_email(date: str, games: list[dict]) -> str:
    """Render the prediction email HTML.

    Args:
        date: Game date (YYYY-MM-DD).
        games: List of matchup dicts. Each dict has fixed metadata and a
            flexible ``predictions`` list so new prediction types can be
            added without touching the renderer.

            Structure::

                {
                    "home_team": "New York Yankees",
                    "away_team": "Boston Red Sox",
                    "game_time": "7:05 PM ET",
                    "venue_name": "Yankee Stadium",
                    "home_pitcher": "Gerrit Cole",
                    "away_pitcher": "Brayan Bello",
                    "predictions": [
                        {
                            "label": "Win Probability",
                            "home_value": "62%",
                            "away_value": "38%",
                            "home_pct": 62,   # 0-100, used for bar width
                            "away_pct": 38,
                        },
                        # future: strikeouts, first HR, etc.
                    ],
                }

    Returns:
        Rendered HTML string.
    """
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=True,
    )
    template = env.get_template("prediction_email.html")
    return template.render(date=date, games=games)
