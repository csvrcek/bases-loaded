"""Contextual and environmental feature computation.

Computes game-level features:
- park_factor_runs: Stadium run multiplier (from static park factor data)
- park_factor_hr: Stadium HR multiplier (from static park factor data)
- weather_temp_f: Forecasted temperature (0 for domes)
- weather_wind_mph: Forecasted wind speed
- weather_wind_dir: Wind direction relative to home plate
- home_travel_fatigue / away_travel_fatigue: Timezone transitions in last 7 days
"""

import polars as pl


def compute_contextual_features(
    game_logs: pl.DataFrame,
    weather: pl.DataFrame,
    park_factors: pl.DataFrame,
    schedules: pl.DataFrame,
) -> pl.DataFrame:
    """Compute contextual features for all games.

    Returns DataFrame with columns:
        game_id, park_factor_runs, park_factor_hr, weather_temp_f,
        weather_wind_mph, weather_wind_dir, home_travel_fatigue, away_travel_fatigue
    """
    games = game_logs.with_columns(
        pl.col("game_date").str.to_date("%Y-%m-%d").alias("date")
    )

    result = games.select("game_id", "date", "venue_name", "home_team", "away_team")

    # --- Park factors (static lookup by venue) ---
    result = _join_park_factors(result, park_factors)

    # --- Weather (per-game lookup) ---
    result = _join_weather(result, weather)

    # --- Travel fatigue (timezone transitions in last 7 days) ---
    result = _join_travel_fatigue(result, schedules)

    return result.select(
        "game_id",
        "park_factor_runs",
        "park_factor_hr",
        "weather_temp_f",
        "weather_wind_mph",
        "weather_wind_dir",
        "home_travel_fatigue",
        "away_travel_fatigue",
    )


def _join_park_factors(
    result: pl.DataFrame, park_factors: pl.DataFrame
) -> pl.DataFrame:
    if len(park_factors) > 0:
        return result.join(
            park_factors.select("venue_name", "park_factor_runs", "park_factor_hr"),
            on="venue_name",
            how="left",
        ).with_columns(
            pl.col("park_factor_runs").fill_null(1.0),
            pl.col("park_factor_hr").fill_null(1.0),
        )
    return result.with_columns(
        pl.lit(1.0).alias("park_factor_runs"),
        pl.lit(1.0).alias("park_factor_hr"),
    )


def _join_weather(result: pl.DataFrame, weather: pl.DataFrame) -> pl.DataFrame:
    if len(weather) > 0:
        return result.join(
            weather.select(
                "game_id",
                pl.col("temp_f").alias("weather_temp_f"),
                pl.col("wind_mph").alias("weather_wind_mph"),
                pl.col("wind_dir").alias("weather_wind_dir"),
            ),
            on="game_id",
            how="left",
        ).with_columns(
            pl.col("weather_temp_f").fill_null(72),
            pl.col("weather_wind_mph").fill_null(0),
            pl.col("weather_wind_dir").fill_null("None"),
        )
    return result.with_columns(
        pl.lit(72).alias("weather_temp_f"),
        pl.lit(0).alias("weather_wind_mph"),
        pl.lit("None").alias("weather_wind_dir"),
    )


def _join_travel_fatigue(
    result: pl.DataFrame, schedules: pl.DataFrame
) -> pl.DataFrame:
    if len(schedules) == 0:
        return result.with_columns(
            pl.lit(0).alias("home_travel_fatigue"),
            pl.lit(0).alias("away_travel_fatigue"),
        )

    sched = (
        schedules.with_columns(
            pl.col("game_date").str.to_date("%Y-%m-%d").alias("date")
        )
        .sort("team", "date")
    )

    # Timezone change = 1 when current game's timezone differs from previous game
    sched_with_tz = sched.with_columns(
        (pl.col("venue_tz") != pl.col("venue_tz").shift(1).over("team"))
        .fill_null(False)
        .cast(pl.Int32)
        .alias("tz_change")
    ).sort("date")

    # Rolling 7-day sum of timezone transitions per team
    tz_fatigue = (
        sched_with_tz.rolling(
            index_column="date", period="7d", by="team", closed="left"
        )
        .agg(pl.col("tz_change").sum().alias("travel_fatigue"))
    )

    # Join for home team
    result = result.join(
        tz_fatigue.select(
            pl.col("team"), pl.col("date"),
            pl.col("travel_fatigue").alias("home_travel_fatigue"),
        ),
        left_on=["home_team", "date"],
        right_on=["team", "date"],
        how="left",
    )

    # Join for away team
    result = result.join(
        tz_fatigue.select(
            pl.col("team"), pl.col("date"),
            pl.col("travel_fatigue").alias("away_travel_fatigue"),
        ),
        left_on=["away_team", "date"],
        right_on=["team", "date"],
        how="left",
    )

    return result.with_columns(
        pl.col("home_travel_fatigue").fill_null(0),
        pl.col("away_travel_fatigue").fill_null(0),
    )
