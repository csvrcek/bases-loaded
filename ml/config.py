"""ML pipeline configuration. Feature schema derived from docs/features.md."""

TARGET_COL = "target_home_win"

# Columns that are metadata, not features
NON_FEATURE_COLS = ["game_id", "game_date", TARGET_COL]

# --- Feature lists (derived from docs/features.md) ---

# Starting Pitching features (per team)
_SP_NUMERIC = [
    "sp_sierra_std",
    "sp_fip_30d",
    "sp_k_minus_bb_14d",
    "sp_rest_days",
    "sp_last_pitch_count",
]

# Offensive features (per team)
_OFFENSE_NUMERIC = [
    "team_wrc_plus_14d_split",
    "team_woba_std_split",
    "team_k_pct_14d",
    "team_iso_14d",
]

# Bullpen features (per team)
_BULLPEN_NUMERIC = [
    "bullpen_pitch_count_3d",
    "bullpen_xfip_14d",
]

# Expand per-team features with home_/away_ prefixes
_PER_TEAM_NUMERIC = _SP_NUMERIC + _OFFENSE_NUMERIC + _BULLPEN_NUMERIC
NUMERIC_FEATURES = (
    [f"home_{f}" for f in _PER_TEAM_NUMERIC]
    + [f"away_{f}" for f in _PER_TEAM_NUMERIC]
)

# Binary features (per team)
BINARY_FEATURES = ["home_closer_unavailable", "away_closer_unavailable"]

# Categorical features that need one-hot encoding
CATEGORICAL_FEATURES = [
    "home_sp_handedness",   # L / R
    "away_sp_handedness",   # L / R
    "weather_wind_dir",     # In / Out / Cross L-R / etc.
]

# Game-level contextual features (no home/away prefix)
CONTEXTUAL_NUMERIC = [
    "park_factor_runs",
    "park_factor_hr",
    "weather_temp_f",
    "weather_wind_mph",
    "home_travel_fatigue",
    "away_travel_fatigue",
]

ALL_NUMERIC_FEATURES = NUMERIC_FEATURES + CONTEXTUAL_NUMERIC + BINARY_FEATURES

# --- XGBoost hyperparameters ---

XGBOOST_PARAMS = {
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "tree_method": "hist",
    "max_depth": 5,
    "learning_rate": 0.05,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 3,
    "seed": 42,
}

NUM_BOOST_ROUNDS = 500
EARLY_STOPPING_ROUNDS = 20

# --- Train/validation split ---

VALIDATION_SPLIT_RATIO = 0.2
