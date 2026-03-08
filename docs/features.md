# Bases Loaded: ML Feature Directory

**Target Variable:** `target_home_win` (Integer: 1 if Home Team wins, 0 if Away Team wins)
**Granularity:** One row per game.

## 1. Starting Pitching (SP) Features
These features evaluate the starting pitcher's true talent and current form.

*Note: Prefix with `home_` or `away_`.*

| Feature Name | Data Type | Description |
| :--- | :--- | :--- |
| `sp_sierra_std` | Float | Season-to-Date Skill-Interactive ERA. Measures true pitching skill. |
| `sp_fip_30d` | Float | Rolling 30-day Fielding Independent Pitching. Captures recent form. |
| `sp_k_minus_bb_14d` | Float | Rolling 14-day Strikeout % minus Walk %. Indicates immediate command. |
| `sp_handedness` | Categorical | 'L' (Left) or 'R' (Right). Used to determine offensive splits. |
| `sp_rest_days` | Integer | Number of days since the pitcher's last start. |
| `sp_last_pitch_count` | Integer | Number of pitches thrown in the pitcher's most recent outing. |

## 2. Offensive Lineup Features
These features aggregate the team's offensive capability, specifically measured against the opposing starting pitcher's handedness.

*Note: Prefix with `home_` or `away_`.*

| Feature Name | Data Type | Description |
| :--- | :--- | :--- |
| `team_wrc_plus_14d_split` | Float | Rolling 14-day wRC+ vs opposing SP's handedness (e.g., vs LHP or RHP). |
| `team_woba_std_split` | Float | Season-to-Date wOBA vs SP's handedness. |
| `team_k_pct_14d` | Float | Rolling 14-day team Strikeout %. |
| `team_iso_14d` | Float | Rolling 14-day team Isolated Power (Extra base hitting ability). |

## 3. Bullpen & Relief Features
These features measure the fatigue and quality of the relief pitching staff available for today's game.

*Note: Prefix with `home_` or `away_`.*

| Feature Name | Data Type | Description |
| :--- | :--- | :--- |
| `bullpen_pitch_count_3d` | Integer | Total pitches thrown by all team relievers over the last 3 calendar days. |
| `bullpen_xfip_14d` | Float | Rolling 14-day Expected Fielding Independent Pitching for the bullpen. |
| `closer_unavailable` | Binary | 1 if the primary closer pitched the last 2 consecutive days, otherwise 0. |

## 4. Contextual & Environmental Features
Game-level variables that impact run-scoring environments and team fatigue.

| Feature Name | Data Type | Description |
| :--- | :--- | :--- |
| `park_factor_runs` | Float | Multiplier for the stadium's historical impact on total runs scored. |
| `park_factor_hr` | Float | Multiplier for the stadium's historical impact on home runs. |
| `weather_temp_f` | Integer | Forecasted temperature in Fahrenheit at first pitch (0 for domes). |
| `weather_wind_mph` | Integer | Forecasted wind speed at first pitch. |
| `weather_wind_dir` | Categorical | Direction of wind relative to home plate (e.g., 'In', 'Out', 'Cross L-R'). |
| `home_travel_fatigue` | Integer | Timezone shifts + days without rest for the home team. |
| `away_travel_fatigue` | Integer | Timezone shifts + days without rest for the away team. |