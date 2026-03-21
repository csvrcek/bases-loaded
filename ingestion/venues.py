"""MLB venue metadata: coordinates, roof type, and timezone.

Used by the weather scraper to determine whether to fetch outdoor weather
and to provide coordinates for the OpenWeather API.
"""

# Roof types: "open", "retractable", "dome"
VENUES = {
    "Chase Field": {"lat": 33.4455, "lon": -112.0667, "roof": "retractable", "tz": "America/Phoenix"},
    "Truist Park": {"lat": 33.8907, "lon": -84.4677, "roof": "open", "tz": "America/New_York"},
    "Oriole Park at Camden Yards": {"lat": 39.2838, "lon": -76.6216, "roof": "open", "tz": "America/New_York"},
    "Fenway Park": {"lat": 42.3467, "lon": -71.0972, "roof": "open", "tz": "America/New_York"},
    "Wrigley Field": {"lat": 41.9484, "lon": -87.6553, "roof": "open", "tz": "America/Chicago"},
    "Guaranteed Rate Field": {"lat": 41.8299, "lon": -87.6338, "roof": "open", "tz": "America/Chicago"},
    "Great American Ball Park": {"lat": 39.0975, "lon": -84.5069, "roof": "open", "tz": "America/New_York"},
    "Progressive Field": {"lat": 41.4962, "lon": -81.6852, "roof": "open", "tz": "America/New_York"},
    "Coors Field": {"lat": 39.7561, "lon": -104.9942, "roof": "open", "tz": "America/Denver"},
    "Comerica Park": {"lat": 42.3390, "lon": -83.0485, "roof": "open", "tz": "America/New_York"},
    "Minute Maid Park": {"lat": 29.7572, "lon": -95.3555, "roof": "retractable", "tz": "America/Chicago"},
    "Kauffman Stadium": {"lat": 39.0517, "lon": -94.4803, "roof": "open", "tz": "America/Chicago"},
    "Angel Stadium": {"lat": 33.8003, "lon": -117.8827, "roof": "open", "tz": "America/Los_Angeles"},
    "Dodger Stadium": {"lat": 34.0739, "lon": -118.2400, "roof": "open", "tz": "America/Los_Angeles"},
    "loanDepot park": {"lat": 25.7781, "lon": -80.2196, "roof": "retractable", "tz": "America/New_York"},
    "American Family Field": {"lat": 43.0280, "lon": -87.9712, "roof": "retractable", "tz": "America/Chicago"},
    "Target Field": {"lat": 44.9817, "lon": -93.2776, "roof": "open", "tz": "America/Chicago"},
    "Citi Field": {"lat": 40.7571, "lon": -73.8458, "roof": "open", "tz": "America/New_York"},
    "Yankee Stadium": {"lat": 40.8296, "lon": -73.9262, "roof": "open", "tz": "America/New_York"},
    "Oakland Coliseum": {"lat": 37.7516, "lon": -122.2005, "roof": "open", "tz": "America/Los_Angeles"},
    "Citizens Bank Park": {"lat": 39.9061, "lon": -75.1665, "roof": "open", "tz": "America/New_York"},
    "PNC Park": {"lat": 40.4469, "lon": -80.0057, "roof": "open", "tz": "America/New_York"},
    "Petco Park": {"lat": 32.7076, "lon": -117.1570, "roof": "open", "tz": "America/Los_Angeles"},
    "Oracle Park": {"lat": 37.7786, "lon": -122.3893, "roof": "open", "tz": "America/Los_Angeles"},
    "T-Mobile Park": {"lat": 47.5914, "lon": -122.3325, "roof": "retractable", "tz": "America/Los_Angeles"},
    "Busch Stadium": {"lat": 38.6226, "lon": -90.1928, "roof": "open", "tz": "America/Chicago"},
    "Tropicana Field": {"lat": 27.7682, "lon": -82.6534, "roof": "dome", "tz": "America/New_York"},
    "Globe Life Field": {"lat": 32.7512, "lon": -97.0832, "roof": "retractable", "tz": "America/Chicago"},
    "Rogers Centre": {"lat": 43.6414, "lon": -79.3894, "roof": "retractable", "tz": "America/Toronto"},
    "Nationals Park": {"lat": 38.8730, "lon": -77.0074, "roof": "open", "tz": "America/New_York"},
}
