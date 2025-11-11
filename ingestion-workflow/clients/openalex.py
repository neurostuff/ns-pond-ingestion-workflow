"""
General settings for OpenAlex client.
(appending email to queries, rate limits, etc.)
"""


class OpenAlexClient:
    def __init__(self, email: str):
        self.email = email
        # Initialize other settings like rate limits here
