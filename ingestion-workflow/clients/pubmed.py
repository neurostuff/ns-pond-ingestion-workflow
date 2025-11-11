"""
General settings for PubMed client.
(appending email to queries, rate limits, etc.)
"""


class PubMedClient:
    def __init__(self, email: str):
        self.email = email
        # Initialize other settings like rate limits here
