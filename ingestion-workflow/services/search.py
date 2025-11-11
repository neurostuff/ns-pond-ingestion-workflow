"""
This module will search for articles
based on a given query/set of parameters.
"""


class ArticleSearchService:
    def __init__(self, query: str):
        self.query = query

    def search(self) -> list:
        # Implement search logic here
        pass


class PubMedSearchService(ArticleSearchService):
    def search(self) -> list:
        """Search for articles using PubMed."""
        raise NotImplementedError("PubMedSearchService search method not implemented.")
