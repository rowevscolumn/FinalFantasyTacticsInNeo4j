import pandas as pd
import numpy as np
import sys
from pathlib import Path
from graphdatascience import GraphDataScience

class Neo4JConnection:
    """
    This class will connect to the database automatically, then run the cypher commands

    Attributes:
        host (str): URL of the database location
        username (str): Username to log into database
        password (str): Password to log into database
        gds (graphdatascience): the connected package to connect and run the commands

    Args:
        host (str="bolt://localhost:7687"): URL of the database location
        username (str="neo4j"): Username to log into database
        password (str="password"): Password to log into database

    """
    def __init__(self, host: str="bolt://localhost:7687", username:str="neo4j", password:str = "password") -> None:
        self.host:str = host
        self.username:str = username
        self.password:str = password

        self.gds = self.__NeoConnect(self.host, self.username, self.password)
        pass

    def __NeoConnect(self, host: str, username:str, password:str) -> GraphDataScience:
        """
        Package to connect to Neo4J database

        Args:
            host (str): URL of the database location
            username (str): Username to log into database
            password (str): Password to log into database

        Returns:
            GraphDataScience

        """
        return GraphDataScience(host, auth=(username, password))

    def __str__(self) -> str:
        """
        Returns:
            str: Version number. Helps confirm that it's connected.

        """
        return self.gds.version()

    def runCypher(self, cypherStatement:str) -> pd.DataFrame:
        """
        Runs Cypher commands from the database

        Args:
            cypherStatement (str): CQL statement to run to the database

        Returns:
            pd.DataFrame: Outputs to a dataframe

        """
        return self.gds.run_cypher(cypherStatement)

    def clearDatabase(self) -> bool:
        """
        Empties the database., all nodes, relationships and constraints
        """
        self.runCypher("CALL apoc.schema.assert({}, {})")
        self.runCypher("MATCH (n) DETACH DELETE n;")
        return True

    def printStatistics(self) -> pd.DataFrame:
        """
        Prints the mets statistics of the database

        Returns:
            pd.DataFrame
        """
        return self.runCypher("CALL apoc.meta.stats()")
