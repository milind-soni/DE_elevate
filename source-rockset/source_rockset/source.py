#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from rockset import RocksetClient
import logging
import movie_list


"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""
rs = RocksetClient(api_key='VuVQACkPPwNKHhPleE48PJHSgUuuJ5hNhfyIw9JSB5S3nrSiyrnqpfH9JatopHJE',
                   host='https://api.use1a1.rockset.com')
logger = logging.getLogger("airbyte")


# Basic full refresh stream
class RocksetStream(HttpStream):
    """
    TODO remove this comment

    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

    Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
    contains the endpoints
        - GET v1/customers
        - GET v1/employees

    then you should have three classes:
    `class RocksetStream(HttpStream, ABC)` which is the current class
    `class Customers(RocksetStream)` contains behavior to pull data for customers using v1/customers
    `class Employees(RocksetStream)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalRocksetStream((RocksetStream), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """

    # TODO: Fill in the url base. Required.
    url_base = "https://api.use1a1.rockset.com"

    primary_key = None

    def __init__(self, movie_id: int, **kwargs):
        super().__init__(**kwargs)
        # Here's where we set the variable from our input to pass it down to the source.
        self.movie_id = movie_id

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        # The api requires that we include the Pokemon name as a query param so we do that in this method.
        return {"movie_id": self.movie_id}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:

        params = list()

        response = rs.QueryLambdas.execute_query_lambda(
            query_lambda='movieapi',
            version='2319ffe78416d6c2',
            workspace='commons',
            parameters=params
        )

        yield {response.results}


# Source
class SourceRockset(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        logger.info("Checking Movie API connection...")
        input_movie = config["movie_id"]
        if input_movie not in movie_list.MOVIE_LIST:
            result = f"Input Pokemon {input_movie} is invalid. Please check your spelling and input a valid Pokemon."
            logger.info(f"PokeAPI connection failed: {result}")
            return False, result
        else:
            logger.info(
                f"PokeAPI connection success: {input_movie} is a valid Pokemon")
            return True, None
        # try:
        #     rs = RocksetClient(api_key='VuVQACkPPwNKHhPleE48PJHSgUuuJ5hNhfyIw9JSB5S3nrSiyrnqpfH9JatopHJE',host='https://api.use1a1.rockset.com')
        #     return True, None
        # except Exception as e:
        #     return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:

        return [RocksetStream(movie_id=config["movie_id"])]
