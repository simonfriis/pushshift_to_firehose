import requests
from urllib.parse import urlencode
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from datetime import datetime
import json
from ratelimiter import RateLimiter



class PushshiftAPI:
    def __init__(self, cloudwatch_logger) -> None:
        self.start_time = datetime.utcnow()
        self.request_count = 0
        self.last_request = None
        self.last_response = None
        self.cloudwatch = cloudwatch_logger
    
        """Confgure requests module"""
        # Define retry strategy for requests
        retry_strategy = Retry(
            total=5,
            status_forcelist=[408, 429, 500, 502, 503, 504, 522],
            method_whitelist=["GET"],
            backoff_factor=4,
            respect_retry_after_header=False
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.request = requests.Session()
        self.request.mount("https://", adapter)
        self.request.mount("http://", adapter)


    def _create_params(self, subreddits:list, before:int, after:int, size:int):

            params = {
                "subreddit": ','.join(subreddits),
                "size": size,
                "sort": "desc",
                "sort_type": "created_utc",
                "metadata": "true"
            }

            if before is not None:
                params['before'] = before

            if after is not None:
                params['after'] = after

            return params

    def _set_endpoint(self, post_type:str) -> str:
        # Specify endpoint to search based on post type
        if post_type == "comments":
            return "comment"
        elif post_type == "submissions":
            return "submission"
        else:
            raise Exception("Could not specify Pushshift endpoint")

    def _create_url(self, endpoint:str, params:dict) -> str:
        url = f"https://api.pushshift.io/reddit/{endpoint}/search"
        encoded_url = url + '?' + urlencode(params)

        return encoded_url


    @RateLimiter(max_calls=100, period=60)
    def _get(self, endpoint:str, params:dict):
        url = self._create_url(endpoint, params)
        response = self.request.get(url)
        
        self.request_count += 1

        return PushshiftResponse(response, endpoint, params, cloudwatch_logger=self.cloudwatch)


    def get(self, post_type:str, subreddits:list, before:int=None, after:int=None, size:int=100):
        """Retrieve comments from pushshift.

        Args:
            post_type (str): Submissions or comments.
            subreddits (list): List of subreddits to search.
            before (int, optional): Max created_utc of posts to get (epoch timestamp). Defaults to None.
            after (int, optional): Max created_utc of posts to get (epoch timestamp). Defaults to None.
            size (int, optional): Number of results to return. Max is 100. Defaults to 100.

        Returns:
            PushshiftResponse: Returns a PushshiftResponse object.
        """
        endpoint = self._set_endpoint(post_type)
        params = self._create_params(subreddits=subreddits, before=before, after=after, size=size)
        response = self._get(endpoint, params)

        return response


    def get_next(self, response):
        # Check if response is empty or no more results

        endpoint = response.endpoint
        params = response.request_params

        # Set max_created_utc to earliest result
        params['before'] = response.min_created_at
        response = self._get(endpoint=endpoint, params=params)

        return response


    def progress(self):
        # Return crawler status
        current_time = datetime.utcnow()
        elapsed_time = current_time - self.start_time
        return f"Crawled {self.request_count} pages in {round(elapsed_time.seconds / 60.0, 2)} mins."        


    def log_progress(self, interval:int=100):
        # Record progress every N pages
        if self.request_count % interval == 0:
            self.cloudwatch.log(self.progress())


class PushshiftResponse:
    def __init__(self, response, endpoint, request_params, cloudwatch_logger) -> None:
        self.cloudwatch = cloudwatch_logger

        self._validate_response(response)

        self.response = response
        self.endpoint = endpoint
        self.request_params = request_params
        
        self.json = response.json()
        self.data = self.json.get('data')
        self.metadata = self.json.get('metadata')

        self._validate_metadata(self.metadata)
        self._validate_data(self.data)

        self.min_created_at = self._min_created_at(self.data)
        self.max_created_at = self._max_created_at(self.data)

    def _validate_response(self, response):
        # This should only trigger if all retries failed
        if not response.ok:
            message = f'ERROR: Response not ok. Server returned status code {response.status_code}. Query params: {json.dumps(self.request_params)}.'
            self.cloudwatch.log(message)
            raise Exception(message)
        else:
            return True

    def _validate_metadata(self, metadata):
        # Check if metadata exists
        if metadata is None:
            message = f"No metadata returned for query {json.dumps(self.request_params)}"
            self.cloudwatch.log(message)
            raise Exception(message)
            
        # Check if request timed out
        if metadata["timed_out"]:
            self.cloudwatch.log(f"WARNING: Search timed out. Metadata:\n{json.dumps(metadata, indent=4)}")

        # Check number of results returned
        if metadata['total_results']==0:
            message = f"No data returned for query {json.dumps(self.request_params)}. Metadata:\n{json.dumps(metadata, indent=4)}"
            self.cloudwatch.log(message)
            raise Exception(message)

        # Check shards
        shards = metadata["shards"]
        if shards["failed"] > 0:
            self.cloudwatch.log(f'WARNING: Failed shards: {shards["failed"]}. Metadata:\n{json.dumps(metadata, indent=4)}')
        if shards["skipped"] > 0:
            self.cloudwatch.log(f'WARNING: Skipped shards: {shards["skipped"]}. Metadata:\n{json.dumps(metadata, indent=4)}')

        return True


    def _validate_data(self, data):
        if data is None:
            message = f"No data returned for query {json.dumps(self.request_params)}. Metadata:\n{json.dumps(self.metadata, indent=4)}"
            self.cloudwatch.log(message)
            raise Exception(message)

        return True


    def _max_created_at(self, data):
        return data[0].get('created_utc')
    
    def _min_created_at(self, data):
        return data[-1].get('created_utc')