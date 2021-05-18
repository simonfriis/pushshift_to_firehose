# Keep some local logs for debugging and making it easier to restart the scraper when it times out

import os
import json
from datetime import datetime
from collections import deque

class MetadataLog:
    def __init__(self, cloudwatch_logger, filename:str, filter_subreddits:list=None) -> None:
        self.cloudwatch = cloudwatch_logger
        self.filename = f'{filename}'

        self.filtered_log_last_line = None
        self.max_created_utc = None
        self.min_created_utc = None

        # If file exists, get timestamps
        if self._log_exists():
            self.cloudwatch.log(f"Local log file found")

            # Get last line mentioning subreddits from log. Return None if subreddits not in log
            self.filtered_log_last_line = self._filtered_log_last_line(filter_subreddits)

            # If subreddits found in log
            if self.filtered_log_last_line is not None:

                # Set timestamps
                timestamps = self.filtered_log_last_line.get('last_result_timestamps')
                self.max_created_utc = timestamps.get('max_created_utc')
                self.min_created_utc = timestamps.get('min_created_utc')
                
                self.cloudwatch.log(f"Found subreddits {filter_subreddits} in local log. min_created_utc={self.min_created_utc}; max_created_utc={self.max_created_utc}\nLast filtered line from local log file is {self.filtered_log_last_line}.")
            else:
                self.filtered_log_last_line = None
        else:
            print("Local log file does not exist")

    def _log_exists(self):
        return os.path.exists(self.filename)

    def _read_log(self):
        with open(self.filename, 'r') as f:
            for line in f:
                yield json.loads(line)

    def _filter_log(self, log, subreddits):
        for line in log:
            if line['subreddit'] == subreddits:
                yield line

    def _filtered_log_last_line(self, filter_subreddits:list) -> dict:
        """Get last line of filtered log"""
        filtered_log = self._read_log()
        filtered_log = self._filter_log(filtered_log, filter_subreddits)
        filtered_log_last_line = deque(filtered_log, maxlen=1)

        if filtered_log_last_line:
            return filtered_log_last_line.pop()
        else:
            return None
    
    def _write_to_log(self, json_string:dict):
        with open(self.filename, 'a+', encoding='utf-8') as outfile:
            outfile.write(json.dumps(json_string) + '\n')

        return True

    def add_result_metadata(self, result):
        """Write metadata from PushshiftResponse object to metadata log. This
        function adds the min and max created_at post dates from the result to
        the metadata. This is helpful when the scraper needs to restart so it
        knows where it left off.

        Args:
            result (PushshiftResponse): The PushshiftResponse object containing metadata to write.

        Returns:
            bool: True
        """
        metadata = result.metadata

        # Add min and max created_utc incase scraper needs to restart
        metadata['last_result_timestamps'] = {
            'max_created_utc': result.max_created_at,
            'min_created_utc': result.min_created_at
        }

        # Add current time for debugging purposes
        metadata['retrieved_from_pushshift'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        # Append metadata to file
        self._write_to_log(metadata)

        return True
 

    def _get_last_line(self):
        """Parse and return last line of log file

        Returns:
            dict: Last line of file in json format.
        """
        with open(self.filename, 'rb') as f:
            f.seek(-2, os.SEEK_END)

            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)

            last_line = f.readline().decode()

        return json.loads(last_line)
