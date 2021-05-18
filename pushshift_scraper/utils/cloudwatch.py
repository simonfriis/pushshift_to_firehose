import boto3
import time

class CloudWatchLog:

    def __init__(self, log_group:str, log_stream:str):
        self.client = boto3.client('logs')
        self.SEQUENCE_TOKEN = 'none'
        self.LOG_GROUP = log_group
        self.LOG_STREAM = log_stream
       
        
        self.create_log_stream()


    def create_log_stream(self):
        try:
            response = self.client.create_log_stream(
                logGroupName=self.LOG_GROUP,
                logStreamName=self.LOG_STREAM
            )
        except self.client.exceptions.ResourceAlreadyExistsException as e:
            # Keep going if already exists, adding event should update sequence token
            message = f'Log stream {self.LOG_STREAM} already exists.'
            self.log(message)
        else:
            message = f'Created log stream {self.LOG_STREAM}.'

            # Don't use sequence token if log stream is new
            self.log(message, use_sequence_token=False)

    

    def log(self, message, use_sequence_token=True):
        
        # Output message to console
        print(f"CloudWatch: {message}")

        try:
            response = self._put_event(message, use_sequence_token=use_sequence_token)
        except self.client.exceptions.InvalidSequenceTokenException as e:
            # Use sequence token if returned. If expected sequence token is null, don't send a sequence token.
            if 'expectedSequenceToken' in e.response:
                # Update sequence token and retry
                self.SEQUENCE_TOKEN = e.response['expectedSequenceToken']
                response = self._put_event(message)
            elif e.response['Error']['Message'] == 'The given sequenceToken is invalid. The next expected sequenceToken is: null':
                response = self._put_event(message, use_sequence_token=False)
        else:
            self.SEQUENCE_TOKEN = response['nextSequenceToken']
            return response


    def _put_event(self, message, use_sequence_token=True):
        """Omit sequence token when initializing log stream with first message"""
        if use_sequence_token:
            response = self.client.put_log_events(
                logGroupName=self.LOG_GROUP,
                logStreamName=self.LOG_STREAM,
                logEvents=[
                    {
                        'timestamp': int(time.time() * 1000),
                        'message': message
                    }
                ],
                sequenceToken = self.SEQUENCE_TOKEN
            )
        else:
            response = self.client.put_log_events(
                logGroupName=self.LOG_GROUP,
                logStreamName=self.LOG_STREAM,
                logEvents=[
                    {
                        'timestamp': int(time.time() * 1000),
                        'message': message
                    }
                ]
            )
        return response
