import boto3
import json

class Firehose:

    def __init__(self, cloudwatch_logger, delivery_stream:str) -> None:
        self.firehose = boto3.client('firehose')
        self.cloudwatch = cloudwatch_logger
        self.delivery_stream = delivery_stream
        
        # Track stats (not implemented)
        self.batches_sent = 0
        self.total_records_sent = 0

    def send_result(self, data:list):
        """Send Pushshift results to AWS firehose

        Args:
            data (list): A list of records

        Returns:
            bool: Returns True if successful
        """
        records = self._process_data(data)
        put_records = self._put_batch(records)

        return put_records


    def _process_data(self, data:list) -> str:
        """Process result data before sending to Firehose

        Args:
            data (list): PushshiftResponse data

        Returns:
            str: A UTF-8 encoded string
        """
        # Convert records to a string
        records = [json.dumps(record) for record in data]
        records = '\n'.join(records) + '\n'
        records = records.encode('utf-8')

        return records


    def _put_batch(self, records:str):
        """Send batch of records to firehose

        Args:
            records (str): Data blob to send

        Raises:
            e: Fail to send to Firehose

        Returns:
            bool: True if successful
        """
        try:
            self.firehose.put_record_batch(
                DeliveryStreamName=self.delivery_stream,
                Records=[{'Data': records}]
            )

            self.batches_sent += 1
        except Exception as e:
            self.cloudwatch.log(f'ERROR: Failed to send results to firehose {self.delivery_stream}. Exception {e}')
            raise e
        else:
            return True