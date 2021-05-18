import argparse
import configparser
import json

from utils.cloudwatch import CloudWatchLog
from utils.firehose import Firehose
from utils.pushshift_api import PushshiftAPI
from utils.local_logs import MetadataLog


if __name__ == "__main__":

    """Config file"""
    config = configparser.ConfigParser()
    config.read('pushshift_scraper/settings.cfg')

    LOCAL_LOG_FILENAME = config['local_log']['filename']
    CLOUDWATCH_LOG_GROUP = config['cloudwatch']['log_group']
    CLOUDWATCH_LOG_STREAM = config['cloudwatch']['log_stream']
    FIREHOSE_TEST = config['firehose']['test_destination']
    FIREHOSE_COMMENTS = config['firehose']['comments_destination']
    FIREHOSE_SUBMISSIONS = config['firehose']['submissions_destination']

    
    """Command line arguments"""
    parser = argparse.ArgumentParser(description='Request reddit posts from the pushshift API')
    parser.add_argument('post_type', choices=['submissions', 'comments'], help='Type of posts to return: submissions or comments')
    parser.add_argument('subreddits', nargs='+', help='Space separated list of subreddit(s) to crawl')
    parser.add_argument('--after', type=int, nargs=1, help='Return posts after this date. Takes a UNIX timestamp.')
    parser.add_argument('--before', type=int, nargs=1, help='Return posts before this date. Takes a UNIX timestamp.')
    parser.add_argument('--size', type=int, nargs=1, default=100, help='Number of posts to return.')
    parser.add_argument('--local_log', help='Local log file to write metadata to.')
    parser.add_argument('--cloudwatch_log_group', help='Cloudwatch log group to write to.')
    parser.add_argument('--cloudwatch_log_stream', help='Cloudwatch log stream to write to.')
    parser.add_argument('--firehose', help='Firehose delivery stream to send results to.')
    parser.add_argument('--no_resume', action='store_true', help='Do not use timestamps from metadata file and resume previous search.')
    parser.add_argument('--test', action='store_true', help='Do a test run.')

    parsed_args = parser.parse_args()
    args = vars(parsed_args) # Access args as dict

    """Configure parameters"""
    # Convert lists to int
    for key in ['before', 'after', 'size']:
        val = args[key]
        if val is not None and isinstance(val, list):
            args[key] = val[0]

    POST_TYPE = args['post_type']
    SUBREDDITS = args['subreddits']
    BEFORE = args['before']
    AFTER = args['after']
    SIZE = args['size']
    NO_RESUME = args['no_resume']
    TEST = args['test']

    # Override settings.cfg if arguments supplied via cmd line
    if args['local_log'] is not None:
        LOCAL_LOG_FILENAME = args['local_log']
    if args['cloudwatch_log_group'] is not None:
        CLOUDWATCH_LOG_GROUP = args['cloudwatch_log_group']
    if args['cloudwatch_log_stream'] is not None:
        CLOUDWATCH_LOG_STREAM = args['cloudwatch_log_stream']
    if args['firehose'] is not None:
        FIREHOSE = args['firehose']



    """Configure services"""
    # Configure AWS CloudWatch logging
    cloudwatch = CloudWatchLog(
        log_group=CLOUDWATCH_LOG_GROUP,
        log_stream=CLOUDWATCH_LOG_STREAM)

    # Configure AWS Firehose
    # Override settings.cfg if firehose delivery stream passed via cmd line
    if FIREHOSE is None:
        if TEST:
            FIREHOSE = FIREHOSE_TEST
        elif POST_TYPE == 'comments':
            FIREHOSE = FIREHOSE_COMMENTS
        elif POST_TYPE == 'submissions':
            FIREHOSE = FIREHOSE_SUBMISSIONS
        else:
            raise Exception('Firehose delivery stream not specified')
        
    firehose = Firehose(
        cloudwatch_logger=cloudwatch,
        delivery_stream=FIREHOSE)

    # Configure local metadata log
    # This is useful when the scraper needs to be restarted. This will let it
    # pick up from where it left off.
    metadata_log = MetadataLog(
        cloudwatch_logger=cloudwatch,
        filename=LOCAL_LOG_FILENAME,
        filter_subreddits=SUBREDDITS)

    # Configure Pushshift API
    api = PushshiftAPI(cloudwatch_logger=cloudwatch)


    """Configure parameters"""
    cloudwatch.log(f"""Using paramters:
        Post type: {POST_TYPE}
        Subreddits: {SUBREDDITS}
        Before: {BEFORE}
        After: {AFTER}
        Size: {SIZE}
        No resume: {NO_RESUME}
        Test: {TEST}
        Local log: {LOCAL_LOG_FILENAME}
        Cloudwatch log group:{CLOUDWATCH_LOG_GROUP}
        Cloudwatch log stream: {CLOUDWATCH_LOG_STREAM}
        Firehose delivery stream: {FIREHOSE}
    """)
    
    # Use previous result timestamp from metadata log if possible and --no_resume is False
    if metadata_log.filtered_log_last_line is not None and BEFORE is not None and metadata_log.min_created_utc < BEFORE:
        if NO_RESUME:
            cloudwatch.log(f"INFO: 'before' timestamp {metadata_log.min_created_utc} from metadata log less than CLI arg {BEFORE}, but --no_resume is set.")
        else:
            cloudwatch.log(f"Using 'before' timestamp {metadata_log.min_created_utc} from metadata log instead of CLI arg {BEFORE}")
            BEFORE = metadata_log.min_created_utc


    """Get results"""
    # Get first page of results
    result = api.get(post_type=POST_TYPE, subreddits=SUBREDDITS, before=BEFORE, after=AFTER, size=SIZE)
    firehose.send_result(result.data)
    metadata_log.add_result_metadata(result)

    # Continue getting results
    while result.metadata['total_results'] > 0:
        result = api.get_next(result)
        firehose.send_result(result.data)
        metadata_log.add_result_metadata(result)
        
        # Record progress at regular intervals
        api.log_progress()

        # If testing, retrieve up to two pages
        if TEST and api.request_count >= 10:
            cloudwatch.log("Stopping test.")
            break

    # Log result of scrape
    cloudwatch.log(f"Finished crawl. Result: {api.progress()} Last result metadata:\n{json.dumps(result.metadata, indent=4)}")