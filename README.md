# Introduction

This script retrieves data from the [Pushshift.io API](https://pushshift.io/api-parameters/) and send it to an AWS firehose. Logs are sent to Cloudwatch to make it easier to run this from EC2.

**Warning:** I wrote this script for my own personal use. There will undoubtedly be bugs and unexpected behavior. I am making it public in the event that others find find it useful. Proceed with appropriate caution.

# Configuration
Set default arguments in `pushshift_scraper/settings.cfg`. Default settings can be overridden by providing them through the command line.

# Quickstart

Run `pushshift_scraper/pushshift_scraper.py` from the command line.

To get all submissions to r/wallstreetbets:

```shell
python3 pushshift_scraper/pushshift_scraper.py submissions wallstreetbets
```

To get all comments to r/wallstreetbets:

```shell
python3 pushshift_scraper/pushshift_scraper.py comments wallstreetbets
```

To get all submissions to multiple subreddits:

```shell
python3 pushshift_scraper/pushshift_scraper.py comments wallstreetbets dogecoin superstonk
```

Several arguments can be specified, including min and max dates. To see more, type:

```shell
python3 pushshift_scraper/pushshift_scraper.py --help
```
