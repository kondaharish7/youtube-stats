
import googleapiclient.discovery
from yt_stats_pwds import *

def get_yt_client():
    api_service_name = "youtube"
    api_version = "v3"
    yt_client = googleapiclient.discovery.build(api_service_name, api_version, developerKey = ysa_DEVELOPER_KEY)
    return yt_client

