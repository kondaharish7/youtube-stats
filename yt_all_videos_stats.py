"""
Extract videos from a youtube channel
"""
import json
import pandas as pd
import googleapiclient.discovery
from yt_stats_pwds import *

api_service_name = "youtube"
api_version = "v3"

yt_client = googleapiclient.discovery.build(api_service_name, api_version, developerKey = DEVELOPER_KEY)

# yt_channel_ids_list = [{'channelId': 'UCWv7vMbMWH4-V0ZXdmDpPBA', 'channelTitle': 'Programming with Mosh'},{'channelId': 'UC8butISFwT-Wl7EV0hUK0BQ', 'channelTitle': 'freeCodeCamp.org'}]
yt_channel_ids_list = [{'channelId': 'UC8butISFwT-Wl7EV0hUK0BQ', 'channelTitle': 'freeCodeCamp.org'}]
# Extract all the videos from each channel id
yt_topic_video_ids_list = []; next_pg_token1 = ""
for value in yt_channel_ids_list:
    print(value['channelId'])
    while True:
        yt_search_resp = yt_client.search().list(part='id,snippet', type='video', channelId=value['channelId'], pageToken=next_pg_token1, fields="prevPageToken,nextPageToken,items(id,snippet(title))", maxResults=50).execute()
        # print(yt_search_resp)

        for a in yt_search_resp['items']:
            if "python" in a['snippet']['title'].lower():
                yt_topic_video_ids_list.append([a['id']['videoId'], a['snippet']['title'], value['channelTitle']])

        try:
            if yt_search_resp['nextPageToken']:
                next_pg_token1 = yt_search_resp['nextPageToken']
        except Exception as pg_token_err:
            # print("There isn't a next page token")
            break
        # break

df_all_topic_videos = pd.DataFrame(yt_topic_video_ids_list, columns=['video_id','video_title','channel_title'])
print(df_all_topic_videos)
