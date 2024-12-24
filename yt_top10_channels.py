
"""
Find the top 10 best youtube channels to learn a particular topic (Scala, Python, apache hudi).
"""

import json
import pandas as pd
import googleapiclient.discovery
from yt_stats_pwds import *

api_service_name = "youtube"
api_version = "v3"

yt_client = googleapiclient.discovery.build(api_service_name, api_version, developerKey = DEVELOPER_KEY)

#Get the video id's list from the search query
search_query = "python tutorials"
total_search_videos = 50 #in the multiples of 50

yt_video_ids_list = []; next_pg_token = ""
for iter in range(1,int(total_search_videos/50)+1):
    print(f"loop: {iter}, page:{next_pg_token}")
    yt_search_resp = yt_client.search().list(part='id', type='video', pageToken=next_pg_token, q=search_query, maxResults=10).execute()
    # print(yt_serch_resp)
    next_pg_token = yt_search_resp['nextPageToken']
    for a in yt_search_resp['items']:
        yt_video_ids_list.append(a['id']['videoId'])

# # get Channels id's list from the resulted videos list
# yt_channel_ids_list = []
# for yt_video_id in yt_video_ids_list:
#     channel_info_resp = yt_client.videos().list(part='id,snippet', fields="items(id,snippet(channelTitle,channelId))",id=yt_video_id).execute()
#     for k,v in channel_info_resp.items():
#         # print(f"{k}\n\t{v}")
#         yt_channel_ids_list.append(v[0]['snippet'])
#     break
# print(yt_channel_ids_list)
#
# #search for Topic videos in each channel
# yt_topic_video_ids_list = []; next_pg_token1 = ""
# for value in yt_channel_ids_list:
#     print(value['channelId'])
#     for i in range(1,5+1):
#         yt_search_resp = yt_client.search().list(part='id', type='video', channelId=value['channelId'], pageToken=next_pg_token1, fields="prevPageToken,nextPageToken,pageInfo,items(id)", maxResults=50).execute()
#         print(yt_search_resp)
#
#         for a in yt_search_resp['items']:
#             yt_topic_video_ids_list.append(a['id']['videoId'])
#         if not yt_search_resp['nextPageToken']:
#             break
#         else:
#             next_pg_token1 = yt_search_resp['nextPageToken']
