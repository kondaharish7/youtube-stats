"""
Extract videos from a youtube channel
"""
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 2000)
pd.set_option('max_colwidth', 10000)
from datetime import *
job_start_time = datetime.now()

from yt_authenticate_yt_api import *
from yt_Authenticate_AWS import *
from yt_stats_contexts import *

yt_client = get_yt_client()

search_query = "python"

yt_channel_ids_list = [{'channelId': 'UCWv7vMbMWH4-V0ZXdmDpPBA', 'channelTitle': 'Programming with Mosh'}]

# Extract all the videos from each channel id
yt_topic_video_ids_list = []; next_pg_token1 = ""
for value in yt_channel_ids_list:
    # print(value['channelId'])
    print(f"{datetime.now()} | Pulling all the videos on the topic from the Channel: {value['channelTitle']}. ", end="");log_time = datetime.now()
    while True:
        yt_search_resp = yt_client.search().list(part='id,snippet',
                                                 type='video',
                                                 channelId=value['channelId'],
                                                 pageToken=next_pg_token1,
                                                 fields="prevPageToken,nextPageToken,items(id,snippet(title))",
                                                 maxResults=50
                                                 ).execute()

        for a in yt_search_resp['items']:
            if search_query in a['snippet']['title'].lower():
                yt_topic_video_ids_list.append([value['channelTitle'], a['id']['videoId'], a['snippet']['title']])

        try:
            if yt_search_resp['nextPageToken']:
                next_pg_token1 = yt_search_resp['nextPageToken']
        except Exception as pg_token_err:
            # print("There isn't a next page token")
            break
        break #to limit to 1 loop
    print(f"elapsed, {datetime.now() - log_time}")

df_all_topic_videos = pd.DataFrame(yt_topic_video_ids_list, columns=['channel_title', 'video_id', 'video_title'])

print(f"{datetime.now()} | Pulling the video stats from all the Channels. ", end="");log_time = datetime.now()
yt_video_stats_list = []
for yt_video_details in yt_topic_video_ids_list:
    video_info_resp = yt_client.videos().list(
                                                part='statistics',
                                                fields="items(statistics(viewCount,likeCount))",
                                                id=yt_video_details[1]
                                            ).execute()
    yt_video_stats_list.append(yt_video_details+[video_info_resp['items'][0]['statistics']['viewCount'],video_info_resp['items'][0]['statistics']['likeCount']])
print(f"elapsed, {datetime.now() - log_time}")

df_video_stats = pd.DataFrame(yt_video_stats_list, columns=['channel_title', 'video_id', 'video_title', 'View_cnt', 'Likes_Cnt']).drop_duplicates()
convert_dict = {'View_cnt': int, 'Likes_Cnt': int}
df_video_stats = df_video_stats.astype(convert_dict)
# print(df_video_stats)

df_video_stats_smry = df_video_stats.groupby('channel_title').aggregate({'video_id':'count','View_cnt':'sum', 'Likes_Cnt':'sum'}).reset_index()
df_video_stats_smry['Likes_%'] = (df_video_stats_smry['Likes_Cnt'] / df_video_stats_smry['View_cnt']).round(2)
df_video_stats_smry.columns = ['Channel_Title','Total_Videos','Total_views','Total_likes','Likes_%']
print(df_video_stats_smry)

print(f"\n{str('--')*10}\n{job_start_time} | {datetime.now()} | {datetime.now() - job_start_time}")

# yt_channel_ids_list = [{'channelId': 'UCWv7vMbMWH4-V0ZXdmDpPBA', 'channelTitle': 'Programming with Mosh'},{'channelId': 'UC8butISFwT-Wl7EV0hUK0BQ', 'channelTitle': 'freeCodeCamp.org'}]