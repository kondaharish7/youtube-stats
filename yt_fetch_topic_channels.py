"""
Find the youtube channels for a particular search topic (Scala, Python, Apache Hudi).
"""
import pandas as pd
from datetime import *
job_start_time = datetime.now()

from yt_authenticate_yt_api import *
from yt_Authenticate_AWS import *
from yt_stats_contexts import *

yt_client = get_yt_client()

#Get the video id's list from the search query
search_query = "javascript tutorials"
total_search_videos = 500 #in the multiples of 50

yt_video_ids_list = []; next_pg_token = ""
for iter in range(1,int(total_search_videos/50)+1):
    # print(f"loop: {iter}, page:{next_pg_token}")
    try:
        yt_search_resp = yt_client.search().list(
                                                part='id',
                                                type='video',
                                                pageToken=next_pg_token,
                                                q=search_query,
                                                maxResults=total_search_videos
                                            ).execute()
        next_pg_token = yt_search_resp['nextPageToken']
        for a in yt_search_resp['items']:
            yt_video_ids_list.append(a['id']['videoId'])
    except Exception as yt_search_error:
        print(f"{yt_search_error}")

# Extract Channels id's list from the resulted videos list
yt_channel_ids_list = []
for yt_video_id in yt_video_ids_list:
    channel_info_resp = yt_client.videos().list(
                                                part='snippet',
                                                fields="items(snippet(channelTitle,channelId))",
                                                id=yt_video_id
                                            ).execute()
    yt_channel_ids_list.append(channel_info_resp['items'][0]['snippet'])
    # break

df_topic_chnls = pd.DataFrame(yt_channel_ids_list).drop_duplicates()
df_topic_chnls['search_topic'] = search_query
# print(df_topic_chnls)

topic_chnls_list_file = f"{search_query.replace(" ", "_")}.csv"
topic_chnls_list_s3_key = f"s3://{aws_s3_bucket}/data/yt_search_queries/yt_channels_list/{topic_chnls_list_file}"
df_topic_chnls.to_csv(topic_chnls_list_s3_key, index=False, storage_options={"key": root_user_access_key, "secret": root_user_sceret_key})

print(f"\n{str('--')*10}\n{job_start_time} | {datetime.now()} | {datetime.now() - job_start_time}")


# topic_chnls_s3_key_full = f"s3://{aws_s3_bucket}/data/yt_channels/full/search_query={search_query.replace(" ", "_")}/date={datetime.now().date()}/{topic_chnls_file}"
# topic_chnls_s3_key_latest = f"s3://{aws_s3_bucket}/data/yt_channels/latest/search_query={search_query.replace(" ", "_")}/{search_query.replace(" ", "_")}.csv"
