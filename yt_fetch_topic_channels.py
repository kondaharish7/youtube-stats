"""
Find the youtube channels for a particular search topic (Scala, Python, Apache Hudi).
"""
import pandas as pd
from datetime import *
job_start_time = datetime.now()

from yt_authenticate_yt_api import *
from yt_Authenticate_AWS import *
from yt_stats_contexts import *

pd.set_option('display.max_columns', None); pd.set_option('display.max_rows', None); pd.set_option('display.width', 2000); pd.set_option('max_colwidth', 10000)

yt_client = get_yt_client()

search_query = "apache hudi tutorials"

#Get the video list from the search query
total_search_videos = 50 #in the multiples of 50
yt_video_ids_list = []; next_pg_token = ""
print(f"{datetime.now()} | Searching for the videos. ", end=""); log_time = datetime.now()
for iter in range(1,int(total_search_videos/50)+1):
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
        print(f"{datetime.now()} | {yt_search_error}")
print(f"elapsed, {datetime.now() - log_time}")

# Extract Channels id's list from the resulted videos list
print(f"{datetime.now()} | Pulling the distinct channel id's from the search result videos. ", end=""); log_time = datetime.now()
yt_channel_ids_list = []
for yt_video_id in yt_video_ids_list:
    video_info_resp = yt_client.videos().list(
                                                part='snippet',
                                                fields="items(snippet(channelTitle,channelId))",
                                                id=yt_video_id
                                            ).execute()
    # print(channel_info_resp)
    yt_channel_ids_list.append(video_info_resp['items'][0]['snippet'])
    # break
print(f"elapsed, {datetime.now() - log_time}")

print(f"{datetime.now()} | Pulling the Channel's info. ", end=""); log_time = datetime.now()
yt_channel_info_list = []
for channel_details in yt_channel_ids_list:
    channel_info_resp = yt_client.channels().list(part='statistics',
                                                  fields="items(statistics(viewCount,subscriberCount,videoCount))",
                                                  id=channel_details['channelId']
                                                  ).execute()
    # print(channel_info_resp)
    yt_channel_info_list.append([channel_details['channelId'], channel_details['channelTitle'], channel_info_resp['items'][0]['statistics']['viewCount'], channel_info_resp['items'][0]['statistics']['subscriberCount'], channel_info_resp['items'][0]['statistics']['videoCount']])
print(f"elapsed, {datetime.now() - log_time}")

print(f"{datetime.now()} | Saving the results to S3. ", end=""); log_time = datetime.now()
df_topic_chnls = pd.DataFrame(yt_channel_info_list, columns=['channel_id','channel_title','total_views','total_subscribers','total_videos']).drop_duplicates()
convert_dict = {'total_views': int, 'total_subscribers': int, 'total_videos':int}
df_topic_chnls = df_topic_chnls.astype(convert_dict)
df_topic_chnls = df_topic_chnls.sort_values(by=['total_views'], ascending=[False])
df_topic_chnls['search_topic'] = search_query

topic_chnls_list_file = f"{search_query.replace(" ", "_")}.csv"
topic_chnls_list_s3_key = f"s3://{aws_s3_bucket}/data/yt_search_queries/yt_channels_list/{topic_chnls_list_file}"
df_topic_chnls.to_csv(topic_chnls_list_s3_key, index=False, storage_options={"key": root_user_access_key, "secret": root_user_sceret_key})
print(f"elapsed, {datetime.now() - log_time}")

print(f"\n{str('--')*10}\n{job_start_time} | {datetime.now()} | {datetime.now() - job_start_time}")
