"""
Extract videos from a youtube channel
"""
import pandas as pd
from datetime import *
job_start_time = datetime.now()

from yt_authenticate_yt_api import *
from yt_Authenticate_AWS import *
from yt_stats_contexts import *

pd.set_option('display.max_columns', None); pd.set_option('display.max_rows', None); pd.set_option('display.width', 2000); pd.set_option('max_colwidth', 10000)

yt_client = get_yt_client()
search_query = "python tutorials"
yt_channel_ids_list = []

try:
    video_stats_smry_file = f"yt_{search_query.replace(" ", "_")}_video_stats_smry.csv"
    df_video_stats_old = pd.read_csv(f"s3://{aws_s3_bucket}/data/yt_search_queries/yt_video_stats/{video_stats_smry_file}",
                                storage_options={"key": root_user_access_key, "secret": root_user_sceret_key}
                                )
    print(f"Video stats file found.")
    df_chnls_list = pd.read_csv(f"s3://{aws_s3_bucket}/data/yt_search_queries/yt_channels_list/{search_query.replace(" ", "_")}.csv",
                                storage_options={"key": root_user_access_key, "secret": root_user_sceret_key}
                                )
    df_new = df_chnls_list.merge(df_video_stats_old, how='left', left_on='channelTitle', right_on='Channel_Title')
    df_new = df_new[['channelId', 'channelTitle', 'search_topic', 'Channel_Title']]
    df_new = df_new[df_new['Channel_Title'].isna()].head(1)
    for index, row in df_new.iterrows():
        row_dict = {}
        row_dict['channelId'] = row.loc['channelId']; row_dict['channelTitle'] = row.loc['channelTitle']
        yt_channel_ids_list.append(row_dict)
except:
    df_chnls_list = pd.read_csv(f"s3://{aws_s3_bucket}/data/yt_search_queries/yt_channels_list/{search_query.replace(" ", "_")}.csv",
                                storage_options={"key": root_user_access_key, "secret": root_user_sceret_key}
                                )
    df_chnls_list = df_chnls_list.head(3)
    print(f"No Video stats file found, hence pulling Channels list from channels list file.")
    for index, row in df_chnls_list.iterrows():
        row_dict = {}
        row_dict['channelId'] = row.loc['channelId']; row_dict['channelTitle'] = row.loc['channelTitle']
        yt_channel_ids_list.append(row_dict)

# print(yt_channel_ids_list)
# yt_channel_ids_list = [{'channelId': 'UCWv7vMbMWH4-V0ZXdmDpPBA', 'channelTitle': 'Programming with Mosh'}, {'channelId': 'UC8butISFwT-Wl7EV0hUK0BQ', 'channelTitle': 'freeCodeCamp.org'},{'channelId': 'UCsvqVGtbbyHaMoevxPAq9Fg', 'channelTitle': 'Simplilearn'}]
yt_channel_ids_list = [{'channelId': 'UC8butISFwT-Wl7EV0hUK0BQ', 'channelTitle': 'freeCodeCamp.org'}]

# Extract all the videos from each channel id
yt_topic_video_ids_list = []; next_pg_token1 = ""
for value in yt_channel_ids_list:
    print(f"{datetime.now()} | Pulling all the videos on the topic from the Channel: {value['channelTitle']}. ", end="");log_time = datetime.now()
    while True:
        try:
            print(f"\n{datetime.now()} | searching for the videos from : {value['channelTitle']}. ")
            yt_search_resp = yt_client.search().list(part='id,snippet',
                                                     type='video',
                                                     q="python",
                                                     channelId=value['channelId'],
                                                     pageToken=next_pg_token1,
                                                     fields="prevPageToken,nextPageToken,items(id,snippet(title))",
                                                     maxResults=50
                                                     ).execute();print(yt_search_resp)

            for a in yt_search_resp['items']:
                # yt_topic_video_ids_list.append([value['channelTitle'], a['id']['videoId'], a['snippet']['title']])
                if search_query.replace(" tutorials", "") in a['snippet']['title'].lower():
                    yt_topic_video_ids_list.append([value['channelTitle'], a['id']['videoId'], a['snippet']['title']])#;print([value['channelTitle'], a['id']['videoId'], a['snippet']['title']])

            try:
                if yt_search_resp['nextPageToken']:
                    next_pg_token1 = yt_search_resp['nextPageToken']
            except Exception as pg_token_err:
                print("No next page token found.")
                break
        except Exception as api_err:
            print(api_err)
            break
        # break #to limit to 1 loop
    print(f"elapsed, {datetime.now() - log_time}")
# print(yt_topic_video_ids_list);print()
print(f"{datetime.now()} | Pulling the video stats from all the Channels. ", end="");log_time = datetime.now()
yt_video_stats_list = []
for yt_video_details in yt_topic_video_ids_list:
    # print(f"{yt_video_details[0]},",end="")
    video_info_resp = yt_client.videos().list(part='statistics',
                                              fields="items(statistics(viewCount,likeCount))",
                                              id=yt_video_details[1]
                                             ).execute()
    if 'likeCount' in video_info_resp.keys():
        like_cnt = video_info_resp['items'][0]['statistics']['likeCount']
    else:
        like_cnt = 0
    yt_video_stats_list.append(yt_video_details+[video_info_resp['items'][0]['statistics']['viewCount'], like_cnt])
print(f"elapsed, {datetime.now() - log_time}")
# print(yt_video_stats_list);print()
df_video_stats = pd.DataFrame(yt_video_stats_list, columns=['channel_title', 'video_id', 'video_title', 'View_cnt', 'Likes_Cnt']).drop_duplicates()
convert_dict = {'View_cnt': int, 'Likes_Cnt': int}
df_video_stats = df_video_stats.astype(convert_dict)
print(df_video_stats);print()
df_video_stats_smry = df_video_stats.groupby('channel_title').aggregate({'video_id':'count','View_cnt':'sum', 'Likes_Cnt':'sum'}).reset_index()
df_video_stats_smry['Likes_%'] = (df_video_stats_smry['Likes_Cnt'] / df_video_stats_smry['View_cnt']).round(2)
df_video_stats_smry.columns = ['Channel_Title', 'Total_Videos', 'Total_views', 'Total_likes', 'Likes_%']
print(df_video_stats_smry);print()
df_final = pd.concat([df_video_stats_old, df_video_stats_smry], ignore_index=True)
df_final = df_final.sort_values(by=['Total_views'], ascending=[False])
print(df_final)

# Upload the file to S3
video_stats_smry_s3_key = f"s3://{aws_s3_bucket}/data/yt_search_queries/yt_video_stats/{video_stats_smry_file}"
df_final.to_csv(video_stats_smry_s3_key, index=False, storage_options={"key": root_user_access_key, "secret": root_user_sceret_key})

print(f"\n{str('--')*10}\n{job_start_time} | {datetime.now()} | {datetime.now() - job_start_time}")

