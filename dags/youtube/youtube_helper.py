from helpers.Common import get_youtube_db_hook

def update_status(table_name, id, status):
    from datetime import datetime
    
    hook = get_youtube_db_hook()
    
    sql = f"""
        UPDATE {table_name}
        SET status = %s,
            modified_at = %s
        WHERE id = %s
    """
    params = (status, datetime.now(), id)
    hook.run(sql, parameters=params)

def update_status_video(video_ids, status):
    hook = get_youtube_db_hook()
    query = f"""
        UPDATE video
        set status = '{status}'
        where id in ({video_ids})
    """
    hook.run(sql=query)

def get_youtube_client(api_key):
    from googleapiclient.discovery import build

    return build('youtube', 'v3', developerKey=api_key)

def reset_quota_exceeded_keys():
    from googleapiclient.errors import HttpError

    hook = get_youtube_db_hook()
    sql = """
        SELECT "id", "key" FROM api_key
        WHERE "quota_exceeded" = TRUE
    """
    keys = hook.get_records(sql)

    for key_id, api_key in keys:
        youtube = get_youtube_client(api_key)
        try:
            youtube_api_request(youtube, method='videos', part='snippet', type='channel',
                relevanceLanguage="ar",maxResults=1
            ).execute()

            sql_update = """
                UPDATE api_key
                SET "quota_exceeded" = FALSE
                WHERE "id" = %s
            """
            hook.run(sql_update, parameters=[key_id])
        except HttpError as e:
            if e.resp.status != 403:
                raise e

def fetch_api_key():
    hook = get_youtube_db_hook()
    sql = """
        SELECT "id", "key" FROM api_key
        WHERE "quota_exceeded" = FALSE
        ORDER BY "last_run_dttm" DESC
    """
    result = hook.get_first(sql)
    if not result:
        reset_quota_exceeded_keys()
        result = hook.get_first(sql)
        if not result:
            raise Exception("No available key to use.")
    return result

def switch_api_key(current_key_id):
    hook = get_youtube_db_hook()
    sql = """
        UPDATE api_key
        SET "quota_exceeded" = TRUE
        WHERE "id" = %s
    """
    hook.run(sql, parameters=[current_key_id])
    return fetch_api_key()

def update_key_usage(key_id, usage_count):
    from datetime import datetime
    hook = get_youtube_db_hook()
    sql = """
        UPDATE api_key
        SET "usage" = "usage" + %s, "last_run_dttm" = %s
        WHERE "id" = %s
    """
    hook.run(sql, parameters=[usage_count, datetime.now(), key_id])

def execute_request(api_function, **kwargs):
    from googleapiclient.errors import HttpError

    key_id, api_key = fetch_api_key()
    youtube = get_youtube_client(api_key)
    key_usage = 0

    while True:
        try:
            request = api_function(youtube, **kwargs)
            response = request.execute()
            key_usage += 1
            update_key_usage(key_id, key_usage)
            return response
        except HttpError as e:
            if e.resp.status == 403:
                print(f"API Key {api_key} failed. Switching key.")
                key_id, api_key = switch_api_key(key_id)
                youtube = get_youtube_client(api_key)
                key_usage = 0
            else:
                raise e

def youtube_api_request(youtube, method, **kwargs):

    valid_methods = {
        'videos': youtube.videos().list,
        'channels': youtube.channels().list,
        'search': youtube.search().list,
        'playlists': youtube.playlists().list,
        'playlistItems': youtube.playlistItems().list,
        'captions': youtube.captions().list
    }
    
    if method in valid_methods:
        return valid_methods[method](**kwargs)
    else:
        raise ValueError(f"Invalid YouTube API method: {method}")
    
def format_tags_for_postgresql(tags):
    if not tags:
        return "{}"  
    escaped_tags = [tag.replace('"', '\\"') for tag in tags]  
    return "{" + ", ".join(f'"{tag}"' for tag in escaped_tags) + "}"

def extract_topic_names(topic_categories):
    topic_names = []
    for url in topic_categories:
        topic_name = url.split('/')[-1]
        topic_name = topic_name.replace('_', ' ').title()
        topic_names.append(topic_name)
    return topic_names

def find_topic_ids(topic_names):
    hook = get_youtube_db_hook()
    topic_ids = []
    for name in topic_names:
        query = "SELECT id FROM topic WHERE name = %s;"
        params = (name,)    
        result = hook.get_first(query, parameters=params)

        if result:
            topic_ids.append(result[0])
            print(f"Found ID: {result[0]} for topic: {name}") 
        else:
            print(f"No ID found for topic: {name}")  
    return topic_ids

def parse_info(json_list, type):
    import pandas as pd
    from datetime import datetime

    data = []

    for json_data in json_list:
        if not isinstance(json_data, dict) or 'items' not in json_data:
            continue

        for item in json_data['items']:
            if type == 'playlist':
                entry = {
                    'id': item.get('id', ''),
                    'channel_id': item['snippet'].get('channelId', ''),
                    'url': f"https://www.youtube.com/playlist?list={item.get('id', '')}",
                    'title': item['snippet'].get('title', ''),
                    'description': item['snippet'].get('description', ''),
                    'publish_date': item['snippet'].get('publishedAt', ''),
                    'item_count': item['contentDetails'].get('itemCount', 0),
                    'status': 'not collected',
                    'created_at': datetime.now()
                }
            elif type == 'playlistVideos':
                video_id = item['snippet']['resourceId']['videoId']
                playlist_id  = item['snippet'].get('playlistId', None)
                channel_id = item['snippet']['channelId']
                entry = {
                    'id': video_id,
                    'playlist_id': playlist_id,
                    'channel_id': channel_id
                }
            elif type == 'liveVideos':
                video_id = item['id']['videoId']
                channel_id = item['snippet']['channelId']
                entry = {
                    'id': video_id,
                    'playlist_id': None,
                    'channel_id': channel_id
                }
            elif type == 'video':
                video_id = item['id']
                tags = item['snippet'].get('tags', [])
                tags_string = format_tags_for_postgresql(tags)

                entry = {
                    'category_id': item['snippet'].get('categoryId', ''),
                    'url': f"https://www.youtube.com/watch?v={video_id}",
                    'title': item['snippet'].get('title', ''),
                    'description': item['snippet'].get('description', ''),
                    'tags': tags_string,
                    'language': item['snippet'].get('defaultAudioLanguage', ''),
                    'duration': item['contentDetails'].get('duration', ''),
                    'dimension': item['contentDetails'].get('dimension', ''),
                    'definition': item['contentDetails'].get('definition', ''),
                    'caption': item['contentDetails'].get('caption', ''),
                    # 'recording_date': item.get('recordingDetails', {}).get('recordingDate', None),
                    'licensed_content': item['contentDetails'].get('licensedContent', False),
                    'for_kids': item['status'].get('madeForKids', False),
                    'publish_date': item['snippet'].get('publishedAt', ''),  
                    'view_count': item['statistics'].get('viewCount', 0),
                    'like_count': item['statistics'].get('likeCount', 0),
                    'dislike_count': item['statistics'].get('dislikeCount', 0),
                    'favorite_count': item['statistics'].get('favoriteCount', 0),
                    'comment_count': item['statistics'].get('commentCount', 0)
                }
            elif type == 'video_topic':
                video_id = item['id']
                topic_ids = item.get('topicDetails', {}).get('topicIds', [])
                if not topic_ids:  # Check if topicIds are missing
                    topic_categories = item.get('topicDetails', {}).get('topicCategories', [])
                    if topic_categories:
                        topic_names = extract_topic_names(topic_categories)
                        topic_ids = (find_topic_ids(topic_names))

                        for topic_id in topic_ids:
                            if topic_id:
                                entry = {
                                    'video_id': video_id,
                                    'topic_id': topic_id
                                }
                                data.append(entry)
                continue
            
            elif type == 'caption':
                entry = {
                    'id': item.get('id', ''),
                    'video_id': item['snippet'].get('videoId', ''),
                    'language': item['snippet'].get('language', ''),
                    'last_updated': item['snippet'].get('lastUpdated', ''),
                }
            else:
                continue  

            if entry is not None: 
                data.append(entry)
    
    return pd.DataFrame(data)

def get_channel_id(video_id):

    hook = get_youtube_db_hook()
    
    query = "SELECT channel_id FROM video WHERE id = %s;"
    params = (video_id,)
        
    result = hook.get_first(query, parameters=params)
    
    channel_id = result[0]
    print(f"Channel ID: {channel_id}")

    return channel_id

def update_video_path(video_path, video_id):
    from datetime import datetime

    hook = get_youtube_db_hook()
            
    sql = f"""
        UPDATE video
        SET path = %s,
            modified_at = %s
        WHERE id = %s
    """
    params = (video_path, datetime.now(), video_id)
    
    hook.run(sql, parameters=params)

def get_video_path(video_id):

    hook = get_youtube_db_hook()
    
    query = "SELECT path FROM video WHERE id = %s;"
    params = (video_id,)
        
    result = hook.get_first(query, parameters=params)
    
    video_path = result[0]
    print(f"Video Path: {video_path}")

    return video_path

def convert_row(row):
    import numpy as np

    try:
        return tuple(x.item() if isinstance(x, np.generic) else x for x in row)
    except Exception as e:
        for index, value in enumerate(row):
            if isinstance(value, np.generic):
                print(f"Issue with column index {index} having value {value} with type {type(value)}")
        raise e

def get_published_at(response):
    import logging
    from datetime import datetime

    try:
        if 'items' in response and response['items']:
            item = response['items'][0]
            if 'snippet' in item and 'publishedAt' in item['snippet']:
                published_at_str = item['snippet']['publishedAt']
                published_at_datetime = datetime.fromisoformat(published_at_str.replace("Z", "+00:00"))
                published_at = published_at_datetime.strftime("%Y-%m")
                return published_at
            else:
                logging.error("'snippet' or 'publishedAt' not found in item.")
                return 'unknown'
        else:
            logging.error("No items found in the API response.")
            return 'unknown'
    except Exception as e:
        logging.error(f"An error occurred while processing published date: {e}")
        return 'unknown'