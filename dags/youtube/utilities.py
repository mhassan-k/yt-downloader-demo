import os
import subprocess
from datetime import datetime
from helpers import Common
from helpers import WasbClient
from airflow.exceptions import AirflowSkipException


NoLongerAvailableErrorMsg = "Video unavailable. This video is no longer available because the uploader has closed their YouTube account."
RemovedErrorMsg = "Video unavailable. This video has been removed by the uploader"
PrivateVideoErrorMsg = (
    "Private video. Sign in if you've been granted access to this video"
)
VideoUnavailableErrorMsg = "Video unavailable. This content isn't available, try again later."
ContentNoLongerAvailableErrorMsg = "The following content is not available on this app.. Watch on the latest version of YouTube."
DownloadInternalServerMsg = "ERROR: [download] Got error: HTTP Error 500: Internal Server Error"
HTTP403ErrorMsg = "ERROR: unable to download video data: HTTP Error 403: Forbidden"
BotConfirmationErrorMsg = "Sign in to confirm you’re not a bot"
InvalidURIMsg = "The requested URI does not represent any resource on the server"
OffensiveErrorMsg = "The following content has been identified by the YouTube community as inappropriate or offensive to some audiences"
HTTP429ErrorMsg = "HTTP Error 429: Too Many Requests"
UnsupportedURLMsg = "WARNING: [generic] Falling back on generic information extractor. ERROR: Unsupported URL"


def get_videos_id():
    from airflow.models import Variable

    videos_retreieve_cnt = Variable.get("videos_retreieve_cnt")
    hook = Common.get_youtube_db_hook()
    query = f"""
        SELECT id from video where status = 'not downloaded'
        limit {videos_retreieve_cnt}
    """
    rows = hook.get_records(query)
    list_rows = [item for sublist in rows for item in sublist]
    return list_rows

def update_status_progress_video(video_ids):
    db = Common.get_youtube_db_hook()
    query = f"""
        UPDATE video
        set status = 'downloading'
        where id in ({video_ids})
    """
    db.run(sql=query)

def update_status(table_name, id, status):

    hook = Common.get_youtube_db_hook()

    sql = f"""
        UPDATE {table_name}
        SET status = %s,
            modified_at = %s
        WHERE id = %s
    """
    params = (status, datetime.now(), id)
    hook.run(sql, parameters=params)

def download_video(video_id, proxy_ip):
    # proxy_port = "3128"
    proxy_endpoint = "http://3qflb2641jpu9855ikzv0g:hig1ofr7l8a01ijqywkzs8i@172.205.9.121:443"

    # Construct the YouTube URL with the video ID
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    # Create a folder with the video ID as its name
    folder_name = f"/tmp/videos/video_{video_id}/"
    os.makedirs(folder_name, exist_ok=True)
    # Change directory to the folder
    os.chdir(folder_name)
    print("Get YouTube cookie")

    try:
        get_cookie = subprocess.run(
            ['curl', '-O', 'https://youtubedlpcookie.blob.core.windows.net/cookie/cookies-youtube-com.txt'],
            check=True,  
            text=True,  
            capture_output=True 
        )
        print("Output:", get_cookie.stdout)
    except subprocess.CalledProcessError as e:
         print("Error:", e.stderr)

    print("------------------------------------------------------------")
    files = os.listdir('.')
    if files:
        print("Files in the directory:", files)
    else:
        print("No files found in the directory.")
    print("------------------------------------------------------------")

    cookie_path = f'{folder_name}/cookies-youtube-com.txt'

    print(f"Start downloding {video_id}")
    # Run yt-dlp command to download the best quality video

    # if proxy_ip == "":
    #     list_subs_command = ["yt-dlp", "--list-subs", video_url]
    # else:
    #     list_subs_command = ["yt-dlp","--list-subs","--proxy", f"{proxy_ip}:{proxy_port}", video_url]
    
    list_subs_command = ["yt-dlp","--list-subs","--proxy", proxy_endpoint, 
                         "--ignore-config", "--no-check-certificates", video_url, "--cookies", cookie_path]

    list_subs_process = subprocess.run(
        list_subs_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
    )
    print(list_subs_process.stdout)

    has_subtitles = "Available subtitles" in list_subs_process.stdout
    caption_flag = "true" if has_subtitles else "false"
    print("Subtitles available:", has_subtitles)

    ffmpeg_location = "/usr/bin/ffmpeg"

    # if proxy_ip == "":
    command = ["yt-dlp","--write-sub", "--all-subs", "--sub-format", "vtt",
                "--extract-audio","--audio-format", "wav","--keep-video",
                "--ffmpeg-location", ffmpeg_location, "--ignore-config", "--no-check-certificates",
                "--proxy",proxy_endpoint, video_url, "--cookies", cookie_path, "-R", "20", "--continue",
                "-r", "6M"]
    # else:
    #     command = ["yt-dlp","--write-sub", "--all-subs", "--sub-format", "vtt",
    #         "--extract-audio","--audio-format", "wav","--keep-video",
    #         "--ffmpeg-location", ffmpeg_location,"--proxy", 
    #         f"{proxy_ip}:{proxy_port}", video_url]
        
    process = subprocess.run(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
    )
    print(process.stdout)

    if process.returncode != 0:
        raise Exception(process.stderr)
    
    video_size = 0
    folder_files = os.listdir(folder_name)
    mkv_files = [f for f in folder_files if f.endswith('.mkv')]
    mp4_files = [f for f in folder_files if f.endswith(f'[{video_id}].mp4')]
    webm_files = [f for f in folder_files if f.endswith(f'[{video_id}].webm')]

    if mkv_files:
        for file in mkv_files:
            video_size += os.path.getsize(os.path.join(folder_name, file))
    elif mp4_files:
        for file in mp4_files:
            video_size += os.path.getsize(os.path.join(folder_name, file))
    elif webm_files:
        for file in webm_files:
            video_size += os.path.getsize(os.path.join(folder_name, file))

    print(f'File Size:{video_size}')

    update_caption_size(caption_flag, video_size, video_id)

def upload_video(video_id):
    from youtube.youtube_helper import get_video_path
    video_path = get_video_path(video_id)

    con = WasbClient.WasbClient()
    folder_name = f"/tmp/videos/video_{video_id}/"
    mkv_present = any('.mkv' in file for file in os.listdir(folder_name))
    mp4_present = any(f'[{video_id}].mp4' in file for file in os.listdir(folder_name))
    vtt_present = any('.vtt' in file for file in os.listdir(folder_name))

    try:
        for filename in os.listdir(folder_name):
            if mkv_present and '.mkv' in filename:
                blob_name = f'{video_path}/{filename}'
            elif not mkv_present and f'[{video_id}].mp4' in filename:
                blob_name = f'{video_path}/{filename}'
            elif not mkv_present and not mp4_present and f'[{video_id}].webm' in filename:
                blob_name = f'{video_path}/{filename}'
            elif vtt_present and '.vtt' in filename:
                blob_name = f'{video_path.replace("/videos/", "/audios/")}/{filename}'
            elif not vtt_present and '.json' in filename:
                blob_name = f'{video_path.replace("/videos/", "/audios/")}/{filename}'
            elif '.wav' in filename:
                blob_name = f'{video_path.replace("/videos/", "/audios/")}/{filename}'
            else:
                continue

            source_file_path = os.path.join(folder_name, filename)
            print(source_file_path)
            con.upload_file(
                source_file_path, "data-ingestion", blob_name
            )
    except Exception as err:
        raise Exception(err)


def update_status_video(video_id, execution_time, status):
    hook = Common.get_youtube_db_hook()
    execution_time_mins = execution_time / 60
    execution_time_formatted = "{:.3f}".format(execution_time_mins)

    sql = f"""
        UPDATE video
        set status = %s,
        execution_time = %s,
        modified_at = %s

        where id = %s
    """
    params = (status, execution_time_formatted, datetime.now(), video_id)
    hook.run(sql, parameters=params)

def insert_error_and_update_status(video_id, execution_time, error_msg, created_at=None):
    hook = Common.get_youtube_db_hook()

    if created_at is None:
        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    update_status_video(video_id, execution_time, "error")

    row = [(video_id, error_msg, created_at)]
    fields = ['video_id', 'error_msg', 'created_at']
        
    hook.insert_rows(table='video_error', rows=row, target_fields=fields)


def update_caption_size(caption, size, video_id):
    hook = Common.get_youtube_db_hook()

    sql = f"""
        UPDATE video
        SET caption = %s,
            size = %s
        WHERE id = %s
    """
    params = (caption, size, video_id)
    hook.run(sql, parameters=params)