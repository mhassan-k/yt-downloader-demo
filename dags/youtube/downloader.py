from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import timedelta
import pendulum
import os
import csv
import subprocess

@dag(
    dag_id="video_downloader",
    start_date=pendulum.datetime(2023, 5, 4, 3, 0, 0, tz="UTC"),
    schedule_interval="*/55 * * * *",
    catchup=False,
    tags=["youtube"],
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=15),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=1),
    },
)
def youtube_downloader():

    @task
    def read_video_ids():
        csv_file = Variable.get("data_folder", default_var="/opt/airflow/data/youtube_sample.csv")
        max_videos = int(Variable.get("max_videos_per_run", default_var="1000"))

        video_ids = []
        with open(csv_file, 'r') as file:
            csv_reader = csv.reader(file)
            next(csv_reader)  # Skip header
            for i, row in enumerate(csv_reader):
                if i >= max_videos:
                    break
                video_ids.append(row[0])

        return video_ids

    @task(max_active_tis_per_dag=1000)
    def download_video(video_id: str):
        proxy_endpoint = Variable.get("proxy_endpoint", default_var="http://proxy:port")
        output_folder = Variable.get("output_folder", default_var="/path/to/output/folder")
        cookie_file = Variable.get("cookie_file", default_var="/path/to/cookie/file")

        output_path = os.path.join(output_folder, f"{video_id}")
        os.makedirs(output_path, exist_ok=True)

        video_url = f"https://www.youtube.com/watch?v={video_id}"

        try:
            command = [
                "yt-dlp",
                "--write-sub", "--all-subs", "--sub-format", "vtt",
                "--extract-audio", "--audio-format", "wav", "--keep-video",
                "--ffmpeg-location", "/usr/bin/ffmpeg",
                "--ignore-config", "--no-check-certificates",
                "--proxy", proxy_endpoint,
                "--cookies", cookie_file,
                "-R", "20", "--continue",
                "-r", "6M",
                "-o", f"{output_path}/%(id)s.%(ext)s",
                video_url
            ]

            result = subprocess.run(command, check=True, capture_output=True, text=True)
            print(f"Successfully downloaded video {video_id}")
            print(result.stdout)

        except subprocess.CalledProcessError as e:
            print(f"Error downloading video {video_id}: {e}")
            print(e.stderr)
            raise

        except Exception as e:
            print(f"Unexpected error downloading video {video_id}: {e}")
            raise

    video_ids = read_video_ids()
    download_tasks = download_video.expand(video_id=video_ids)

youtube_downloader()