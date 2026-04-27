import io
import os
import posixpath
import random
import time
import logging
from urllib.parse import urlparse

import boto3
import requests
from dotenv import load_dotenv
from PIL import Image

from app.config import get_config

# Optional cairosvg for SVG conversion
try:
    import cairosvg  # type: ignore
except Exception:  # pragma: no cover
    cairosvg = None

load_dotenv()

logger = logging.getLogger(__name__)


class DownloadImage:
    """
    Helper for uploading original product images to S3.

    Copied from `primarkapi.py` so `primarkapi2.py` can upload images without Selenium.
    """

    def __init__(self, task_id: str = "unknown"):
        self.task_id = task_id
        cfg = get_config()
        self.bucket = cfg.AWS_STORAGE_BUCKET_NAME
        self.region = cfg.AWS_REGION
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=cfg.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=cfg.AWS_SECRET_ACCESS_KEY,
            region_name=cfg.AWS_REGION,
        )
        self.proxy = {
            "http": "http://woollxvh-rotate:0e5z98dv0njo@p.webshare.io:80",
            "https": "http://woollxvh-rotate:0e5z98dv0njo@p.webshare.io:80",
        }
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/100.0.0.0 Safari/537.36"
            ),
        }

    def log(self, message: str):
        """Log message with task_id."""
        logger.info(f"[{self.task_id}] {message}")

    def upload_images_to_s3(self, image_urls: list[str]) -> list[str]:
        """
        Upload images to an Amazon S3 bucket and return uploaded URLs
        in the same order as provided (falls back to original URL on failure).
        """
        uploaded_image_urls: list[str] = []

        for url in image_urls:
            s3_url: str | None = None
            try:
                if url and url != "None":
                    parsed = urlparse(url)
                    image_filename = os.path.basename(parsed.path)
                    response = None

                    # Determine if the URL contains "sephora" and use the proxy if so
                    if "sephora" in url:
                        max_retries = 50
                        retry_count = 0
                        while retry_count < max_retries:
                            try:
                                time.sleep(random.uniform(2, 4))
                                response = requests.get(
                                    url.replace(" ", ""),
                                    headers=self.headers,
                                    proxies=self.proxy,
                                    allow_redirects=True,
                                    timeout=60,
                                )
                                if response.status_code != 200 or response.status_code == 403:
                                    retry_count += 1
                                    continue
                                break
                            except Exception:
                                retry_count += 1
                                continue
                    else:
                        response = requests.get(
                            url.replace(" ", ""),
                            headers=self.headers,
                            allow_redirects=True,
                            timeout=30,
                        )

                    if response and response.status_code == 200:
                        content_type = response.headers.get("Content-Type", "image/jpeg")
                        content_io = io.BytesIO(response.content)
                        s3_object_key = posixpath.join("product", image_filename)
                        self.s3_client.upload_fileobj(
                            content_io,
                            self.bucket,
                            s3_object_key,
                            ExtraArgs={"ContentType": content_type, "ACL": "public-read"},
                        )
                        s3_url = (
                            f"https://s3.{self.region}.amazonaws.com/"
                            f"{self.bucket}/{s3_object_key.replace('+','%2B')}"
                        )
                    else:
                        self.log(f"Failed to download image from URL: {url}")
            except requests.exceptions.Timeout:
                self.log(
                    f"The request timed out. Please try again later or check your internet connection."
                )
                s3_url = None
            except requests.exceptions.RequestException as e:
                self.log(f"An error occurred: {e}")
                s3_url = None
            except Exception as e:
                self.log(f"An error occurred: {str(e)}")
                s3_url = None

            uploaded_image_urls.append(s3_url or url)

        return uploaded_image_urls

    def upload_images(self, images):
        """
        Uploads images to S3 and returns concatenated URL string.
        """
        if images == "" or images is None:
            return ""
        image_urls = images.split("|")
        uploaded_urls = self.upload_images_to_s3(image_urls)
        return "|".join(uploaded_urls)


class ThumbnailDownloadImage:
    """
    Helper for creating and uploading thumbnail images to S3.
    """
    
    def __init__(self, task_id: str = "unknown"):
        """
        Initializes the ThumbnailDownloadImage class.
        """
        self.task_id = task_id
        cfg = get_config()
        self.bucket = cfg.AWS_STORAGE_BUCKET_NAME
        self.region = cfg.AWS_REGION
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=cfg.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=cfg.AWS_SECRET_ACCESS_KEY,
            region_name=cfg.AWS_REGION,
        )
        self.uploaded_image_urls = []

    def log(self, message: str):
        """Log message with task_id."""
        logger.info(f"[{self.task_id}] {message}")

    def resize_image(self, image, max_size):
        """Resize while maintaining aspect ratio and white padding."""
        original_width, original_height = image.size
        aspect_ratio = original_width / original_height
        max_width, max_height = max_size

        if aspect_ratio > 1:
            if original_width > max_width:
                new_width = max_width
                new_height = int(max_width / aspect_ratio)
            else:
                new_width = original_width
                new_height = original_height
        else:
            if original_height > max_height:
                new_height = max_height
                new_width = int(max_height * aspect_ratio)
            else:
                new_width = original_width
                new_height = original_height

        resized_image = image.resize((new_width, new_height), Image.LANCZOS)
        final_image = Image.new("RGB", max_size, (255, 255, 255))
        paste_position = ((max_width - new_width) // 2, (max_height - new_height) // 2)
        final_image.paste(resized_image, paste_position)
        return final_image

    def upload_to_s3(self, bytes_io, image_filename, content_type, folder_name):
        """Upload bytes stream to S3 and store URL."""
        s3_object_key = posixpath.join(folder_name, image_filename)
        self.s3_client.upload_fileobj(
            bytes_io,
            self.bucket,
            s3_object_key,
            ExtraArgs={
                "ContentType": content_type,
                "ACL": "public-read",
            },
        )
        s3_url = (
            f"https://s3.{self.region}.amazonaws.com/"
            f"{self.bucket}/{s3_object_key.replace('+','%2B')}"
        )
        self.uploaded_image_urls.append(s3_url)

    def process_image_url(self, url, folder_name):
        """Download, resize, and upload a single image URL."""
        if url is None or url == "None":
            return

        parsed = urlparse(url)
        image_filename = "thumbnail_" + os.path.basename(parsed.path)
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/100.0.0.0 Safari/537.36"
            ),
        }

        try:
            response = requests.get(
                url, headers=headers, allow_redirects=True, timeout=10
            )
            if response.status_code != 200:
                self.log(f"Failed to download image from URL: {url}")
                return

            content_type = response.headers.get("Content-Type", "")
            content_io = io.BytesIO(response.content)

            if "image/svg+xml" in content_type:
                if cairosvg is None:
                    raise RuntimeError(
                        "cairosvg is not available; install system Cairo or skip SVG conversion"
                    )
                png_image = cairosvg.svg2png(
                    bytestring=content_io.getvalue(),
                    output_width=376,
                    output_height=324,
                )
                content_io = io.BytesIO(png_image)
                image_filename = image_filename.replace(".svg", ".png")
                image = Image.open(content_io)
            else:
                image = Image.open(content_io)

            final_image = self.resize_image(image, (376, 324))
            bytes_io = io.BytesIO()
            final_image.save(bytes_io, format="JPEG", quality=95)
            bytes_io.seek(0)

            self.upload_to_s3(bytes_io, image_filename, "image/jpeg", folder_name)
        except requests.exceptions.Timeout:
            self.uploaded_image_urls.append(url)
            self.log(
                f"The request timed out. Please try again later or check your internet connection."
            )
        except requests.exceptions.RequestException as e:
            self.log(f"An error occurred: {e}")
        except Exception as e:
            self.log(f"An error occurred: {str(e)}")

    def upload_images_to_s3(self, image_urls, folder_name):
        """Upload multiple image URLs as thumbnails."""
        for url in image_urls:
            try:
                self.process_image_url(url, folder_name)
            except Exception as e:
                self.log(f"An error occurred: {str(e)}")

        return self.uploaded_image_urls

    def upload_images(self, images, folder_name="product"):
        """Upload multiple image URLs separated by | and return concatenated URLs."""
        if not images:
            return ""
        image_urls = images.split("|")
        uploaded_urls = self.upload_images_to_s3(image_urls, folder_name)
        return "|".join(uploaded_urls)
