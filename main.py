import hashlib
import json
import mimetypes
import os

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from tqdm import tqdm  # Import tqdm for progress bars


# Function to authenticate and create a Google Drive service instance
def authenticate_gdrive(creds_path):
    with open(creds_path) as creds_file:
        creds_data = json.load(creds_file)

    credentials = service_account.Credentials.from_service_account_info(
        creds_data,
        scopes=['https://www.googleapis.com/auth/drive.file']
    )

    return build('drive', 'v3', credentials=credentials)


# Function to calculate the MD5 checksum of a file
def calculate_md5(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


# Function to check if a file with the same content already exists in Google Drive
def file_exists(service, folder_id, file_name, file_md5):
    query = f"'{folder_id}' in parents and name='{file_name}' and mimeType!='application/vnd.google-apps.folder'"
    results = service.files().list(q=query, fields="files(id, name, md5Checksum)").execute()
    items = results.get('files', [])

    for item in items:
        if item.get('md5Checksum') == file_md5:
            print(f"File {file_name} already exists with the same content.")
            return True
    return False


# Function to check if a folder exists in Google Drive and return its ID
def get_or_create_folder(service, folder_name, parent_id):
    query = f"'{parent_id}' in parents and name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
    results = service.files().list(q=query, fields="files(id)").execute()
    items = results.get('files', [])

    if items:
        print(f"Folder {folder_name} already exists.")
        return items[0]['id']  # Return the ID of the existing folder

    # Create a new folder if it does not exist
    folder_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_id]
    }

    folder = service.files().create(body=folder_metadata, fields='id').execute()
    print(f'Created folder {folder_name} with ID: {folder.get("id")}')
    return folder.get('id')


# Function to upload a file to Google Drive
def upload_file(service, file_path, folder_id):
    file_name = os.path.basename(file_path)
    file_md5 = calculate_md5(file_path)

    if file_exists(service, folder_id, file_name, file_md5):
        return False  # Skip uploading if the file already exists

    file_metadata = {
        'name': file_name,
        'parents': [folder_id]
    }

    mime_type, _ = mimetypes.guess_type(file_path)

    media = MediaFileUpload(file_path, mimetype=mime_type, chunksize=1024 * 1024, resumable=True)

    request = service.files().create(body=file_metadata, media_body=media, fields='id')

    response = None
    while response is None:
        status, response = request.next_chunk()

    print(f'Uploaded {file_path} with ID: {response.get("id")}')
    return True  # Indicate that a file was uploaded


# Recursive function to count total files in directory
def count_files(local_directory):
    total_files = 0
    for item in os.listdir(local_directory):
        item_path = os.path.join(local_directory, item)
        if os.path.isdir(item_path):
            total_files += count_files(item_path)
        elif os.path.isfile(item_path):
            total_files += 1
    return total_files


# Recursive function to upload files and directories with progress tracking using tqdm
def upload_directory(service, local_directory, parent_id):
    total_files_count = count_files(local_directory)

    # Create a tqdm progress bar for the total number of files
    with tqdm(total=total_files_count, desc="Uploading files", unit="file") as pbar:
        for item in os.listdir(local_directory):
            item_path = os.path.join(local_directory, item)

            if os.path.isdir(item_path):
                # Get or create a corresponding folder in Google Drive
                folder_id = get_or_create_folder(service, item, parent_id)
                # Recursively upload the contents of this directory
                upload_directory(service, item_path, folder_id)
            elif os.path.isfile(item_path):
                # Upload the file if it's a regular file
                if upload_file(service, item_path, parent_id):
                    pbar.update(1)  # Increment progress bar on successful upload

                else:
                    pbar.update(1)  # Increment even if skipped


def upload_to_gdrive_from_local_dir(local_directory, gdrive_folder_url, creds_path):
    # Extract folder ID from the URL
    folder_id = gdrive_folder_url.split('/')[-1]

    service = authenticate_gdrive(creds_path)

    # Start uploading the directory contents
    upload_directory(service, local_directory, folder_id)


def list_files_in_folder(creds_path, folder_id):
    service = authenticate_gdrive(creds_path)
    query = f"'{folder_id}' in parents"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])

    if not items:
        print('No files found.')
    else:
        print('Files:')
        for item in items:
            print(f"{item['name']} ({item['id']})")


if __name__ == "__main__":
    LOCAL_DIRECTORY = 'local/file/path'
    folder_id = 'gdrive_folder_id'
    GDRIVE_FOLDER_URL = f'https://drive.google.com/drive/u/2/folders/{folder_id}'
    CREDS_PATH = '/gcloud-creds.json'

    upload_to_gdrive_from_local_dir(LOCAL_DIRECTORY, GDRIVE_FOLDER_URL, CREDS_PATH)
