import mimetypes
import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

SCOPES = ["https://www.googleapis.com/auth/drive.readonly", "https://www.googleapis.com/auth/drive.file"]
FULL_PATH = "/Users/mafed/DnD"

creds = None

if os.path.exists("token.json"):
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            "credentials.json", SCOPES
        )
        creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open("token.json", "w") as token:
        token.write(creds.to_json())

service = build("drive", "v3", credentials=creds)


def return_recent_files():
    results = (
        service.files()
        .list(pageSize=10, fields="nextPageToken, files(id, name)")
        .execute()
    )
    items = results.get("files", [])

    if not items:
        print("No files found.")
        return
    print("Files:")
    for item in items:
        print(f"{item['name']} ({item['id']})")


def get_drive_info():
    # Выполнение запроса к методу about()
    response = service.about().get(fields="user, storageQuota").execute()

    # Получение информации о пользователе и квоте хранилища
    user_info = response.get('user')
    storage_quota = response.get('storageQuota')

    # Вывод информации
    print("User Information:")
    print(f"Name: {user_info.get('displayName')}")
    print(f"Email: {user_info.get('emailAddress')}")
    print("\nStorage Quota:")
    print(f"Total: {storage_quota.get('limit')}")
    print(f"Used: {int(storage_quota.get('usage')) / 8**9}")
    print(f"Available: {int(storage_quota.get('limit')) - int(storage_quota.get('usage'))}")


def folder_upload(service):
    parents_id = {}

    for root, _, files in os.walk(FULL_PATH, topdown=True):
        last_dir = root.split('/')[-1]
        pre_last_dir = root.split('/')[-2]
        if pre_last_dir not in parents_id.keys():
            pre_last_dir = []
        else:
            pre_last_dir = parents_id[pre_last_dir]

        folder_metadata = {'name': last_dir,
                           'parents': [pre_last_dir],
                           'mimeType': 'application/vnd.google-apps.folder'}
        create_folder = service.files().create(body=folder_metadata, fields='id').execute()
        folder_id = create_folder.get('id', [])

        for name in files:
            file_metadata = {'name': name, 'parents': [folder_id]}
            media = MediaFileUpload(
                os.path.join(root, name),
                mimetype=mimetypes.MimeTypes().guess_type(name)[0])
            service.files().create(body=file_metadata,
                                   media_body=media,
                                   fields='id').execute()

        parents_id[last_dir] = folder_id

    return parents_id


#return_recent_files()
get_drive_info()
#folder_upload(service)
