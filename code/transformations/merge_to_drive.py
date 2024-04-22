from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

def authenticate():
    gauth = GoogleAuth()
    gauth.LoadClientConfigFile("pyDrive/client_secrets.json")
    gauth.LocalWebserverAuth()
    return gauth

def upload_to_drive(file_path, folder_id='1zaubLuFwaNfrGQHC5B8Zpn1SPi3IMpXB'):
    gauth = authenticate()
    drive = GoogleDrive(gauth)

    file_name = file_path.split('/')[-1]
    file = drive.CreateFile({'title': file_name, 'parents': [{'id': folder_id}] if folder_id else []})
    file.SetContentFile(file_path)
    file.Upload()
    print(f"Archivo '{file_name}' subido correctamente a Google Drive.")

file_path = "data/merged_data.csv"
upload_to_drive(file_path)