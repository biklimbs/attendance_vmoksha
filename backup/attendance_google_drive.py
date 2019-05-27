from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import sys
sys.path.append('/usr/local/lib/python3.5/dist-packages')
from termcolor import colored
import warnings
warnings.filterwarnings("ignore")
from logger_config import *
import logger_config

PARENT_DIR_ID="1vDLlmfhEDyJXRiV6EeQ0EfF6qI_TpkiK"
ATTENDANCE_FILE_DOWNLOAD="attendance_file/"

#---Configuring log filename---
log_file=os.path.splitext(os.path.basename(__file__))[0]+".log"
log = logger_config.configure_logger('default', ""+DIR+""+LOG_DIR+"/"+log_file+"")



#---Returns google drive object---
def get_drive_obj():
	"""
	It authenticates the client using client id and key,
	creates a drive object using authntication
	"""
	try:
		gauth = GoogleAuth()
		gauth.LocalWebserverAuth()
		drive = GoogleDrive(gauth)
		return drive,True
	except Exception as e:
		log.error(colored(str(e),"red"))
		return None,False

#---List all the contents(files,dir) of given dir---
def list_drive_dir(drive):
	file_list = drive.ListFile({'q': "'%s' in parents and trashed=false" % PARENT_DIR_ID}).GetList()
	if len(file_list)==0:
		log.error(colored("file is not present in this folder","red"))
	else:
		log.info(colored("File Present in google drive:","green"))
		for file1 in file_list:
			log.info(colored(str(file1['title']),"green"))
	return file_list

#---Downloads file present in specified folder---
def download_file(file_list):
	for file1 in file_list:
		file1.GetContentFile(ATTENDANCE_FILE_DOWNLOAD+"attendance.xls")
		log.info(colored("Downloaded: "+str(file1['title']),"cyan"))

#---delete all files---
def delete_all_files(file_list):
	for file1 in file_list:
		log.info(colored("Deleted: "+str(file1['title']),"yellow"))
		file1.Delete()

#---It gets file from google drive and save it in specified folder---
def download_file_from_drive():
	drive,status=get_drive_obj()
	if status:
		file_list=list_drive_dir(drive)
		if len(file_list)==0:
			log.error(colored("Folder is empty","red"))
			download_status=False
		else:
			download_file(file_list)
			delete_all_files(file_list)
			download_status=True
	else:
		download_status=False
	return download_status

#---Main function---
def main():
	download_status=download_file_from_drive()
	if download_status:
		print("file downloaded")
	else:
		print("file not downloaded")
    
#---Main function called---
if __name__=="__main__":
	main()
