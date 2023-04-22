from hdfs import InsecureClient
import config.settings as settings

class HdfsClient:
    def __init__(self):
        self.client = InsecureClient( settings.HDFS_URL, user = settings.HDFS_USER)
        if self.check_hdfs_connection() is True:
            print("HDFS Detected and Connected")
        else:
            print("HDFS Not Connected")

    def check_hdfs_connection(self):
        # Check if hdfs is connected if it can print contents
        try:
            hdfs_files = self.client.list("/")
            if len(hdfs_files) > 0:
                print("HDFS Connection Successfull")
                return True
            else:
                print("HDFS Connection UnSuccessfull")
                return False
        except Exception as e:
            print("Cannot Establish HDFS connection:",e)
    
    def list_directory(self,dir):
        try:
            print("Listing Directory")
            if self.check_hdfs_connection() is True:
                dir_files = self.client.list(dir)
                return dir_files
            else:
                print("Cannot List Directory Since HDFS is not Detected")
                raise ValueError("HDFSNotFoundError")
        except Exception as e:
            print("Failed to list HDFS directory:",e)
    def check_directory_if_exists(self,file_dir):
        try:
            if self.check_hdfs_connection() is True:
                hdfs_status = self.client.status(file_dir,strict= False)
                if hdfs_status is None:
                    print("HDFS Path does not exist")
                    return False
                else:
                    print("HDFS Path Exists")
                    return True
            else:
                print("Cannot List Directory Since HDFS is not Detected")
                return False
                raise ValueError("HDFSNotFoundError")
        except Exception as e:
            print("Failed to check if directory exists:",e)
        
    def create_hdfs_directory(self,file_dir):
        try:
            if self.check_directory_if_exists(file_dir) is True:
                print("Directory Already Exists")
                return True

            print("Creating Directory...")
            dir_to_create = self.client.makedirs(file_dir)
            if self.check_directory_if_exists(file_dir) is True:
                print("Directory Created Successfully")
                return True
            else:
                print("Directory Has Not been created Successfully")
                return False
        except Exception as e:
            print("Directory Failed to create:",e)
            return False

    
        