import boto3, botocore
import smart_open.s3 as sos3
#import smart_open
import csv
import gzip
import io

from typing import List, Dict, Tuple

import requests #for http querying
import pathlib

try:
    import so7z
    success_so7z = True
except ImportError as e:
    print("Warning: Failed to import so7z, so ez_aws will not load the functions that depend on it.")
    print("Note that, for unknown reasons, so7z will not import properly in IPython/Jupyter even when it loads properly in the interpreter.")
    print("The rest of ez_aws will continue to work as normal")
    print("Official error message below:\n")
    print(e)
    success_so7z = False


class AWS:
    """Wrapper class for boto3 that simplifies a lot of processes we have to do repeatedly"""

    def __init__(self, *, session : boto3.session = None, 
                access_key :str = None, secret_key :str = None, region: str = None, 
                credential_file_path: str = None,
                cache_clients = False):
        """Constructor provides multiple methods for establishing the boto3 session that will be re-used repeatedly:
        1. If none of these parameters are entered, it will use session.get_credentials() by default
        Then, in order of precedence, will use the following
            specific boto3 session
            access key + secret_key
            credential_file_path pointing to a .csv file with the necessary credentials"""

        if session != None:
            self.session=session
        elif access_key != None and secret_key != None:
            self.session = boto3.Session(
                aws_access_key_id = access_key,
                aws_secret_access_key=secret_key,
                region_name = region
            )
        elif credential_file_path != None:
            creds = csv.DictReader(open(credential_file_path))
            for row in creds:
                self.session = boto3.Session(
                    aws_access_key_id=row['Access key ID'],
                    aws_secret_access_key=row['Secret access key'],
                    region_name = region
                )
        else: 
            self.session = boto3.Session(region_name = region)
        
        #establishing region, in case it isn't established yet (such as for EC2 instance)
        if self.session.region_name ==None:
            try:
                response = requests.get('http://169.254.169.254/latest/meta-data/placement/region') #gets region from within EC2 instnace
                if response.status_code == 200:  #successful
                    cur_instance_region = response.text
                    self.session = boto3.Session(region_name = cur_instance_region)
            except:
                print("No region specified by user, and could not pull from EC2 instance profile")
            
        #caching the various clients and resources for speed later on
        if cache_clients:
            self.s3_client = self.session.client('s3')
            self.s3_resources = self.session.resource('s3')
            self.iam_client = self.session.client('iam')
            self.ec2_client = self.session.client('ec2')
            self.ec2_resources = self.session.resource('ec2')
        else:
            self.s3_client = None
            self.s3_resources = None
            self.iam_client=None
            self.ec2_client=None
            self.ec2_resources= None

    def is_ec2_instance(self) -> bool:
        """returns true if this program is being run from within an EC2 instance"""
        try:
            response = requests.get('http://169.254.169.254/latest/meta-data/placement/region')
            #print(response.text) #see what the result is first
            return response.status_code ==200 #successful response, will fail if not from an ec2 instance
        except:
            return False

    def get_cur_user(self):
        """returns ARN of the user of this current session"""
        return self.session.client('sts').get_caller_identity()['Arn']

    def get_bucket_request_payment(self, bucket_name: str, bucket_owner_account_id : str = None) -> str:
        """returns one of the following:
        1. 'Requester': if the requester must pay
        2. 'BucketOwner': if the bucket owner must pay
        3. 'AccessDenied': if you do not have necessary permission to get the bucket_request_payment configuration"""

        if self.s3_client==None:
            self.s3_client = self.session.client('s3')
        
        #add try /except with proper error when we see it
        try:
            if bucket_owner_account_id == None:
                response = self.s3_client.get_bucket_request_payment(Bucket=bucket_name)
            else:
                response = self.s3_client.get_bucket_request_payment(Bucket=bucket_name, ExpectedBucketOwner =bucket_owner_account_id)
            return response['Payer']
        except self.s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code']=='AccessDenied':
                return 'AccessDenied'
            else:
                raise e
            
        
        
    
    def get_bucket_size(self, bucket_name : str,
        in_gb = False, in_tb = False, print_progress = False,
        requester_pays = False) -> int :
        """ returns total size (in bytes) of bucket"""
        total_file_size = 0
        if self.s3_client==None:
            self.s3_client = self.session.client('s3')

        if requester_pays:
            all_objects = self.s3_client.list_objects_v2(Bucket = bucket_name, RequestPayer='requester')
        else:
            all_objects = self.s3_client.list_objects_v2(Bucket = bucket_name)                        
        for obj in all_objects['Contents']:
            total_file_size+=obj['Size']
        
        group = 1 
        while all_objects['IsTruncated']:
            if print_progress:
                print("List of objects truncated for get_bucket_size, breaking into groups. This is group: " + str(group))
            group = group+1
            nextToken = all_objects['NextContinuationToken']
            if requester_pays:
                all_objects = self.s3_client.list_objects_v2(Bucket = bucket_name, ContinuationToken= nextToken, RequestPayer='requester')
            else:
                all_objects = self.s3_client.list_objects_v2(Bucket = bucket_name, ContinuationToken= nextToken)
            for obj in all_objects['Contents']:
                total_file_size+=obj['Size']

        if in_gb:
            return total_file_size / 1.0E9
        if in_tb:
            return total_file_size / 1.0E12
        return total_file_size #default is in bytes
    
    def list_bucket_names(self, only_accessible = False) -> List[str]:
        """outputs a sorted list of bucket names.

        If only_accessible is true, it will only list the buckets for which you have ListBucket access"""
        if self.s3_client==None:
            self.s3_client = self.session.client('s3')
        response = self.s3_client.list_buckets() #never truncated according to boto3
        bucket_dict_list = response['Buckets']
        bucket_name_list = [x['Name'] for x in bucket_dict_list]
        
        if only_accessible: #check which buckets you have access to
            accessible_buckets = []
            for bucket in bucket_name_list:
                if self.can_access_bucket(bucket):
                    accessible_buckets.append(bucket)
            return sorted(accessible_buckets)
        else:
            return sorted(bucket_name_list)
    

    def can_access_bucket(self,bucket: str) -> bool:
        """returns true only if the bucket exists and you have permission to access it
        Note that this will return false if the bucket has requester pays access becuase the head_object function doesn't have a requester_pays options"""
        if self.s3_client==None:
            self.s3_client = self.session.client('s3')
        try:
            response = self.s3_client.head_bucket(Bucket=bucket)
            result_code = response['ResponseMetadata']['HTTPStatusCode']
            return result_code ==200
            #200 = "if the bucket exists and you have permission to access it"
            #404 Not found - bucket doesn't exist
            #403 Forbiden - no permission
        except:
            return False #means either bucket doesn't exist or you don't have permission
    
    def list_keys(self, bucket_name: str, prefix : str = "", requester_pays=False) -> List[str]:
        """returns a list of all file keys in the bucket"""
        filenames = []
        if self.s3_client==None:
            self.s3_client = self.session.client('s3')

        if requester_pays:
            all_objects = self.s3_client.list_objects_v2(Bucket = bucket_name,
                                                    Prefix=prefix,
                                                    MaxKeys=1000,
                                                    RequestPayer='requester')
        else:
            all_objects = self.s3_client.list_objects_v2(Bucket = bucket_name,
                                                    Prefix=prefix,
                                                    MaxKeys=1000)   
        try:
            for obj in all_objects['Contents']:
                filenames.append(obj['Key'])
        except KeyError:
            print("all_objects in list_keys did not have a 'contents' key, returning empty list")
            print(all_objects)
            return []
        
        group = 1 
        while all_objects['IsTruncated']:
            #print("group: " + str(group))
            group = group+1
            nextToken = all_objects['NextContinuationToken']
            if requester_pays:
                all_objects = self.s3_client.list_objects_v2(Bucket = bucket_name,
                                                    Prefix=prefix,
                                                    MaxKeys=1000,
                                                    ContinuationToken= nextToken,
                                                    RequestPayer='requester')
            else:
                all_objects = self.s3_client.list_objects_v2(Bucket = bucket_name,
                                                    Prefix=prefix,
                                                    MaxKeys=1000,
                                                    ContinuationToken= nextToken)
            for obj in all_objects['Contents']:
                filenames.append(obj['Key'])
        return filenames
        
    def get_file_index(self, bucket_name : str, prefix : str = "", fout = None, get_column_names=False, print_progress=False) -> List[Dict]:
        """returns a list with one element for each file in the S3 bucket (with matching prefix)
        Each element is a dictionary with at the very least the following keys: 
        1. 'key'
        2. 'sizebyte'
        3. 'sizegigabyte'

        If get_column_names=True, then each dictionary will also contain a  'column names' key, and its value is a comma-separated list of the column names.

        WARNING: GETTING THE COLUMN NAMES MAY TRANSFER A LOT OF DATA IF THERE ARE MANY SMALL FILES.

        if fout != None, then it writes the index to that file. This is particularly useful if you are creating an enormous index
        And you want to write to the file as you index it in case you hit an error along the way

        fout should be an opened file in write mode"""

        if self.s3_client==None:
            self.s3_client = self.session.client('s3')

        if print_progress:
            print("Beginning file index for bucket=", bucket_name, " and prefix=", prefix)

        index = []
        
        #establishing default values for first run of the while loop
        group =1
        first : bool = True
        all_objects = {'IsTruncated' : True}

        #handling csv writer if necessary
        fieldnames = ['key','sizebyte', 'sizegigabyte']
        if get_column_names:
            fieldnames.append('column names')
        if fout!= None:
            writer = csv.DictWriter(fout, fieldnames)
            writer.writeheader()

        #looping repeatedly until you have all the files
        while all_objects['IsTruncated']:
            if print_progress:
                print("group: " + str(group))
            group+=1
            
            if first:
                all_objects = self.s3_client.list_objects_v2(Bucket = bucket_name,
                                                    Prefix=prefix,
                                                    MaxKeys=1000)
                first = False

            else:
                nextToken = all_objects['NextContinuationToken']
                all_objects = self.s3_client.list_objects_v2(Bucket = bucket_name,
                                                    Prefix=prefix,
                                                    MaxKeys=1000,
                                                    ContinuationToken= nextToken)
            for obj in all_objects['Contents']:
                if print_progress:
                    print(obj['Key'])
                row = self.get_obj_index(bucket_name, obj, get_column_names=get_column_names)
                index.append(row)
                if fout != None:
                    writer.writerow(row)
      
        return index

    def get_obj_index(self, bucket_name, obj, get_column_names=False) -> Dict:
        result = {'key':obj['Key'], 'sizebyte' :obj['Size'], 'sizegigabyte' : obj['Size']/ 1.0E9}

        if get_column_names:
            if obj['Size']==0: #if no data, then no column names
                result['column names'] = "none"
            else:
                column_names = self.get_column_names(bucket_name, obj['Key'])
                column_string = ""
                for column in column_names:
                    column_string += column + ','

                column_string = column_string[:-1] #remove the last comma
                result['column names'] = column_string
        return result

    def get_key_index(self, bucket: str, key: str, get_column_names=False) -> Dict:
        """ returns a dictionary with following keys:
        1. 'key'
        2. 'sizebyte'
        3. 'sizegigabyte'
        4. (optional if get_column_names=True) 'column names'"""

        if self.s3_client==None:
            self.s3_client = self.session.client('s3')

        header = self.s3_client.head_object(
            Bucket=bucket,
            Key = key
        )
        
        size_bytes = int(header['ResponseMetadata']['HTTPHeaders']['content-length'])
        result = {'key': key, 'sizebyte': size_bytes, 'sizegigabyte' : size_bytes / 1.0E9}
        
        if get_column_names:
            column_names = self.get_column_names(bucket, key)
            column_string = ""
            for column in column_names:
                column_string += column + ','

            column_string = column_string[:-1] #remove the last comma
            result['column names'] = column_string
        
        return result



   
        
    def get_column_names(self, bucket_name, key)-> List[str]:
        """returns a list of the column names of each file.
        first step is to get the extension
        For now, only .csv and .csv.gz are implemented"""

        split_key = key.split('.')
        try:
            extension = split_key[-1]
        except IndexError:
            extension =""
        
        try:
            sub_extension = split_key[-2] #e.g. in a .csv.gz file, sub-extension is .csv
        except IndexError:
            sub_extension=""

        if extension == 'csv' or (extension=='gz' and sub_extension=='csv'): #can read it directly with smart open
            try:
                with self.open(bucket_name, key) as fin:
                    reader = csv.DictReader(fin)
                    return reader.fieldnames
            except Exception as e:
                return [f"unreadable csv: {str(e)}"]
        else:
            #print("getColumNames Error: could not read extension of type ", extension, " with sub-extension ", sub_extension)
            return ["unreadable non csv file"]


    def download(self, bucket_name : str , key : str, save_location : str, requester_pays=False) -> None:
        """downloads from from S3 to local computer. 
        save_location should be full path, including filename and extension
        
        Necessarily incurs data transfer out charges"""
        print(f"save_location: {save_location}")
        #check to make sure the folder exists locally
        outpath = pathlib.Path(save_location)
        outdirectory = outpath.parent
        print(f"outdirectory: {outdirectory}")
        if not outdirectory.exists():
            print("Specified directory ", outdirectory.name, " does not exist yet.")
            print("Creating ", outdirectory.name)
            outdirectory.mkdir(parents=True, exist_ok=True)

        #download file
        if self.s3_resources == None:
            self.s3_resources = self.session.resource('s3')
        bucket = self.s3_resources.Bucket(bucket_name)
        extra_args = {}
        if requester_pays:
             extra_args['RequestPayer']='requester'
        response = bucket.download_file(key,save_location, ExtraArgs = extra_args)
        response = bucket.download_file(key,save_location)
        print(response)

    def upload(self, bucket_name : str, key: str, local_file_location: str)-> None:
        """uploads file from local computer to the bucket, with specified key"""
        if self.s3_resources == None:
            self.s3_resources = self.session.resource('s3') 
        bucket = self.s3_resources.Bucket(bucket_name)
        bucket.upload_file(local_file_location,key)

    def copy_within_aws(self, source_bucket_name : str , dest_bucket_name :str , source_file_key: str, dest_file_key = None)-> None:
        """copies file_key from source_bucket to dest_bucket within s3 
        (so never downloaded locally)"""
        if dest_file_key == None:
            dest_file_key = source_file_key

        if self.s3_resources == None:
            self.s3_resources = self.session.resource('s3')

        dest_bucket = self.s3_resources.Bucket(dest_bucket_name)
        source = {'Bucket': source_bucket_name, 'Key': source_file_key}
        dest_bucket.copy(
            source,
            dest_file_key,
            ExtraArgs={'ACL':'bucket-owner-full-control'} #this extra arg assures that the destination bucket owner will have full control 
        )
    

    def open(self, bucket_name: str, key: str, requester_pays=False, mode='rb', decompress=False, encoding=None):
        
        supported_modes = ['rb', 'wb', 'r', 'w']
        if mode not in supported_modes:
            raise ValueError("AWS.open() only accepts the following modes: ", supported_modes)

        #add necessary keywords for requester pays
        object_kwargs = {}
        if requester_pays:
            object_kwargs={'RequestPayer': 'requester'}

        binary_mode = mode[0] + 'b'
        #open the binary version
        result = sos3.open(
            bucket_id = bucket_name,
            key_id = key,
            session = self.session,
            mode=binary_mode,
            object_kwargs = object_kwargs
        )

        if mode in ['r', 'w']:
            if encoding==None:
                print("set encoding to utf-8 because user specified 'r' or 'w' mode but not encoding type")
                encoding='utf-8'

        #decompress
        if decompress:
            extension, sub_extension = self._get_extensions(key)
            if extension== 'gz':
                if encoding!= None:
                    gzip_mode = mode[0] + 't' #text mdoe
                else:
                    gzip_mode = mode[0] #binary for gzip
                result = gzip.open(result, mode=gzip_mode, encoding = encoding)
            elif extension == '7z':
                raise NotImplementedError("Have not implemented 7z decompress in ez_aws.open yet")
            elif extension in ['txt', 'csv']:
                print(f"user requseted to decompress file {key}, but decompress is not necessary for file with extension {extension}")
            else:
                raise NotImplementedError("ez_aws library only decompresses/compresses gz files currently")
        else: #no need to decompress
           if encoding!= None: #transform into decoded stream if you want
               print("Converting to textio wrapper with encoding =", encoding)
               result = io.TextIOWrapper(
                   buffer = result,
                   encoding=encoding,
               )
        return result
       

    def _get_extensions(self, filename: str)-> Tuple[str,str]:
        """returns extension, sub_extension of a file.
        E.g. if a file is "Hello.csv.gz", the extension would be .gz and the sub-extension would be .csv"""
        split_key = filename.split('.')
        try:
            extension = split_key[-1]
        except IndexError:
            extension =""
        
        try:
            sub_extension = split_key[-2] #e.g. in a .csv.gz file, sub-extension is .csv
        except IndexError:
            sub_extension=""

        return extension, sub_extension

    if success_so7z:
        def get_7zip_archive(self, bucket_name: str, key: str, password : str = None, requester_pays=False) -> so7z.SmartOpen7z:
            """Returns a SmartOpen7z object (an archive), given parameters.
            If it is password protected, then you must enter a password to decrypt it."""

            file = self.open(
                    bucket_name =bucket_name,
                    key=key,
                    requester_pays=requester_pays,
                    mode='rb'
            )

            return so7z.SmartOpen7z(
                file=file,
                password=password
            )

