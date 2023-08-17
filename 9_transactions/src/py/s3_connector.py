import boto3
import traceback

class S3LoadFiles:
    def __init__(self, 
                 service_name: str,
                 endpoint_url: str,
                 aws_access_key_id: str, 
                 aws_secret_access_key: str,
                 region_name = None,
                 api_version = None,
                 use_ssl = True,
                 verify = None,
                 config = None) -> None:
        
        self.service_name = service_name
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.api_version = api_version
        self.use_ssl = use_ssl
        self.verify = verify
        self.config = config

        self.session = boto3.session.Session()


    def get_name_files(self,
                       bucket: str):
 
        try:
            self.s3_resource = self.session.resource(service_name = self.service_name,
                                             endpoint_url= self.endpoint_url,
                                             aws_access_key_id = self.aws_access_key_id,
                                             aws_secret_access_key = self.aws_secret_access_key,
                                             region_name = self.region_name,
                                             api_version = self.api_version,
                                             use_ssl = self.use_ssl,
                                             verify = self.verify,
                                             config = self.config) 
            
                
            bucket = self.s3_resource.Bucket(bucket)
        
            name_files = [str(key.key) for key in bucket.objects.all()]
            
        except Exception:
            traceback.print_exc()
            return []

        return name_files
            

    def download_files(self, 
                       bucket: str,
                       key: str,
                       filename: str):
        
        try:
            self.s3_client = self.session.client(service_name = self.service_name,
                                             endpoint_url= self.endpoint_url,
                                             aws_access_key_id = self.aws_access_key_id,
                                             aws_secret_access_key = self.aws_secret_access_key,
                                             region_name = self.region_name,
                                             api_version = self.api_version,
                                             use_ssl = self.use_ssl,
                                             verify = self.verify,
                                             config = self.config)
        
            
            self.s3_client.download_file(
                            Bucket=bucket,
                            Key=key,
                            Filename=f'{filename}{key}')

        except Exception:
            traceback.print_exc()
            

