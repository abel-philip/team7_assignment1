import boto3
import os 

from botocore.exceptions import NoCredentialsError



#ACCESS_KEY = 'AKIA2ZIJVZ3G3AZ4NEXM'
#SECRET_KEY = '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'


def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3')

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print(s3_file)
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False    


#to create array of file names
dir = 'D:/Parab_Ash/MIS/BD_pipeline/Assigntment1/archive/Data'

for i in os.listdir(dir+'/ETFs'): 
    if i.endswith('.txt'): 
        uploaded = upload_to_aws(os.path.join( dir+'/ETFs', i ), 'assignment1team7', 'etf/'+i) 
        
for i in os.listdir(dir+'/Stocks'): 
    if i.endswith('.txt'): 
        uploaded = upload_to_aws(os.path.join( dir+'/Stocks', i ), 'assignment1team7', 'stock/'+i) 

 
print("Upload Successful") 
           

    
