import json
import boto3 
import ntpath

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Data_Store')
def lambda_handler(event, context):
    try:
        # TODO: write code...
        bucket = event['Records'][0]['s3']['bucket']['name']
        file = event['Records'][0]['s3']['object']['key']
        #file_name = ntpath.basename(file)
        #print(file_name)
        # print(bucket)
        #print(file)
        file_type_name = file.split("/")
        file_type = file_type_name[0]#Stock or ETF
        file_name_list = file_type_name[1].split(".")
        file_name = file_name_list[0]
        
        
        
        # TODO implement
        json_object = s3_client.get_object(Bucket=bucket, Key=file)
   
        data = json_object['Body'].read().decode("utf-8")
        stockdata = data.split("\n")
        stockdata.pop(0)
        print(len(stockdata))
        
        try:
            # TODO: write code...
            for std in stockdata:
                #print(">>>",std)
                std_data = std.split(",")
                print(std_data)
                table.put_item(
                Item = {
                   
                    
                    "name" : file_name,
                    "date" : str(std_data[0]),
                    "type" : file_type,
                    "open" : std_data[1],
                    "high" : std_data[2],
                    "low" : std_data[3],
                    "close": std_data[4],
                    "volume": std_data[5],
                    "openInt": std_data[6]
                    }
                )
            
        except Exception as ex:
            print(ex) 
    except Exception as e:
        print(e)
    
    
    
    
    #     # try:
    #     
        # except Exception as e:
        #     print("End of file")
    # dict = json.loads(data)
   
    # table.put_item(dict)
    #print(data)
    # print(str(event))
    #return {
    #    'statusCode': 200,
     #  'body': json.dumps('Hello from Lambda!')
    
