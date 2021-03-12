import json
import boto3

s3 = boto3.client('s3')
client_cm = boto3.client(service_name='comprehendmedical')

def lambda_handler(event, context):

    # This gets triggered from when txt obj file is added to S3 bucket.
    # It extracts the data and analyzes through comprehend medical.
    # Then it writes these results to S3 as another txt file
    
    obj=event['Records'][0]['s3']['object']['key']
    bucket=event['Records'][0]['s3']['bucket']['name']
    try:
        fileobj = s3.get_object(Bucket=bucket, Key=obj)
        filedata = fileobj['Body'].read()
        content = filedata.decode('utf-8')
        detectedEntities = ''        
        result = client_cm.detect_entities(Text=content)        
        entities = result['Entities']
        for entity in entities:
            detectedEntities += 'Entity '+ str(entity) + '\n'

        file_to_create = "cm-out/" + obj + ".cm-result.txt"
        s3.put_object(Body=detectedEntities, Bucket=bucket, Key=file_to_create)

    except Exception as e:
        print("Failed to get Procedure results")
        print(e.message)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Success!')
    }
