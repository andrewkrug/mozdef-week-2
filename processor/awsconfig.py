import boto3
import json
import logging

from datetime import datetime
from os import getenv


def convert_my_iso_8601(iso_8601):
    assert iso_8601[-1] == 'Z'
    iso_8601 = iso_8601[:-1] + '000'
    iso_8601_dt = datetime.strptime(iso_8601, '%Y-%m-%dT%H:%M:%S.%f')
    return str(iso_8601_dt)

def _get_resource_info(cloudwatch_event):
    try:
        hostname = cloudwatch_event['detail']['requestParameters']['evaluations'][0]['complianceResourceId']
    except KeyError:
        hostname = 'AWSConfig'
    return hostname

def _flatten_detail(cloudwatch_event, mozdef_event):
    try:
        for k in cloudwatch_event['detail']['requestParameters']['evaluations'][0]:
            field_key = f'details.requestParameters.evaluations.{k}'            
            mozdef_event[field_key] = cloudwatch_event['detail']['requestParameters']['evaluations'][0][k]
    except KeyError as e:
        print(e)
    
    return mozdef_event

def lambda_handler(event, context={}):
    client = boto3.client('sqs')

    normalized_records = []
    for record in event['Records']:
        try:
            sns_message = json.loads(record['Sns']['Message'])

            mozdef_event = {
                'timestamp': convert_my_iso_8601(record['Sns'].get('Timestamp')),
                'hostname': _get_resource_info(sns_message),
                'processname': 'aws.config',
                'processid': 1337,
                'severity': 'INFO',
                'summary': sns_message['detail'].get('eventName', "UNKNOWN"),
                'category': sns_message['detail']['requestParameters']['evaluations'][0]['complianceResourceType'],
                'source': 'aws.config',
                'tags': [
                    sns_message['detail']['eventName']
                ],
                'details': sns_message.get('detail')
            }

            mozdef_event = _flatten_detail(sns_message, mozdef_event)

            entry = dict(
                Id=record['Sns']['MessageId'],
                MessageBody=json.dumps(mozdef_event)
            )
            normalized_records.append(entry)
            if len(normalized_records) == 10:
                print('sending batch')
                response = client.send_message_batch(
                    QueueUrl=getenv('SQS_URL'),
                    Entries=normalized_records)
                # TODO : Process the response and do something about failures
                del normalized_records[:]
        except Exception as e:
            print(e)
            return 200
        if len(normalized_records) > 0:
            print('sending final batch')
            response = client.send_message_batch(
                QueueUrl=getenv('SQS_URL'),
                Entries=normalized_records)
        return normalized_records

