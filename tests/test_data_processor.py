import boto3
import json
import os
from moto import mock_sqs


@mock_sqs
class TestDataProcessor(object):
    def setup(self):
        fh = open('tests/sample-event.json')
        self.sample_event = fh.read()
        fh.close()

        sqs = boto3.client('sqs')
        response = sqs.create_queue(
            QueueName='fake-queue'
        )

        self.queue_url = response.get('QueueUrl')
    
    def test_json_loads(self):
        assert json.loads(self.sample_event) is not None

    def test_transform_to_mozdef(self):
        os.environ['SQS_URL'] = self.queue_url
        from processor.awsconfig import lambda_handler

        result = lambda_handler(json.loads(self.sample_event), {})

        mozdef_event = json.loads(result[0]['MessageBody'])

        assert mozdef_event['timestamp'] is not None
        assert mozdef_event['details'] is not None
        assert mozdef_event['source'] == 'aws.config'
        assert mozdef_event['severity'] == 'INFO'
        assert mozdef_event['processid'] == 1337
