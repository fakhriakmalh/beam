import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
from apache_beam import window


# service account path yang didapat saat pengaturan service accounts di IAM & ADMIN google cloud
service_account_path = '/home/ffkhr/Downloads/data_stream/bymyself-284004-cc6ecb0a7e38.json'

print("Service account file : ", service_account_path)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path

# input subscription id
input_subscription = 'projects/bymyself-284004/subscriptions/subscriber1'

# output subscription id, pada topic baru di Pub Sub
output_topic = 'projects/bymyself-284004/topics/stream2'

options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

p = beam.Pipeline(options=options)



pubsub_data= (
                p 
                | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription= input_subscription)
                | 'Write to pub sub' >> beam.io.WriteToPubSub(output_topic)
	             )

result = p.run()
result.wait_until_finish()