import os
import time 
from google.cloud import pubsub_v1

if __name__ == "__main__":

    # project id di google cloud
    project = 'bymyself'

    # pubsub topic
    pubsub_topic = 'projects/bymyself-284004/topics/stream1'

    # service account path yang didapat saat pengaturan service accounts di IAM & ADMIN google cloud
    path_service_account = '/home/ffkhr/Downloads/data_stream/bymyself-284004-cc6ecb0a7e38.json'
	
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account    

    # file path csv yang akan disimulasikan untuk di streaming
    input_file = '/home/ffkhr/Downloads/data_stream/store_sales.csv'

    # publisher
    publisher = pubsub_v1.PublisherClient()

    with open(input_file, 'rb') as ifp:
        # skip header
        header = ifp.readline()  
        
        # loop over each record
        for line in ifp:
            event_data = line   # entire line of input CSV is the message
            print('Publishing {0} to {1}'.format(event_data, pubsub_topic))
            publisher.publish(pubsub_topic, event_data)
            time.sleep(1)    