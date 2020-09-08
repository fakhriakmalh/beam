from google.cloud import pubsub_v1
import time 
import os 

if __name__ == "__main__" : 
    #service account path yang didapat saat pengaturan service accounts di IAM & ADMIN google cloud
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/home/ffkhr/Downloads/data_stream/bymyself-284004-cc6ecb0a7e38.json'
    
    #subscription id 
    subscription_path = 'projects/bymyself-284004/subscriptions/subscriber1'
    subscriber = pubsub_v1.SubscriberClient()
    
    def callback(message) : 
        print(('Received message : {}'.format(message)))
        message.ack()
    
    subscriber.subscribe(subscription_path, callback = callback)
    
    while True : 
        time.sleep(60)