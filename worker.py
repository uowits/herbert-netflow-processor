"""
Throttler main worker
"""
from pymongo import MongoClient
from pika import spec
import pika
import json
import logging
import sys
import time
import datetime
from datetime import timedelta
import os
# import pprint

db = None #Mongo Database
user_cache = None
logger = None

#Traffic counters
user_counter = {}
global_counter = {}

WRITE_AFTER_SECONDS = 10 #Number of seconds to flush the local write cache
WRITE_AFTER_RECORDS = 3000 #OR Number of records to process before flushing cache.  Whatever comes first.
last_time_write = datetime.datetime.now()
records_written = 0

def resetCounters():
    """
    Resets the user and global counters
    """
    global global_counter
    global user_counter
    global last_time_write
    global records_written
    last_time_write = datetime.datetime.now()
    records_written = 0
    global_counter = {}
    user_counter = {}
    return

def userRegistration(ch, method, properties, body):
    """
    Hooks into the user registration exchange to update the local cache
    """
    global db
    global logger
    global user_cache
    
    request = json.loads(body)
    user_cache[request['ip_address']] = request['username']
    
    logger.debug("Updating user auth cache %s with %s" % (request['username'], request['ip_address']))
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

def flushCaches(request):
    """
    Flushes the local cache and writes it to the database
    
    Accepts the last request
    """    
    global global_counter
    global user_counter
    # pprint.pprint(global_counter)
    # pprint.pprint(user_counter)
    
    #Setup some date information for how we want to update the counters
    date = datetime.datetime.strptime(request['timestamp_start'][:10], "%Y-%m-%d")
    year, week, dow = date.isocalendar()
    week_start_date = None
    if dow == 7:
        # Since we want to start with Sunday, let's test for that condition.
        week_start_date = date
    else:
        # Otherwise, subtract `dow` number days to get the first day
        week_start_date = date - timedelta(dow)
    month_start_date = datetime.datetime.strptime(request['timestamp_start'][:7], "%Y-%m")
    year_start_date = datetime.datetime.strptime(request['timestamp_start'][:4], "%Y")
    
    for user in user_counter:
        increment_dict = {}
        for community in user_counter[user]:
            increment_dict['communities.%s' % community] = user_counter[user][community]
        
        #Update the counters
        #ToDo:  Turn these into bulk updates
        db.user_yearly_totals.update({
                'username': user,
                'date': year_start_date
            },
            {
                "$inc": increment_dict
            },
            upsert=True
        )

        db.user_monthly_totals.update({
                'username': user,
                'date': month_start_date
            },
            {
                "$inc": increment_dict
            },
            upsert=True
        )

        db.user_weekly_totals.update({
                'username': user,
                'date': week_start_date
            },
            {
                "$inc": increment_dict
            },
            upsert=True
        )

        db.user_daily_totals.update({
                'username': user,
                'date': date,
            },
            {
                '$inc': increment_dict
            },
            upsert=True
        )
    
    #-- Update the global counters
    increment_dict = {}
    for community in global_counter:
        increment_dict['communities.%s' % community] = global_counter[community]
    
    db.yearly_totals.update({
            'date': year_start_date,
        },
        {
            '$inc': increment_dict
        },
        upsert=True
    )

    db.monthly_totals.update({
            'date': month_start_date,
        },
        {
            '$inc': increment_dict
        },
        upsert=True
    )

    db.weekly_totals.update({
            'date': week_start_date,
        },
        {
            '$inc': increment_dict
        },
        upsert=True
    )

    db.daily_totals.update({
            'date': date,
        },
        {
            '$inc': increment_dict
        },
        upsert=True
    )

    


def processNetflow(ch, method, properties, body):
    """
    Function processes raw netflow
    """
    global db
    global logger
    global user_cache
    
    #These caches are the global counters
    global user_counter
    global global_counter
    global last_time_write
    global records_written
    
    
    request = json.loads(body)
    
    #Only process Freedom data for now
    if "10.64." not in request['ip_dst']:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    
    #Load up the user who has this destination address
    user = 'UNKNOWN'
    if not request['ip_dst'] in user_cache:
        #the user does not exist in out local cache.  Fetch it from disk
        db_user = db.user_ip.find_one( {'_id': request['ip_dst']})
        if db_user:
            user = db_user['username']
    else:
        user = user_cache[request['ip_dst']]
    
    request['user'] = user
    logger.debug("Processing Netflow %s" % str(request))
    
    db_netflow = db.processed_netflow
    # db_netflow.insert(request)

    if 'src_comms' not in request:
        request['src_comms'] = ""
    community = request['src_comms']
    if community == "":
        community = "UNKNOWN"

    #-- Update the caches
    #Create a new user if one does not already exist
    if user not in user_counter:
        user_counter[user] = {}
    if community not in user_counter[user]:
        user_counter[user][community] = 0L
    if community not in global_counter:
        global_counter[community] = 0L
    
    #Actually update the caches
    user_counter[user][community] += request['bytes']
    global_counter[community] += request['bytes']
    
    records_written += 1
    time_passed = (datetime.datetime.now() - last_time_write).seconds
    # print time_passed
    if (records_written > WRITE_AFTER_RECORDS) or (time_passed > WRITE_AFTER_SECONDS):
        flushCaches(request) #Update the caches.  Pass the last request in here for the date time stuff.
        resetCounters()

    #Ack the processing of this transaction
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main(settings):
    """
    settings:  The setting dictionary
    """
    global db
    global logger
    global user_cache

    logger.debug("Starting main function..")
    
    #Clear our local caches ready for a hard day of work
    user_cache = {}
    resetCounters() 
    
    #Setup the MongoDB Connection
    mongo_client = MongoClient(settings['mongodb_server'], 27017)
    db = mongo_client[settings['mongodb_database']]
    db.authenticate(settings['mongodb_username'], settings['mongodb_password'])
    
    #Create a collection if one does not exist of 2GB to store the Netflow information in
    if not 'processed_netflow' in db.collection_names():
        db.create_collection('processed_netflow', size=2147483648)
    
    #Setup the message queue
    exclusive = False
    durable=True

    credentials = pika.PlainCredentials(settings['amqp_username'], settings['amqp_password'])
    amqp_connection = pika.BlockingConnection(pika.ConnectionParameters(settings['amqp_server'],credentials=credentials))
    amqp_channel = amqp_connection.channel()
    amqp_channel.exchange_declare(exchange=settings['amqp_user_auth_exchange'] ,type='fanout')
    amqp_channel.exchange_declare(exchange=settings['amqp_raw_netflow_exchange'] ,type='fanout')
    amqp_channel.queue_declare(queue=settings['amqp_raw_netflow_queue'], durable=durable, exclusive=exclusive)
    
    #Setup a local queue for updating our user cache
    personal_queue_name = amqp_channel.queue_declare(exclusive=True).method.queue
    
    #Setup the basic consume settings so we don't try and process too much at a time
    amqp_channel.basic_qos(prefetch_count=5)

    #Bind to the queues and start consuming
    amqp_channel.queue_bind(exchange=settings['amqp_raw_netflow_exchange'], queue=settings['amqp_raw_netflow_queue'])
    amqp_channel.queue_bind(exchange=settings['amqp_user_auth_exchange'], queue=personal_queue_name)

    amqp_channel.basic_consume(userRegistration, queue=personal_queue_name)
    amqp_channel.basic_consume(processNetflow, queue=settings['amqp_raw_netflow_queue'])
    
    amqp_channel.start_consuming()
    
if __name__ == "__main__":
    #Load up the settings from disk
    global logger
    logging.basicConfig()
        
    settings = {}
    for setting in open('settings.txt', 'r').read().split('\n'):
        setting = setting.strip()
        if setting == '' or setting[0] in ['!', '#'] or ':' not in setting:
            continue
        key, value = setting.split(":")
        settings[key.strip()] = value.strip()
        
    if 'forks' in settings:
        for i in range(int(settings['forks'])):
            if not os.fork():
                break
    
    
    #If we're in debug/testing.. just run and die
    logger = logging.getLogger('worker')
    if 'mode' in settings and settings['mode'] == 'debug':
        logger.setLevel(logging.DEBUG)
        main(settings)
        sys.exit(0)
    elif 'mode' in settings and settings['mode'] == 'test':
        logger.setLevel(logging.INFO)
        main(settings)
        sys.exit(0)
    else:
        logger.setLevel(logging.INFO)
        
    
    #If we're in production, print out the exception and try and restart the app
    while 1:
        try:
            main(settings)
        except:
            logging.critical("-- ERROR Has occured in Herbert Netflow Processor.  Sleeping and re-running")
            logging.critical(sys.exc_info()[0])
            time.sleep(5)
