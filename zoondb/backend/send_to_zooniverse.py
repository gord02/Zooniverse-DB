
import json
import os
import schedule
import time
from datetime import date
import logging
import argparse
from typing import List, Dict
import numpy as np

import asyncio
import nest_asyncio
nest_asyncio.apply()
import motor.motor_asyncio

from dotenv import load_dotenv
from tqdm import tqdm

# Zooniverse imports
# The new Zooniverse API for supporting user-created projects.
from panoptes_client import Panoptes, Project, SubjectSet, Subject
import magic


# Create a new connection to a single MongoDB instance at host:port.
client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017")
# Connection to a database 
db = client['zooniverseDB']

# gets waterfall plots from database
async def upload_waterfalls_from_db(project_id):
    waterfalls_not_uploaded = []
    total_number_of_waterfalls_not_uploaded = await db.events.count_documents({"subject_id": None, "upload_date": None})
    cursor_filtered = db.events.find({"subject_id": None, "upload_date": None }) 
    if(total_number_of_waterfalls_not_uploaded == 0):
        logging.info(f' No waterfalls to upload')
        return
    for doc in await cursor_filtered.to_list(total_number_of_waterfalls_not_uploaded):
        waterfalls_not_uploaded.append(doc)
    logging.info(f'Waterfalls to upload: {len(waterfalls_not_uploaded)}')

    authenticate()

    upload_waterfalls(waterfalls_not_uploaded, project_id)


def upload_waterfalls(waterfalls, project_id): 
    authenticate()
    project = Project(project_id)
    logging.info(f'Uploading {len(waterfalls)} subjects')

    for waterfall in tqdm(waterfalls):
        task = loop.create_task(uploader(waterfall, project))
        loop.run_until_complete(task)

async def uploader(waterfall, project):
    locations = waterfall['beams']['data_paths'].values()

    id = waterfall['_id']
    # Updates the document in the database to have an upload date
    the_date = str(date.today())
    result = await db.events.update_one({'_id': id}, {'$set': {'upload_date': the_date}}) 

    metadata = get_real_metadata(waterfall)
    subject_id = upload_subject(locations=locations, project=project, metadata=metadata)
    # subject has been uploaded to Panoptes so now database needs to be updated to reflect this change knowing which specifc document was updated
    result = await db.events.update_one({'_id': id}, {'$set': {'subject_id': subject_id}}) 
    result = await db.events.update_one({'_id': id}, {'$set': {'transfer_status': 'COMPLETE'}}) 

def get_real_metadata(waterfall):
    return {
        'event': waterfall['event'],
        'beams': waterfall['beams'],
        'upload_date': date.today().strftime('%Y-%m-%d'),
        '#training_subject': False,
        '#feedback_subject': False
    }


# == Interactions with Panoptes ==

def upload_subject(locations: List, project: Project, metadata: Dict):
    # creation of a subject
    subject = Subject()
    # subject is linked to project 
    subject.links.project = project
    test_plots = ['/Users/gordon/Desktop/zooniverse-db/files_for_zoon/data/frb-archiver/plot2.png', '/Users/gordon/Desktop/zooniverse-db/files_for_zoon/data/frb-archiver/plot3.png'] # testing
    for location in locations:
        i = int(np.random.choice(range(0, 2))) # testing
        if not os.path.isfile(test_plots[i]): # testing
        # if not os.path.isfile(location):
            raise FileNotFoundError('Missing subject location: {}'.format(location))
        # subject.add_location(location)
        subject.add_location(test_plots[i]) #testing
    subject.metadata.update(metadata)

    # gets or creates subject set to push subject to
    subject_set = get_or_create_subject_set(project.id)

    # saving subject
    subject.save()
    # adding subject to subject set
    subject_set.add(subject)
    return subject.id

def authenticate(): 
    load_dotenv()  # take environment variables from .env
    auth_username = os.environ.get("username")
    auth_password = os.environ.get("password")
    Panoptes.connect(username = auth_username, password = auth_password)

def get_or_create_subject_set(project_id):
    name = "gordon_testing_2" # testing
    # name = str(date.today())
    try:
        # subjects sets are differentiated by date, their display names are the date they were created
        # for the date that files are sent over to Zooniverse, first check if the date has already been created
        return next(SubjectSet.where(project_id=project_id, display_name=name))
    except StopIteration:
        logging.info(f"Didn't find subject set {name} - creating it")
        # if the date has not been used creeate the subject set with the today's date as its display name
        return create_subject_set(project_id, name)

def create_subject_set(project_id, name):
    # creating subject set
    subject_set = SubjectSet()
    subject_set.links.project = Project(project_id)
    # name should just be today's date
    name = name
    subject_set.display_name = name
    subject_set.save()
    return subject_set

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='Upload waterfall plots to Zooniverse as subjects')  
    parser.add_argument('--project', dest='project_id', type=int, default=12887, help='Project ID to upload waterfalls to (default Bursts from Space)')
    parser.add_argument('--uploaders', dest='n_uploaders', type=int, default=10, help='Max async upload coroutines (ie. simultaneous uploads')

    args = parser.parse_args() 
    project_id = args.project_id 

    # Interactions with database need to be done asynchronously 
    loop = asyncio.get_event_loop()

    # Every thursday at 12pm, the function to run the send to Zooniverse code is run using Schedule
    schedule.every().thursday.at("12:00").do(loop.run_until_complete(upload_waterfalls_from_db(project_id)))

    # Loop so that the scheduled task keeps on running indefinitely
    while True:
        # Checks whether a scheduled task is pending to run or not
        schedule.run_pending()
        time.sleep(1)
    