
import json
import os
from datetime import date
import logging
import asyncio
import argparse
import time
from typing import List, Dict

import motor.motor_asyncio
import numpy as np #dont need
from dotenv import load_dotenv

# import pandas as pd #dont need
from tqdm import tqdm
# The new Zooniverse API for supporting user-created projects.
from panoptes_client import Panoptes, Project, SubjectSet, Subject


# Create a new connection to a single MongoDB instance at host:port.
client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017")
# Connection to a database 
db = client['zooniverseDB']
# # Connect to collection in the DB zooniverseDB
# events = db['events']


# gets waterfall plots from database
def upload_waterfalls_from_db(db_loc, project_id):
    items_filtered = []
    waterfalls_not_uploaded = items_filtered
    cursor_filtered = db.events.find({"subject_id": None, "upload_date": None, "snr":{'$lt': 2} }) # lt is 'less than' so a search for a snr values less than 2
    for doc in cursor_filtered.to_list():
        items_filtered.append(doc)
        waterfalls_not_uploaded.append(doc)

    logging.info(f'Waterfalls to upload: {len(waterfalls_not_uploaded)}')
    logging.info('Uploading from subfolders: \n')
    logging.info('Sending to subject sets: \n')

    authenticate()
    get_or_create_subject_set(project_id)
    upload_waterfalls(waterfalls_not_uploaded, project_id)


def upload_waterfalls(waterfalls, project_id): 
    authenticate()
    # ============here==================
    project = Project(project_id)

    logging.info(f'Uploading {len(waterfalls)} subjects')

    for waterfall in tqdm.tqdm(waterfalls):
        uploader(waterfall, project)

def uploader(waterfall, project):
        # Update event document to reflect that the waterfall plot has been uploaded 
        locations = waterfall['data_paths'].values()
        print(locations)
 
        # Gets subject_set name
        subject_set = get_or_create_subject_set(project.id)
        subject_set_name = subject_set.display_name

        # !!!
        id = waterfall['_id']
        # Updates the document in the database to have an upload date
        result = db.events.update_one({'_id': id}, {'$set': {{'upload_date': date.today()}}}) 
        metadata = get_real_metadata(waterfall)

        subject_id = upload_subject(locations=locations, project=project, subject_set_name=subject_set_name, metadata=metadata)
        # subject has been uploaded to Panoptes so now database needs to be updated to reflect this change knowing which specifc document was updated
        result = db.events.update_one({'_id': id}, {'$set': {{'subject_id': subject_id}}}) 

def get_real_metadata(waterfall):
    return {
        'event': waterfall.event,
        'beam': waterfall.beam,
        'upload_date': date.today().strftime('%Y-%m-%d'),
        # !!!
        '#training_subject': False,
        '#feedback_subject': False
    }

# get data of waterfall
# def get_subject_set_name_from_subfolder(waterfall):
#     # Query panoptes to see if date is already a subject set, if not, create one
#     # subfolder = waterfall.subfolder
#     # if subfolder.startswith('/'):
#     #     subfolder = subfolder[1:]

#     print("Date today: ", date.today())
#     # return subfolder.replace('/', '__') # name of parent folder
#     # return date.today() # name of parent folder
#     return 'test-subject-set'




# pure panoptes utilities below, could extract

def upload_subject(locations: List, project: Project, subject_set_name: str, metadata: Dict):
    # ============here==================
    # creation of a subject
    subject = Subject()
    # add files
    # subject is linked to project 
    subject.links.project = project
    for location in locations:
        if not os.path.isfile(location):
            raise FileNotFoundError('Missing subject location: {}'.format(location))
        subject.add_location(location)

    subject.metadata.update(metadata)

    # ==
    # need to add subject to subject set
    subject_set_name = subject_set_name #necessary line?
    # uses subject set name to get subject set
    subject_set = get_or_create_subject_set(project.id, subject_set_name)

    # saving subject
    subject.save()
    # adding subject to subject set
    subject_set.add(subject)
    return subject.id

def authenticate(): 
    load_dotenv()  # take environment variables from .env
    auth_username = username
    auth_password = password
    Panoptes.connect(username = auth_username, password = auth_password)

def get_or_create_subject_set(project_id):
    list_of_subject_sets = []
    # loops through subject sets inside of subject sets list
    for subject_set in project.links.subject_sets:
        # Puts the name, which is actually the date, of each subject_set into list
        list_of_subject_sets.append(subject_set.display_name)
    # for each name of a subject_set inside list, check if the current date is already present in list
    for subject in project.links.subject_sets:
        if(date.today() == subject.display_name):
            # subject_set of current date already exists so use that subject set
            return subject
    # subject set needs to be created
    return create_subject_set(project_id)
 

# does this need to be a function on its own?
# def make_subject_sets(project_id, names):
#     # to avoid threading issues where I might try to make the same subject set twice, make them all at the start
#     for name in names:
#         return get_or_create_subject_set(project_id, name)


# does this need to be its own function?
# def get_subject_set(project_id, name):
#     # will fail if duplicate display name - don't do this (not allowed, perhaps)
#     try:
#         return next(SubjectSet.where(project_id=project_id, display_name=name))
#     except StopIteration:
#         raise ValueError(f'Project {project_id} has no subject set {name}')

# does this need to be its own function?
def create_subject_set(project_id):
    # ============here==================
    # creating subject set
    subject_set = SubjectSet()
    subject_set.links.project = Project(project_id)
    name = date.today()
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

    # upload_waterfalls_from_db(db_loc, project_id)
