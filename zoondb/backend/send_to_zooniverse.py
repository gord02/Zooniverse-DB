
import json
import os
from datetime import date
import logging
import argparse
import time
import math
from typing import List, Dict

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
    # need to be called here?
    get_or_create_subject_set(project_id)

    upload_waterfalls(waterfalls_not_uploaded, project_id)


def upload_waterfalls(waterfalls, project_id): 
    authenticate()
    # ============here==================
    project = Project(project_id)
    logging.info(f'Uploading {len(waterfalls)} subjects')

    for waterfall in tqdm(waterfalls):
        task = loop.create_task(uploader(waterfall, project))
        loop.run_until_complete(task)

async def uploader(waterfall, project):
    locations = waterfall['beams']['data_paths'].values()
    # plot = list(locations)[0]
    # plot_smooth = list(locations)[1]
    # locations = waterfall['data_paths'].values()
    # print("locations: ", locations)

    # Gets subject_set name
    subject_set = get_or_create_subject_set(project.id)
    subject_set_name = subject_set.display_name

    id = waterfall['_id']
    # Updates the document in the database to have an upload date
    the_date = str(date.today())
    result = await db.events.update_one({'_id': id}, {'$set': {'upload_date': the_date}}) 

    metadata = get_real_metadata(waterfall)
    subject_id = upload_subject(locations=locations, project=project, subject_set_name=subject_set_name, metadata=metadata)
    # subject has been uploaded to Panoptes so now database needs to be updated to reflect this change knowing which specifc document was updated
    result = await db.events.update_one({'_id': id}, {'$set': {'subject_id': subject_id}}) 

def get_real_metadata(waterfall):
    return {
        'event': waterfall['event'],
        'beams': waterfall['beams'],
        'upload_date': date.today().strftime('%Y-%m-%d'),
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
    # subject is linked to project 
    subject.links.project = project
    # path_prefix = "/Users/gordon/Desktop/zooniverse-db/files_for_zoon/data/frb-archiver/"
    test_plots = ['/Users/gordon/Desktop/zooniverse-db/files_for_zoon/data/frb-archiver/plot1.png','/Users/gordon/Desktop/zooniverse-db/files_for_zoon/data/frb-archiver/plot2.png', '/Users/gordon/Desktop/zooniverse-db/files_for_zoon/data/frb-archiver/plot3.png']
    # for location in locations:
    for plot in test_plots:
        if not os.path.isfile(plot):
            raise FileNotFoundError('Missing subject location: {}'.format(plot))

        subject.add_location(plot)
    subject.metadata.update(metadata)

    subject_set_name = subject_set_name 
    # Gets subject set 
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
    # Returns the test subject set created for testing 
    # project = Project.where(project_id = project_id)
    # print("project id: ", project.project_id)
    test_subject_set = SubjectSet.find(96428)
    # print("test_subject_set name: ", test_subject_set.display_name)
    return test_subject_set
    # list_of_subject_sets = []
    # for subject_set in project.links.subject_sets:
    #     list_of_subject_sets.append(subject_set.display_name)
    # for subject in project.links.subject_sets:
    #     if(subject_set_test == subject.display_name):
    #         return subject
    # return create_subject_set(project_id)


    # return subject_set_test
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
    subject_set.display_name = 'subject_set_test'
    # subject_set.display_name = name
    subject_set.save()
    # return subject set test
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
    loop.run_until_complete(upload_waterfalls_from_db(project_id))
    