
import json
import os
from datetime import date
import logging
import asyncio
import argparse
import time
from typing import List, Dict

# import 
# from PIL import Image #dont need
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

    # GAIN ACCESS TO MY DB(CLIENT? SIMILAR TO HOW ITS DOWN FOR RWQUEST)

    # change around for my db
    # filter to: not uploaded (no date or subject_id) and snr rank in the top 3 for that beam (0, 1, 2)
    # waterfalls_not_uploaded = session.query(db_utils.Waterfall).filter(db_utils.Waterfall.upload_date == None).filter(db_utils.Waterfall.subject_id == None).filter(db_utils.Waterfall.signal_to_noise_rank <= 2
    # # ).filter(db_utils.Waterfall.dispersion >= 80.
    # # can also add snr 7.5-8.5, but I think this should happen on CHIME's end
    # ).all()

    items_all = []
    items_filtered = []
    waterfalls_not_uploaded = items_filtered
    # docs = client.zooniverse.events.find({}, projection={"_id": 0})
    # async for d in docs:
    #     items.append(d)

    cursor_all = db.events.find({})
    for d in cursor_all.to_list():
        items_all.append(d)
                                                                    # signal_to_noise_rank???????
    cursor_filtered = db.events.find({"subject_id": None, "upload_date": None, "snr":{'$lt': 2} }) # lt is 'less than' so a search for a snr values less than 2
    for doc in cursor_filtered.to_list():
        items_filtered.append(doc)
        waterfalls_not_uploaded.append(doc)

    logging.info(f'Waterfalls to upload: {len(waterfalls_not_uploaded)}')
    logging.info('Uploading from subfolders: \n')
    
    # gets subfolder name which is the date they were sent in
    # logging.info(pd.value_counts([w.subfolder for w in waterfalls_not_uploaded]))

    authenticate()

    #????? what is in these subfolders?
    # gets name for sbject set, it is the data
    # subject_set_names = [get_subject_set_name_from_subfolder(w) for w in waterfalls_not_uploaded]
    logging.info('Sending to subject sets: \n')
    # logging.info(pd.value_counts(subject_set_names))

    # Make edits to structring here 
    get_or_create_subject_set(project_id)
    # make_subject_sets(project_id, list(set(subject_set_names)))

    upload_waterfalls(waterfalls_not_uploaded, project_id)


def upload_waterfalls(waterfalls, project_id): 
    authenticate()  # inplace
    # ============here==================
    project = Project(project_id)

    logging.info(f'Uploading {len(waterfalls)} subjects')

    for waterfall in tqdm.tqdm(waterfalls):
        uploader(waterfall, project)


def uploader(waterfall, project):
        # Update event document to reflect that the waterfall plot has been uploaded 
      
        # result = db.events.update_one({'_id': id}, {'$set': {'subject_id': 0}}) 
        # waterfall.upload_date = date.today()
        # waterfall.subject_id = 0
        locations = waterfall['data_paths'].values()
        print(locations)

        # smoothed_plot_loc = waterfall.smoothed_plot_loc
        # plot_loc = waterfall.plot_loc
        # locations = [smoothed_plot_loc, plot_loc]
        #  !!!
        # subject_set_name = get_subject_set_name_from_subfolder(waterfall)
        # session.commit()
        get_subject_set()

        # !!!
        metadata = get_real_metadata(waterfall)
        # session.commit()

        subject_id = upload_subject(locations=locations, project=project, subject_set_name=subject_set_name, metadata=metadata)
        id = waterfall['_id']
        result = db.events.update_one({'_id': id}, {'$set': {{'upload_date': date.today()}, {'subject_id': subject_id}}}) 
        # add subject set property to schema for each waterfall so you can distsigusish which ones have been added
        # Once subject is uploaded, mark in database which zooniverse subject is that by querying database
        result =  db.events.update_one({'i': 51}, {'$set': {'key': 'value'}})
        # session.commit

def get_real_metadata(waterfall):
    return {
        'event': waterfall.event,
        'beam': waterfall.beam,
        'upload_date': date.today().strftime('%Y-%m-%d'),
        '#training_subject': False,
        '#feedback_subject': False
    }

# get data of waterfall
def get_subject_set_name_from_subfolder(waterfall):

    # Query panoptes to see if date is already a subject set, if not, create one


    # subfolder = waterfall.subfolder
    # if subfolder.startswith('/'):
    #     subfolder = subfolder[1:]

    print("Date today: ", date.today())
    # return subfolder.replace('/', '__') # name of parent folder
    # return date.today() # name of parent folder
    return 'test-subject-set'




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

def get_or_create_subject_set(project_id, name):
    list_of_subject_sets = []
    for subject in project.links.subject_sets:
        # print(subject.display_name)
        list_of_subject_sets.append(subject.display_name)
    for subject in project.links.subject_sets:
        if(date.today() == subject.display_name):
            # return get_subject_set(project_id, subject)
            return subject
        else:
            return date.today()
    # # check if subject set already exists
    # subject_set = None
    # try:
    #     return get_subject_set(project_id, name)
    # except ValueError:
    #     logging.info(f'Didnt find subject set {name} - creating it')
    #     return create_subject_set(project_id, name)

# does this need to be a function on its own?
def make_subject_sets(project_id, names):
    # to avoid threading issues where I might try to make the same subject set twice, make them all at the start
    for name in names:
        return get_or_create_subject_set(project_id, name)


# does this need to be its own function?
def get_subject_set(project_id, name):
    # will fail if duplicate display name - don't do this (not allowed, perhaps)
    try:
        return next(SubjectSet.where(project_id=project_id, display_name=name))
    except StopIteration:
        raise ValueError(f'Project {project_id} has no subject set {name}')

# does this need to be its own function?
def create_subject_set(project_id, name):
    # ============here==================
    # creating subject set
    subject_set = SubjectSet()
    subject_set.links.project = Project(project_id)
    subject_set.display_name = name
    subject_set.save()
    return subject_set


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='Upload waterfall plots to Zooniverse as subjects')  
    parser.add_argument('--project', dest='project_id', type=int, default=12887, help='Project ID to upload waterfalls to (default Bursts from Space)')
    parser.add_argument('--uploaders', dest='n_uploaders', type=int, default=10, help='Max async upload coroutines (ie. simultaneous uploads')

# Can you explain what this project_id is and if it would be required in my own code
    args = parser.parse_args() 
    project_id = args.project_id 
    # print("project_id: ", project_id)
    project = Project(project_id)
    # 2021-05-31
    # print("date: ", date.today())
    # query for all subject set ids
    # print("project.links.subject_sets: ", project.links.subject_sets)
    # list_of_subject_sets = []
    # for subject in project.links.subject_sets:
    #     # print(subject.display_name)
    #     list_of_subject_sets.append(subject.display_name)

    sub = SubjectSet.where(project_id=88906)
    print(sub)
    # query for a specifc project given its id
    # project_88906 = Project.find(88906)
    # print("Name: ", project_88906.name)

    # upload_waterfalls_from_db(db_loc, project_id)
