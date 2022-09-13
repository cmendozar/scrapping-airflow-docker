import requests
from bs4 import BeautifulSoup

import pymongo
import datetime


def etl_jobs(day: str) -> None:
    # REQUEST THE PAGE
    r = requests.get(url='https://www.getonbrd.com/empleos-data')
    html = r.text

    # EXTRACT JOBS FROM URL
    soup = BeautifulSoup(html, features="html.parser")
    raw_jobs = soup.find_all(attrs={"class": "remote"})
    # print(raw_jobs)
    # TRANSFORM DATA
    jobs = []
    for raw_job in raw_jobs:
        job_dict = {}
        job = BeautifulSoup(str(raw_job), features="html.parser")
        job_dict['name'] = job.find('strong').string
        attr = [attr for attr in job.find(
            attrs={'class': "size0"}).stripped_strings]
        job_dict['company'] = attr[0]
        try:
            job_dict['location'] = attr[1].split('\n')[0]
            job_dict['location extra'] = attr[1].split('\n')[1]
        except:
            job_dict['location'] = attr[1]
            job_dict['location extra'] = ''

        try:
            job_dict['level'] = job.find('span').string.split('|')[0]
            job_dict['work day'] = job.find('span').string.split('|')[1]
        except:
            job_dict['level'] = job.find('span').string
            job_dict['work day'] = ''

        job_dict['link'] = job.find('a').get('href')
        job_dict['portal'] = 'getonboard'
        job_dict['search date'] = datetime.datetime.strptime(day, "%Y-%m-%d")
        job_dict['publish date'] = job.find(
            attrs={'class': "gb-results-list__date color-hierarchy3"}).string.split('\n')[1]
        job_dict['key'] = job_dict['name'] + \
            job_dict['company'] + job_dict['publish date']
        jobs.append(job_dict)

    # MONGO USER AND PASS
    #USER_ID = config('USER_ID')
    #PASSWORD_USER = config('PASSWORD_USER')

    # LOAD JOBS TO MONGO COLLECTION
    database = 'scrapping'
    collection = 'jobs'

    user_id =  # Your user id
    pass_id =  # your password for user

    uri = f'mongodb+srv://'  # your uri

    client_mongo = pymongo.MongoClient(uri)
    database_mongo = client_mongo[database]
    jobs_collection = database_mongo[collection]
    jobs_collection.create_index('link', unique=True)

    try:
        load_jobs = jobs_collection.insert_many(jobs)
    except:
        inserted = []
        not_inserted = []
        for j in jobs:
            try:
                load = jobs_collection.insert_one(j)
                inserted.append(load.inserted_id)
            except:
                not_inserted.append(j['link'])
        print('TOTAL INSERTED: {} jobs'.format(len(jobs)))
        print('TOTAL INSERTED: {} jobs'.format(len(inserted)))
        print('TOTAL NOT INSERTED: {} jobs'.format(len(not_inserted)))
    print('TOTAL JOBS INSERTED: {}'.format(len(load_jobs.inserted_ids)))
    client_mongo.close()
