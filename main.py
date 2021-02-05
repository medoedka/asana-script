import requests
import psycopg2
import pandas as pd
import os
from datetime import datetime
from sqlalchemy import create_engine

con = psycopg2.connect(
      database="d26t8fug1qg2sd",
      user="vydnpfaidoqwaf",
      password="8c935c05d75de48d6e52229b30143d5f64f060e9ae70671d9d98b658bb3ccf77",
      host="ec2-52-207-124-89.compute-1.amazonaws.com",
      port="5432"
    )

cur = con.cursor()

headers = {
    'Accept': 'application/json',
    'Authorization': 'Bearer 0/6e35e89ef91a5b06a3799a277ba0ff14'
}

params = (
    ('workspace', '264406100181662'),
    ('owner', '269092239480498'),
)

creds_file = os.path.abspath('asana-277914-461100f6405e.json')

active_projects = pd.DataFrame(columns=['task_name',
                                  'project_name',
                                  'start',
                                  'end',
                                  'completion',
                                  'assignee_name',
                                  'money',
                                  'time_for_task',
                                  'section_name',
                                  'task_id'])

archived_projects = pd.DataFrame(columns=['task_name',
                                  'project_name',
                                  'start',
                                  'end',
                                  'completion',
                                  'assignee_name',
                                  'money',
                                  'time_for_task',
                                  'section_name',
                                  'task_id'])


archived_projects_count = 1
cur.execute("SELECT * FROM archived_projects")
archived_projects_from_db = len(cur.fetchall())
engine = create_engine('postgresql+psycopg2://pavexihglsrfmg:9a86e2c4308d17f405606f221f8e93fcdc87191c3fa8ffa4d57407bee975dc93@ec2-54-144-45-5.compute-1.amazonaws.com/d93nml55fdjh7l')


def project_division():
    global archived_projects_from_db
    parser(False, active_projects)
    active_projects.to_sql('active_projects', engine, if_exists='replace')
    if archived_projects_count != archived_projects_from_db:
        parser(True, archived_projects)
        archived_projects.to_sql('archived_projects', engine, if_exists='replace')
    else:
        pass
    time_dataframe = pd.DataFrame({'time': str(datetime.now())}, index=[0])
    time_dataframe.to_sql('last_update', engine, if_exists='replace')


def parser(bool_type, project_type):
    primary_key = 1
    response_projects = requests.get('https://app.asana.com/api/1.0/portfolios/1199577154060949/items',
                                         headers=headers, params=params)
    projects = response_projects.json()['data']
    for j in projects:
        project_id = j['gid']
        project_info = requests.get('https://app.asana.com/api/1.0/projects/{}'.format(project_id),
                                    headers=headers, params=params)
        if project_info.json()['data']['archived'] == bool_type:
            project_tasks = requests.get('https://app.asana.com/api/1.0/projects/{}/tasks'.format(project_id),
                                         headers=headers)
            tasks = project_tasks.json()['data']
            for k in tasks:
                try:
                    task_id = k['gid']
                    task_data = requests.get('https://app.asana.com/api/1.0/tasks/{}'.format(task_id), headers=headers)
                    task_info = task_data.json()['data']
                    task_name = k['name'] if k['name'] is not None else ' '
                    project_name = j['name'] if j['name'] is not None else ' '
                    start = task_info['created_at'] if task_info['created_at'] is not None else ' '
                    end = task_info['due_on'] if task_info['due_on'] is not None else ' '
                    completion = task_info['completed_at'] if task_info['completed_at'] is not None else ' '
                except KeyError:
                    start = ' '
                    end = ' '
                    completion = ' '
                try:
                    assignee_name = task_info['assignee']['name'] if task_info['assignee']['name'] is not None else ' '
                except TypeError:
                    assignee_name = ' '
                except KeyError:
                    assignee_name = ' '
                try:
                    money = task_info['custom_fields'][1]['number_value'] \
                        if task_info['custom_fields'][1]['number_value'] is not None else ' '
                except TypeError:
                    money = ' '
                except KeyError:
                    money = ' '
                except IndexError:
                    pass
                try:
                    time_for_task = task_info['custom_fields'][0]['number_value'] \
                        if task_info['custom_fields'][0]['number_value'] is not None else ' '
                except TypeError:
                    time_for_task = ' '
                except KeyError:
                    time_for_task = ' '
                except IndexError:
                    pass
                section_name = task_info['memberships'][0]['section']['name']
                try:
                    project_type.loc[primary_key] = [task_name, project_name, start[:10], end, completion[:10],
                                                        assignee_name, money, time_for_task, section_name, task_id]
                except:
                    print('fail')
                    project_type.loc[primary_key] = [task_name, project_name, start[:10], end, completion[:10],
                                                        assignee_name, money, time_for_task, section_name, task_id]
                primary_key += 1
        else:
            global archived_projects_count
            archived_projects_count += 1


project_division()