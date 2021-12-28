# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from .job import Job
from .crawler import Crawler
from .base_resource import *

class Entities(object):
    def __init__(self, **kwargs):
        self.Crawlers = kwargs.get('Crawlers')
        self.Jobs = kwargs.get('Jobs')

    def _extract_triggers(self, crawlers, jobs, workflow_name, starting_trigger_schedule, name_suffix):
        triggers = []
        starting_trigger = {
            'WorkflowName': workflow_name,
            'Name': "{}_starting_trigger".format(workflow_name),
            'Actions': []
        }
        if starting_trigger_schedule:
            starting_trigger['Type'] = "SCHEDULED"
            starting_trigger['Schedule'] = starting_trigger_schedule
            starting_trigger['StartOnCreation'] = True
        else:
            starting_trigger['Type'] = "ON_DEMAND"

        for crawler in crawlers:
            if 'DependsOn' in crawler:
                trigger = {'WorkflowName': workflow_name,
                           'Type': 'CONDITIONAL',
                           'Name': "{}_{}_trigger".format(workflow_name, crawler['Name']),
                           'Predicate': {'Conditions': []},
                           'StartOnCreation': True,
                           'Actions': [{'CrawlerName': crawler['Name']}]}
                for dependency, state in crawler['DependsOn'].items():
                    if isinstance(dependency, Crawler):
                        trigger['Predicate']['Conditions'].append({
                            'LogicalOperator': 'EQUALS',
                            'CrawlerName': "{}_{}".format(dependency.Name, name_suffix),
                            'CrawlState': state
                        })
                    elif isinstance(dependency, Job):
                        trigger['Predicate']['Conditions'].append({
                            'LogicalOperator': 'EQUALS',
                            'JobName': "{}_{}".format(dependency.Name, name_suffix),
                            'State': state
                        })
                    else:
                        raise TypeError("Dependencies must be of type Job or Crawler, but found {} instead".format(
                            type(dependency)))
                crawler.pop('DependsOn')
                if 'WaitForDependencies' in crawler:
                    trigger['Predicate']['Logical'] = crawler['WaitForDependencies']
                    crawler.pop('WaitForDependencies')
                triggers.append(trigger)
            else:
                starting_trigger['Actions'].append({'CrawlerName': crawler['Name']})

        for job in jobs:
            if 'DependsOn' in job:
                trigger = {'WorkflowName': workflow_name,
                           'Type': 'CONDITIONAL',
                           'Name': "{}_{}_trigger".format(workflow_name, job['Name']),
                           'Predicate': {'Conditions': []},
                           'StartOnCreation': True,
                           'Actions': [{'JobName': job['Name']}]}
                for dependency, state in job['DependsOn'].items():
                    if isinstance(dependency, Crawler):
                        trigger['Predicate']['Conditions'].append({
                            'LogicalOperator': 'EQUALS',
                            'CrawlerName': "{}_{}".format(dependency.Name, name_suffix),
                            'CrawlState': state
                        })
                    elif isinstance(dependency, Job):
                        trigger['Predicate']['Conditions'].append({
                            'LogicalOperator': 'EQUALS',
                            'JobName': "{}_{}".format(dependency.Name, name_suffix),
                            'State': state
                        })
                    else:
                        raise TypeError("Dependencies must be of type Job or Crawler, but found {} instead".format(
                            type(dependency)))
                job.pop('DependsOn')
                if 'WaitForDependencies' in job:
                    trigger['Predicate']['Logical'] = job['WaitForDependencies']
                    job.pop('WaitForDependencies')
                triggers.append(trigger)
            else:
                starting_trigger['Actions'].append({'JobName': job['Name']})
        triggers.append(starting_trigger)
        return triggers

    def validate(self):
        crawlers_valid = True
        jobs_valid = True
        if self.Crawlers:
            crawlers_valid = all(crawler.validate() for crawler in self.Crawlers)
        if 'Jobs' in self.__dict__:
            jobs_valid = all(job.validate() for job in self.Jobs)
        return crawlers_valid and jobs_valid

    def to_json(self, workflow_name, starting_trigger_schedule, name_suffix):
        result = {}
        crawlers = []
        if self.Crawlers:
            for crawler in self.Crawlers:
                cur_crawler = crawler.to_json()
                cur_crawler['Name'] = "{}_{}".format(cur_crawler['Name'], name_suffix)
                crawlers.append(cur_crawler)
        jobs = []
        if self.Jobs:
            for job in self.Jobs:
                cur_job = job.to_json()
                cur_job['Name'] = "{}_{}".format(cur_job['Name'], name_suffix)
                jobs.append(cur_job)
        triggers = self._extract_triggers(crawlers, jobs, workflow_name, starting_trigger_schedule, name_suffix)
        return crawlers, jobs, triggers


class Workflow(BaseResource):
    def __init__(self, **kwargs):
        self.__dict__['_validator'] = {
            'Name': (str, True),
            'Description': (str, False),
            'DefaultRunProperties': (dict, False),
            'Tags': (dict, False),
            'OnSchedule': (str, False),
            'Entities': (Entities, True)
        }
        self.Name = kwargs.get('Name')
        self.Description = kwargs.get('Description')
        self.DefaultRunProperties = kwargs.get('DefaultRunProperties')
        self.Tags = kwargs.get('Tags')
        self.OnSchedule = kwargs.get('OnSchedule')
        self.Entities = kwargs.get('Entities')

    def validate(self):
        '''
        Ensure all required fields are present and each entity is valid.
        An Exception will be thrown at the first occurrence of an invalid
        entity.
        '''
        fields_present = all(field in self.__dict__ for field, required in self._validator.items() if required[1])
        if not fields_present:
            raise TypeError('Required Workflow Fields were missing: {}'.format(self._getMissingFields()))

        entities_valid = self.Entities.validate()
        return fields_present and entities_valid

    def to_json(self, name_suffix = ""):
        self.validate()
        result = {}
        result['Workflow'] = {key: value for key, value in self.__dict__.items() if
                              key in self._validator and key not in ["Entities", "OnSchedule"]}
        result['Crawlers'], result['Jobs'], result['Triggers'] = self.Entities.to_json(self.Name,
                                                                                       self.OnSchedule if "OnSchedule" in self.__dict__ else "",
                                                                                       name_suffix)
        return result