"""
data loading module: inserts processed job data into database.
"""

import logging
from typing import List, Dict, Any, Optional

from src.utils.database import get_db_manager

logger = logging.getLogger('load')


class JobDataLoader:
    """loads processed job data into the database."""
    
    def __init__(self):
        self.db = get_db_manager()
        self.stats = {
            'jobs_inserted': 0,
            'job_descriptions_inserted': 0,
            'skills_inserted': 0,
            'job_skills_linked': 0,
            'errors': 0
        }
    
    def load_jobs(self, processed_jobs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        loads a batch of processed job records into the database.
        inserts into jobs, job_descriptions, and job_skills tables.
        """
        for job_data in processed_jobs:
            try:
                # 1. insert into 'jobs' table
                job_id = self._insert_job(job_data)
                if not job_id:
                    self.stats['errors'] += 1
                    continue
                self.stats['jobs_inserted'] += 1

                # 2. insert into 'job_descriptions' table
                if self._insert_job_description(job_id, job_data):
                    self.stats['job_descriptions_inserted'] += 1

                # 3. insert and link skills
                self._insert_and_link_skills(job_id, job_data.get('extracted_skills', []))
            
            except Exception as e:
                logger.error(f"error loading processed job {job_data.get('job_url', 'n/a')}: {e}")
                self.stats['errors'] += 1
        
        logger.info(f"finished loading batch. stats: {self.stats}")
        return self.stats
    
    def _insert_job(self, job_data: Dict[str, Any]) -> Optional[int]:
        """insert a job record into the 'jobs' table and return its id."""
        try:
            query = """
                insert or ignore into jobs 
                (platform_job_id, platform, job_url, job_title, 
                 scraped_at, seniority_level, salary_min, salary_max, salary_currency)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            params = (
                job_data.get('platform_job_id'),
                job_data.get('platform'),
                job_data.get('job_url'),
                job_data.get('job_title'),
                job_data.get('scraped_at'),
                job_data.get('seniority_level'),
                job_data.get('salary_min'),
                job_data.get('salary_max'),
                job_data.get('salary_currency')
            )
            
            # use execute_insert with return_id=true to get the last inserted rowid
            # if job was ignored, we need to retrieve its existing job_id
            job_id = self.db.execute_insert(query, params, return_id=True)
            if job_id is None: # job was ignored, retrieve existing id
                existing_job_query = "select job_id from jobs where job_url = ?"
                existing_job_result = self.db.execute_query(existing_job_query, (job_data.get('job_url'),), fetch_one=True)
                if existing_job_result:
                    job_id = existing_job_result[0]

            return job_id
        
        except Exception as e:
            logger.error(f"error inserting job {job_data.get('job_url', 'n/a')}: {e}")
            return None
            
    def _insert_job_description(self, job_id: int, job_data: Dict[str, Any]) -> bool:
        """insert job description into the 'job_descriptions' table. returns True if success."""
        try:
            query = """
                insert or ignore into job_descriptions
                (job_id, raw_description, clean_description)
                values (?, ?, ?)
            """
            params = (
                job_id,
                job_data.get('raw_description'),
                job_data.get('clean_description')
            )
            return self.db.execute_insert(query, params) is True
        except Exception as e:
            logger.error(f"error inserting job description for job_id {job_id}: {e}")
            return False

    def _insert_and_link_skills(self, job_id: int, skills: List[Dict[str, Any]]) -> None:
        """
        inserts new skills into the 'skills' table (if they don't exist)
        and links them to the job in 'job_skills'.
        """
        for skill in skills:
            skill_name = skill.get('skill_name')
            skill_category = skill.get('skill_category')
            
            if not skill_name:
                continue

            try:
                # insert skill into 'skills' table if it doesn't exist, get its id
                skill_id_query = "select skill_id from skills where skill_name = ?"
                existing_skill_id = self.db.execute_query(skill_id_query, (skill_name,), fetch_one=True)

                skill_id = None
                if existing_skill_id:
                    skill_id = existing_skill_id[0]
                else:
                    insert_skill_query = """
                        insert into skills (skill_name, skill_category)
                        values (?, ?)
                    """
                    skill_id = self.db.execute_insert(insert_skill_query, (skill_name, skill_category), return_id=True)
                    if skill_id:
                        self.stats['skills_inserted'] += 1
                
                # link skill to job in 'job_skills' table
                if skill_id:
                    link_query = """
                        insert or ignore into job_skills (job_id, skill_id)
                        values (?, ?)
                    """
                    if self.db.execute_insert(link_query, (job_id, skill_id)):
                        self.stats['job_skills_linked'] += 1

            except Exception as e:
                logger.error(f"error inserting/linking skill '{skill_name}' for job_id {job_id}: {e}")
                self.stats['errors'] += 1
    
    def get_statistics(self) -> Dict[str, Any]:
        """return loading statistics."""
        return self.stats.copy()
