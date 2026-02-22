import logging
import re
from typing import Dict, Any, List, Optional
from datetime import datetime

from src.config import SkillConfig, SeniorityConfig

logger = logging.getLogger('transformer')


class JobDataTransformer:
    """transforms raw job data into analytics-ready format."""
    
    def __init__(self):
        self.skill_keywords_map = SkillConfig.build_keyword_set()
    
    def transform_job(self, raw_job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        transform a single raw job record.
        
        args:
            raw_job: raw job data from scraper
            
        returns:
            processed job with extracted insights or none if essential data is missing.
        """
        try:
            # essential fields for transformation
            job_title_raw = raw_job.get('job_title', '')
            job_description_raw = raw_job.get('job_description', '')
            platform = raw_job.get('platform')
            job_url = raw_job.get('job_url')

            if not job_title_raw or not job_description_raw or not platform or not job_url:
                logger.warning(f"skipping transformation for job due to missing essential fields: {job_url}")
                return None
            
            job_description_clean = self._clean_description(job_description_raw)
            
            processed_job = {
                'platform_job_id': self._generate_platform_job_id(raw_job), # new unique id for the platform
                'platform': platform,
                'job_url': job_url,
                'job_title': job_title_raw,
                'company_name': raw_job.get('company', ''),
                'location': raw_job.get('location', ''),
                'posted_date': self._normalize_date(raw_job.get('posting_date')),
                'scraped_at': raw_job.get('scraped_at', datetime.now().isoformat()),
                'seniority_level': self._detect_seniority(job_title_raw, job_description_raw),
                'salary_min': None,
                'salary_max': None,
                'salary_currency': 'VND',  # default for Vietnam
                'raw_description': job_description_raw,
                'clean_description': job_description_clean,
                'processed_at': datetime.now().isoformat(),
            }
            
            # extract salary if available
            salary_data = self._extract_salary(job_title_raw + ' ' + job_description_raw)
            if salary_data:
                processed_job['salary_min'] = salary_data.get('min')
                processed_job['salary_max'] = salary_data.get('max')
                processed_job['salary_currency'] = salary_data.get('currency', 'VND')
            
            # extract skills and tools
            processed_job['extracted_skills'] = self._extract_skills(job_description_raw, job_title_raw)
            
            return processed_job
        
        except Exception as e:
            logger.error(f"error transforming job from {raw_job.get('job_url', 'n/a')}: {e}")
            return None
    
    def _generate_platform_job_id(self, raw_job: Dict[str, Any]) -> str:
        """
        generates a unique id for the job within its platform.
        this often can be derived from the job_url.
        """
        job_url = raw_job.get('job_url')
        if job_url:
            # example: for "https://www.vietnamworks.com/data-analyst-job-123456",
            # extract "123456" or the whole slug "data-analyst-job-123456"
            # for simplicity, we'll use a hash for now to ensure uniqueness if url structure varies
            return f"{raw_job['platform']}_{hash(job_url)}"
        return f"{raw_job['platform']}_no_url_{datetime.now().timestamp()}"

    def _normalize_date(self, date_string: Optional[str]) -> Optional[str]:
        """
        normalize date strings into 'yyyy-mm-dd' format.
        this is a placeholder for a more robust date parsing function.
        """
        if not date_string:
            return None
        
        # attempt to parse common formats. this should be expanded.
        try:
            # example: "2024-01-15t10:30:00.000z" (iso format)
            if 't' in date_string and '-' in date_string:
                return datetime.fromisoformat(date_string.replace('z', '+00:00')).strftime('%y-%m-%d')
            # example: "2024-01-15"
            if re.match(r'\d{4}-\d{2}-\d{2}', date_string):
                return date_string
            # add more parsing logic for various string formats here
        except ValueError:
            logger.debug(f"could not parse date string: {date_string}")
            return None
        return None # could not normalize
    
    def _extract_skills(self, job_description: str, job_title: str) -> List[Dict[str, Any]]:
        """
        extract skills from job description and title using skillconfig.
        
        returns:
            list of {skill_name, skill_category}
        """
        skills_found = []
        combined_text = (job_description + ' ' + job_title).lower()
        
        all_skill_configs = SkillConfig.get_all_skills()
        
        for category_name, skills_in_category in all_skill_configs.items():
            for skill_name, keywords in skills_in_category.items():
                for keyword in keywords:
                    # use word boundary regex for accurate matching
                    pattern = rf'\b{re.escape(keyword)}\b'
                    if re.search(pattern, combined_text):
                        # add only if not already found to avoid duplicates from different keywords
                        if not any(s['skill_name'] == skill_name for s in skills_found):
                            skills_found.append({
                                'skill_name': skill_name,
                                'skill_category': self._map_category_to_skill_type(category_name)
                            })
                        break # found one keyword for this skill, move to next skill
        return skills_found
    
    def _clean_description(self, description: str) -> str:
        """clean and normalize job description."""
        if not description:
            return ''
        
        # remove html tags if any
        description = re.sub(r'<[^>]+>', '', description)
        
        # remove extra whitespace
        description = ' '.join(description.split())
        
        # remove very long strings (likely urls or garbage)
        # this part should be used carefully as it might remove valid long words
        # for a portfolio project, it's a simple heuristic
        words = description.split()
        words = [w for w in words if len(w) < 100] 
        description = ' '.join(words)
        
        return description
    
    def _detect_seniority(self, job_title: str, job_description: str) -> str:
        """
        detect seniority level from job title and description using seniorityconfig.
        """
        combined_text = (job_title + ' ' + job_description).lower()
        
        # prioritize based on the order defined in seniorityconfig.seniority_levels
        # (e.g., manager/lead before senior, senior before mid, etc.)
        # we'll reverse iterate through the default order to catch higher seniority first
        for level_key in reversed(list(SeniorityConfig.SENIORITY_LEVELS.keys())):
            patterns = SeniorityConfig.SENIORITY_LEVELS[level_key]
            for pattern in patterns:
                if re.search(rf'\b{re.escape(pattern)}\b', combined_text):
                    return SeniorityConfig.map_level(level_key)
        
        return "not specified"
    
    def _extract_salary(self, text: str) -> Optional[Dict[str, Any]]:
        """
        extract salary information from text.
        this is a basic extractor and can be enhanced for more complex patterns.
        """
        text_lower = text.lower()
        
        # patterns for ranges: "x - y million", "x to y triệu"
        # example: 10 - 15 triệu, 10-15tr, 10.000.000 - 15.000.000 vnđ
        patterns = [
            r'(\d[\d\.]*)\s*(?:-|đến|tới)\s*(\d[\d\.]*)\s*(?:triệu|tr|vnd|vnđ)', # 10 - 15 triệu, 10.000.000 - 15.000.000 vnd
            r'(\d[\d\.]*)\s*(?:-|đến|tới)\s*(\d[\d\.]*)', # 10 - 15 (assume triệu)
            r'up to\s*(\d[\d\.]*)\s*(?:triệu|tr|vnd|vnđ)', # up to 15 triệu
            r'from\s*(\d[\d\.]*)\s*(?:triệu|tr|vnd|vnđ)', # from 10 triệu
            r'(\d[\d\.]*)\s*(?:triệu|tr|vnd|vnđ)\s*-\s*(\d[\d\.]*)\s*(?:triệu|tr|vnd|vnđ)', # 10 triệu - 15 triệu
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text_lower)
            if match:
                try:
                    min_val_str = match.group(1).replace('.', '')
                    max_val_str = match.group(2).replace('.', '') if len(match.groups()) > 1 else None
                    
                    min_salary = int(float(min_val_str))
                    max_salary = int(float(max_val_str)) if max_val_str else min_salary * 1.2 # estimate max if only min is given
                    
                    # convert 'trieu' or 'tr' to actual millions if not already
                    if any(unit in match.group(0) for unit in ['triệu', 'tr']):
                        min_salary *= 1_000_000
                        if max_val_str: max_salary *= 1_000_000
                    elif len(str(min_salary)) < 7: # if number is small, and no unit like 'trieu', assume it's in millions too
                        min_salary *= 1_000_000
                        if max_val_str: max_salary *= 1_000_000

                    return {
                        'min': min_salary,
                        'max': max_salary,
                        'currency': 'vnd'
                    }
                except (ValueError, IndexError):
                    continue
        
        # single value salary (e.g., "10 million vnd")
        single_value_pattern = r'(\d[\d\.]*)\s*(?:triệu|tr|vnd|vnđ)'
        match = re.search(single_value_pattern, text_lower)
        if match:
            try:
                salary_val_str = match.group(1).replace('.', '')
                salary = int(float(salary_val_str))
                if any(unit in match.group(0) for unit in ['triệu', 'tr']):
                    salary *= 1_000_000
                elif len(str(salary)) < 7: # heuristic for single value
                    salary *= 1_000_000
                
                return {
                    'min': salary,
                    'max': salary * 1.2, # assume a 20% range for single value
                    'currency': 'vnd'
                }
            except (ValueError, IndexError):
                pass

        return None
    
    def _map_category_to_skill_type(self, category: str) -> str:
        """map skill category to a more general skill type for the database."""
        mapping = {
            'programming_languages': 'Language',
            'bi_tools': 'Tool',
            'cloud_platforms': 'Platform',
            'databases': 'Database',
            'big_data_technologies': 'Technology',
            'etl_tools': 'Tool',
            'version_control': 'Tool',
            'statistics_ml': 'Skill',
            'soft_skills': 'Soft Skill',
        }
        return mapping.get(category, 'Other')
    
    def transform_batch(self, raw_jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        transform a batch of raw job records.
        
        args:
            raw_jobs: a list of raw job dictionaries.
            
        returns:
            a list of successfully transformed job dictionaries.
        """
        processed_jobs = []
        for raw_job in raw_jobs:
            processed = self.transform_job(raw_job)
            if processed:
                processed_jobs.append(processed)
        logger.info(f"successfully transformed {len(processed_jobs)} out of {len(raw_jobs)} raw jobs.")
        return processed_jobs
