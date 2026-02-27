import logging
import re
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

from src.config import SkillConfig, SeniorityConfig, LocationConfig
from src.utils.nlp_utils import JobRelevanceFilter, NLPExtractor, FilterStatistics

logger = logging.getLogger('transformer')


class JobDataTransformer:
    """transforms raw job data into analytics-ready format with NLP filtering."""
    
    def __init__(self, enable_nlp_filter: bool = True, relevance_threshold: float = 0.3):
        """
        Initialize transformer with optional NLP-based relevance filtering.
        
        Args:
            enable_nlp_filter: Enable NLP-based job relevance filtering
            relevance_threshold: Similarity threshold for NLP relevance (0-1)
        """
        self.skill_keywords_map = SkillConfig.build_keyword_set()
        self.enable_nlp_filter = enable_nlp_filter
        self.relevance_filter = JobRelevanceFilter(threshold=relevance_threshold) if enable_nlp_filter else None
        self.nlp_extractor = NLPExtractor()
        self.stats = FilterStatistics()
    
    def transform_job(self, raw_job: Dict[str, Any], query: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        transform a single raw job record with optional NLP-based relevance filtering.
        
        args:
            raw_job: raw job data from scraper
            query: search query for NLP-based relevance filtering
            
        returns:
            processed job with extracted insights or none if essential data is missing or job is not relevant.
        """
        try:
            job_title_raw = raw_job.get('job_title', '')
            job_description_raw = raw_job.get('job_description', '')
            platform = raw_job.get('platform')
            job_url = raw_job.get('job_url')

            if not job_title_raw or not job_description_raw or not platform or not job_url:
                logger.warning(f"skipping transformation for job due to missing essential fields: {job_url}")
                self.stats.record_job(relevant=False, errors=True)
                return None
            
            if self.enable_nlp_filter and query and self.relevance_filter:
                is_relevant = self.relevance_filter.is_relevant(job_title_raw, job_description_raw, query)
                if not is_relevant:
                    self.stats.record_job(relevant=False)
                    return None
            
            self.stats.record_job(relevant=True)
            
            job_description_clean = self._clean_description(job_description_raw)

            company_extracted = self._extract_company(job_description_clean, raw_job)
            if not company_extracted:
                self.stats.record_missing_field('company')
            
            location_extracted = self._extract_location(job_description_clean, raw_job)
            if not location_extracted:
                self.stats.record_missing_field('location')
            
            posted_date = self._extract_posted_date(job_description_clean) or self._normalize_date(raw_job.get('posting_date'))
            if not posted_date:
                self.stats.record_missing_field('posted_date')

            processed_job = {
                'platform_job_id': self._generate_platform_job_id(raw_job), # new unique id for the platform
                'platform': platform,
                'job_url': job_url,
                'job_title': job_title_raw,
                'company_name': company_extracted,
                'location': location_extracted,
                'posted_date': posted_date,
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
            self.stats.record_job(relevant=False, errors=True)
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
    
    def _clean_description(self, description: str) -> Optional[str]:
        """Clean and normalize job description to lowercase."""
        if not description:
            return None
        
        description = description.lower()
        description = re.sub(r'<[^>]+>', '', description)
        description = ' '.join(description.split())
        description = re.sub(r'(chi tiết|gợi ý|báo xấu|nộp đơn|chia sẻ)', '', description, flags=re.IGNORECASE)
        words = [w for w in description.split() if len(w) < 100]
        description = ' '.join(words)
        return description if description.strip() else None
    
    def _detect_seniority(self, job_title: str, job_description: str) -> Optional[str]:
        """
        detect seniority level from job title and description using seniorityconfig.
        """
        combined_text = (job_title + ' ' + job_description).lower()
        
        # define an explicit priority order, from highest to lowest
        priority_order = ['director_vp', 'manager_lead', 'senior', 'mid_level', 'junior', 'intern']
        
        for level_key in priority_order:
            patterns = SeniorityConfig.SENIORITY_LEVELS[level_key]
            for pattern in patterns:
                # use word boundaries to avoid partial matches (e.g., 'seniori' matching 'senior')
                if re.search(rf'\b{re.escape(pattern)}\b', combined_text):
                    return SeniorityConfig.map_level(level_key)
        
        return None
    
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
    
    def _extract_posted_date(self, clean_description: str) -> Optional[str]:
        """
        Extract posted date from clean_description after 'ngày cập nhật:' label.
        Return as 'YYYY-MM-DD' format or None if not found.
        """
        if not clean_description:
            return None
        
        m = re.search(r'ngày cập nhật\s*:\s*([^\n]+?)(?=\b(?:công ty|địa điểm|yêu cầu|quyền lợi|$))', clean_description, flags=re.IGNORECASE)
        if m:
            date_raw = m.group(1).strip()
            date_match = re.search(r'(\d{1,2}[/\-.]\d{1,2}[/\-.]\d{2,4}|\d{4}[/\-.]\d{1,2}[/\-.]\d{1,2})', date_raw)
            if date_match:
                date_str = date_match.group(1)
                return self._parse_vietnamese_date(date_str)
        return None
    
    def _parse_vietnamese_date(self, date_str: str) -> Optional[str]:
        """
        Parse Vietnamese date formats: dd/mm/yyyy or yyyy-mm-dd.
        Return as 'yyyy-mm-dd' or None.
        """
        try:
            date_str = date_str.replace('-', '/').replace('.', '/')
            parts = date_str.split('/')
            if len(parts) != 3:
                return None
            
            if int(parts[0]) > 31:
                year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
            else:
                day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
            
            if year < 100:
                year += 2000
            
            return f"{year:04d}-{month:02d}-{day:02d}"
        except (IndexError, ValueError):
            return None

    def _map_category_to_skill_type(self, category: str) -> str:
        """Map skill category to general skill type for database."""
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
    
    def _extract_company(self, clean_description: str, raw_job: Dict[str, Any]) -> Optional[str]:
        """
        Extract company from clean_description after 'công ty:' label until next section.
        Fallback to raw_job['company'] if not found in description.
        """
        if not clean_description:
            return raw_job.get('company') or None
        
        m = re.search(r'công ty\s*:\s*([^\n]+?)(?=\b(?:địa điểm|ngày cập nhật|yêu cầu|quyền lợi|thông tin|$))', clean_description, flags=re.IGNORECASE | re.DOTALL)
        if m:
            company_raw = m.group(1).strip()
            company_clean = re.split(r'\|\n\r', company_raw)[0].strip()
            if company_clean:
                return company_clean
        
        return raw_job.get('company') or None

    def _extract_location(self, clean_description: str, raw_job: Dict[str, Any]) -> Optional[str]:
        """
        Extract location from clean_description after 'địa điểm:' label.
        Map result to canonical location using LocationConfig.
        Fallback to raw_job['location'] if not found.
        """
        if not clean_description:
            return raw_job.get('location') or None
        
        m = re.search(r'địa điểm\s*:\s*([^\n]+?)(?=\b(?:công ty|ngày cập nhật|yêu cầu|quyền lợi|thông tin|$))', clean_description, flags=re.IGNORECASE | re.DOTALL)
        if m:
            loc_raw = m.group(1).strip()
            loc_clean = re.split(r'\|\n\r', loc_raw)[0].strip()
            if loc_clean:
                canonical = LocationConfig.get_canonical_location(loc_clean)
                if canonical:
                    return canonical
        
        return raw_job.get('location') or None
    
    def transform_batch(self, raw_jobs: List[Dict[str, Any]], query: Optional[str] = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        transform a batch of raw job records with optional NLP filtering.
        
        args:
            raw_jobs: a list of raw job dictionaries.
            query: search query for NLP-based relevance filtering.
            
        returns:
            tuple of (processed jobs list, filtering statistics dict)
        """
        processed_jobs = []
        for raw_job in raw_jobs:
            processed = self.transform_job(raw_job, query=query)
            if processed:
                processed_jobs.append(processed)
        
        stats_summary = self.stats.get_summary()
        logger.info(f"successfully transformed {len(processed_jobs)} out of {len(raw_jobs)} raw jobs.")
        if self.enable_nlp_filter and query:
            logger.info(f"job relevance filter ratio: {stats_summary.get('filter_ratio', 'N/A')}")
        
        self.stats.print_summary()
        return processed_jobs, stats_summary
