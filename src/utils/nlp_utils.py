"""NLP utilities for job filtering and analysis using lightweight models."""

import logging
from typing import Dict, Any, Optional, List

try:
    from sentence_transformers import util, SentenceTransformer
    HAS_SENTENCE_TRANSFORMERS = True
except ImportError:
    HAS_SENTENCE_TRANSFORMERS = False

logger = logging.getLogger('nlp_utils')


class JobRelevanceFilter:
    """Filter jobs based on relevance to query using semantic similarity."""
    
    def __init__(self, model_name: str = 'all-MiniLM-L6-v2', threshold: float = 0.3):
        """
        Initialize relevance filter with a lightweight sentence transformer model.
        
        Args:
            model_name: HuggingFace model name (default: all-MiniLM-L6-v2, ~61MB)
            threshold: Similarity threshold (0-1) for job relevance (default: 0.3)
        """
        self.threshold = threshold
        self.model = None
        self.model_name = model_name
        
        if HAS_SENTENCE_TRANSFORMERS:
            try:
                logger.info(f"Loading NLP model: {model_name}")
                self.model = SentenceTransformer(model_name)
                logger.info("NLP model loaded successfully")
            except Exception as e:
                logger.warning(f"Failed to load NLP model: {e}. Falling back to keyword filtering.")
                self.model = None
        else:
            logger.warning("sentence-transformers not installed. Install with: pip install sentence-transformers")
    
    def is_relevant(self, job_title: str, job_description: str, query: str) -> bool:
        """
        Check if job is relevant to query using semantic similarity.
        
        Args:
            job_title: Job title
            job_description: Job description
            query: Search query (e.g., 'Data Analyst')
            
        Returns:
            True if job is relevant, False otherwise
        """
        if not self.model:
            return self._keyword_relevance(job_title, job_description, query)
        
        try:
            combined_text = f"{job_title} {job_description[:500]}"
            job_embedding = self.model.encode(combined_text, convert_to_tensor=True)
            query_embedding = self.model.encode(query, convert_to_tensor=True)
            
            similarity = util.pytorch_cos_sim(job_embedding, query_embedding).item()
            return similarity >= self.threshold
        except Exception as e:
            logger.warning(f"Error in semantic similarity: {e}. Using keyword matching.")
            return self._keyword_relevance(job_title, job_description, query)
    
    def _keyword_relevance(self, job_title: str, job_description: str, query: str) -> bool:
        """Fallback keyword-based relevance check."""
        query_lower = query.lower()
        text = f"{job_title} {job_description}".lower()
        
        keywords = query_lower.split()
        matched = sum(1 for kw in keywords if kw in text)
        return matched >= max(1, len(keywords) // 2)


class NLPExtractor:
    """Extract structured data from job descriptions using lightweight NLP."""
    
    @staticmethod
    def extract_key_phrases(text: str, top_n: int = 5) -> List[str]:
        """
        Extract key phrases from text using simple TF-IDF heuristic.
        
        Args:
            text: Text to extract from
            top_n: Number of top phrases to return
            
        Returns:
            List of key phrases
        """
        if not text or len(text) < 50:
            return []
        
        words = text.lower().split()
        stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
            'of', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during',
            'là', 'cái', 'của', 'và', 'hoặc', 'nhưng', 'trong', 'tại', 'với', 'có'
        }
        
        filtered = [w for w in words if w not in stop_words and len(w) > 3]
        
        from collections import Counter
        freq = Counter(filtered)
        return [word for word, _ in freq.most_common(top_n)]


class FilterStatistics:
    """Track statistics for job filtering and transformation."""
    
    def __init__(self):
        self.total_jobs = 0
        self.filtered_relevant = 0
        self.filtered_irrelevant = 0
        self.extraction_errors = 0
        self.jobs_with_missing_fields = {
            'company': 0,
            'location': 0,
            'posted_date': 0
        }
    
    def record_job(self, relevant: bool, errors: bool = False):
        """Record a job processing result."""
        self.total_jobs += 1
        if relevant:
            self.filtered_relevant += 1
        else:
            self.filtered_irrelevant += 1
        if errors:
            self.extraction_errors += 1
    
    def record_missing_field(self, field_name: str):
        """Record a missing extraction field."""
        if field_name in self.jobs_with_missing_fields:
            self.jobs_with_missing_fields[field_name] += 1
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics."""
        total = self.total_jobs
        if total == 0:
            return {}
        
        return {
            'total_jobs': total,
            'filtered_relevant': self.filtered_relevant,
            'filtered_irrelevant': self.filtered_irrelevant,
            'filter_ratio': f"{(self.filtered_relevant/total*100):.1f}%" if total > 0 else "N/A",
            'extraction_errors': self.extraction_errors,
            'missing_company': self.jobs_with_missing_fields['company'],
            'missing_location': self.jobs_with_missing_fields['location'],
            'missing_posted_date': self.jobs_with_missing_fields['posted_date'],
        }
    
    def print_summary(self):
        """Print summary statistics."""
        summary = self.get_summary()
        if not summary:
            logger.info("No jobs processed")
            return
        
        logger.info("="*60)
        logger.info("FILTERING & EXTRACTION STATISTICS")
        logger.info("="*60)
        logger.info(f"Total jobs processed: {summary.get('total_jobs', 0)}")
        logger.info(f"Relevant jobs: {summary.get('filtered_relevant', 0)}")
        logger.info(f"Filtered out: {summary.get('filtered_irrelevant', 0)}")
        logger.info(f"Relevance ratio: {summary.get('filter_ratio', 'N/A')}")
        logger.info(f"Extraction errors: {summary.get('extraction_errors', 0)}")
        logger.info(f"Missing company: {summary.get('missing_company', 0)}")
        logger.info(f"Missing location: {summary.get('missing_location', 0)}")
        logger.info(f"Missing posted_date: {summary.get('missing_posted_date', 0)}")
        logger.info("="*60)
