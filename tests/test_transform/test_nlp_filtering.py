"""Test NLP filtering and statistics for JobDataTransformer."""

import unittest
from src.transform.job_transformer import JobDataTransformer


class TestNLPFilteringAndStatistics(unittest.TestCase):
    """Test NLP-based job relevance filtering and statistics tracking."""
    
    def setUp(self):
        self.transformer = JobDataTransformer(enable_nlp_filter=True)
    
    def test_transform_batch_with_query_returns_stats(self):
        """Test that transform_batch returns statistics tuple."""
        test_jobs = [
            {
                'job_title': 'Data Analyst',
                'job_description': 'Công ty: TechCorp Địa điểm: Hà Nội Ngày cập nhật: 15/02/2026 Requirements: SQL, Python',
                'platform': 'test',
                'job_url': 'https://example.com/job/1',
                'company': '',
                'location': ''
            },
            {
                'job_title': 'Junior Developer',
                'job_description': 'Công ty: DevCorp Địa điểm: HCM Ngày cập nhật: 20/02/2026 Requirements: JavaScript, React',
                'platform': 'test',
                'job_url': 'https://example.com/job/2',
                'company': '',
                'location': ''
            }
        ]
        
        results, stats = self.transformer.transform_batch(test_jobs, query='Data Analyst')
        
        self.assertIsInstance(results, list)
        self.assertIsInstance(stats, dict)
        self.assertIn('total_jobs', stats)
        self.assertIn('filtered_relevant', stats)
        self.assertIn('filter_ratio', stats)
    
    def test_statistics_track_missing_fields(self):
        """Test that statistics track missing company/location/date."""
        test_job = {
            'job_title': 'Data Engineer',
            'job_description': 'No company or location mentioned in this description',
            'platform': 'test',
            'job_url': 'https://example.com/job/99',
            'company': '',
            'location': ''
        }
        
        self.transformer.transform_job(test_job)
        stats = self.transformer.stats.get_summary()
        
        self.assertGreaterEqual(stats.get('missing_company', 0) + stats.get('missing_location', 0), 1)
    
    def test_keyword_based_relevance_filtering(self):
        """Test keyword-based relevance when NLP model unavailable."""
        transformer = JobDataTransformer(enable_nlp_filter=False)
        
        test_jobs = [
            {
                'job_title': 'Senior Data Analyst',
                'job_description': 'Tìm kiếm Data Analyst với kinh nghiệm SQL, Python, Tableau',
                'platform': 'test',
                'job_url': 'https://example.com/1',
                'company': '',
                'location': ''
            }
        ]
        
        results, stats = transformer.transform_batch(test_jobs, query='Data Analyst')
        self.assertEqual(len(results), 1)
    
    def test_filter_out_irrelevant_jobs(self):
        """Test that irrelevant jobs are filtered out (even with NLP disabled for consistency)."""
        transformer = JobDataTransformer(enable_nlp_filter=False)
        
        test_jobs = [
            {
                'job_title': 'Chef Position',
                'job_description': 'Looking for experienced chef to work in restaurant kitchen',
                'platform': 'test',
                'job_url': 'https://example.com/1',
                'company': '',
                'location': ''
            }
        ]
        
        results, stats = transformer.transform_batch(test_jobs, query='Data Analyst')
        self.assertEqual(stats.get('total_jobs', 0), 1)
    
    def test_extraction_with_complete_fields(self):
        """Test extraction with all formatted fields present."""
        transformer = JobDataTransformer(enable_nlp_filter=False)
        
        test_job = {
            'job_title': 'Data Analyst',
            'job_description': '''Công ty: FPT Software
            Địa điểm: Hà Nội
            Ngày cập nhật: 25/02/2026
            Requirement: Python, SQL''',
            'platform': 'test',
            'job_url': 'https://example.com/1',
            'company': '',
            'location': ''
        }
        
        results, stats = transformer.transform_batch([test_job])
        
        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result['company_name'], 'fpt software')
        self.assertEqual(result['location'], 'hà nội')
        self.assertEqual(result['posted_date'], '2026-02-25')
        self.assertEqual(stats['missing_company'], 0)
        self.assertEqual(stats['missing_location'], 0)
        self.assertEqual(stats['missing_posted_date'], 0)


if __name__ == '__main__':
    unittest.main(verbosity=2)
