# -*- coding: utf-8 -*-
"""Test extraction pipeline with real-world mixed-case Vietnamese job data."""

import unittest
from src.transform.job_transformer import JobDataTransformer


class TestMixedCaseExtraction(unittest.TestCase):
    """Verify extraction works with mixed-case input from real scrapers."""
    
    def setUp(self):
        self.transformer = JobDataTransformer()
    
    def test_mixed_case_vietnamese_extraction(self):
        """Test with realistic mixed-case Vietnamese job description."""
        raw_job = {
            'job_title': 'Senior Data Analyst',
            'job_description': '''CÔNG TY: Tech Corp Vietnam | 
            Địa Điểm: HÀ NỘI | 
            Ngày Cập Nhật: 15/02/2026
            Yêu Cầu: Python, SQL, Tableau | 
            Requirements: 5+ years | 
            Quyền Lợi: Competitive salary''',
            'platform': 'test_platform',
            'job_url': 'https://example.com/job/1',
            'posting_date': '',
            'company': '',
            'location': ''
        }
        
        result = self.transformer.transform_job(raw_job)
        
        self.assertIsNotNone(result, "transform_job should return a result")
        self.assertFalse(result['company_name'] is None, "company_name should not be None")
        self.assertFalse(result['location'] is None, "location should not be None")
        self.assertFalse(result['posted_date'] is None, "posted_date should not be None")
        self.assertTrue(result['clean_description'].islower(), "clean_description must be lowercase")
    
    def test_all_caps_labels_extraction(self):
        """Test extraction when all labels are in ALL CAPS."""
        clean_desc = "CÔNG TY: ABC Corporation | ĐỊA ĐIỂM: HỒ CHÍ MINH | NGÀY CẬP NHẬT: 01/03/2026"
        result = self.transformer.transform_job({
            'job_title': 'Data Engineer',
            'job_description': clean_desc,
            'platform': 'test',
            'job_url': 'https://test.com/1',
            'company': '',
            'location': ''
        })
        
        self.assertIsNotNone(result)
        self.assertTrue(len(result['company_name']) > 0, "Company should be extracted from ALL CAPS input")
        self.assertTrue(len(result['location']) > 0, "Location should be extracted from ALL CAPS input")
        self.assertEqual(result['posted_date'], '2026-03-01')
    
    def test_partial_caps_mixed_text(self):
        """Test with real Vietnamese job site format with mixed caps."""
        job_desc = '''Công Ty: FPT Software
        Địa Điểm: Hà Nội
        Ngày Cập Nhật: 25/02/2026
        
        REQUIREMENTS:
        - Python, SQL, AWS
        - 3+ years experience
        - Team player
        
        BENEFITS:
        - Competitive salary
        - Health insurance
        - Remote work option'''
        
        result = self.transformer.transform_job({
            'job_title': 'Data Analyst',
            'job_description': job_desc,
            'platform': 'test',
            'job_url': 'https://test.com/2',
            'company': '',
            'location': ''
        })
        
        self.assertIsNotNone(result)
        self.assertEqual(result['company_name'], 'fpt software')
        self.assertEqual(result['location'], 'hà nội')
        self.assertEqual(result['posted_date'], '2026-02-25')
        self.assertIn('python', result['clean_description'])
        self.assertIn('aws', result['clean_description'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
