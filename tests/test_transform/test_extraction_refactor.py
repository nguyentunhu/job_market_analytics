"""Test refactored company, location, and date extraction from clean_descriptions."""

import unittest
from src.transform.job_transformer import JobDataTransformer


class TestExtractionRefactor(unittest.TestCase):
    """Test extraction methods that work from clean_description."""
    
    def setUp(self):
        self.transformer = JobDataTransformer()
    
    def test_extract_company_from_clean_description(self):
        """Test company extraction from clean_description with 'công ty:' label."""
        clean_desc = "công ty: tech solutions vietnam địa điểm: hà nội"
        raw_job = {'company': 'fallback_company'}
        result = self.transformer._extract_company(clean_desc, raw_job)
        self.assertEqual(result, 'tech solutions vietnam')
    
    def test_extract_company_fallback(self):
        """Test company fallback to raw_job when not in clean_description."""
        clean_desc = "this description has no company label"
        raw_job = {'company': 'fallback_company'}
        result = self.transformer._extract_company(clean_desc, raw_job)
        self.assertEqual(result, 'fallback_company')
    
    def test_extract_location_from_clean_description(self):
        """Test location extraction from clean_description with 'địa điểm:' label."""
        clean_desc = "công ty: acme địa điểm: hà nội ngày cập nhật: 15/02/2026"
        raw_job = {'location': 'fallback_location'}
        result = self.transformer._extract_location(clean_desc, raw_job)
        self.assertEqual(result, 'hà nội')
    
    def test_extract_location_alternate_format(self):
        """Test location extraction with alternate city name format."""
        clean_desc = "công ty: acme địa điểm: hochiminh ngày cập nhật: 15/02/2026"
        raw_job = {'location': ''}
        result = self.transformer._extract_location(clean_desc, raw_job)
        self.assertEqual(result, 'hồ chí minh')
    
    def test_extract_location_fallback(self):
        """Test location fallback to raw_job when not in clean_description."""
        clean_desc = "this description has no location label"
        raw_job = {'location': 'fallback_location'}
        result = self.transformer._extract_location(clean_desc, raw_job)
        self.assertEqual(result, 'fallback_location')
    
    def test_extract_posted_date(self):
        """Test posted date extraction from 'ngày cập nhật:' label."""
        clean_desc = "công ty: acme địa điểm: hà nội ngày cập nhật: 15/02/2026"
        result = self.transformer._extract_posted_date(clean_desc)
        self.assertEqual(result, '2026-02-15')
    
    def test_extract_posted_date_alternate_format(self):
        """Test posted date extraction with yyyy-mm-dd format."""
        clean_desc = "ngày cập nhật: 2026-02-15 công ty: acme"
        result = self.transformer._extract_posted_date(clean_desc)
        self.assertEqual(result, '2026-02-15')
    
    def test_clean_description_is_lowercase(self):
        """Test that clean_description is lowercase."""
        raw_desc = "SENIOR DATA ANALYST | Công Ty XYZ Địa Điểm: Hà Nội NGày Cập Nhật: 15/02/2026"
        result = self.transformer._clean_description(raw_desc)
        self.assertTrue(result.islower() or not any(c.isupper() for c in result if c.isalpha()))
    
    def test_transform_job_uses_clean_description_extraction(self):
        """Test that transform_job correctly uses clean_description for extraction."""
        raw_job = {
            'job_title': 'Senior Data Analyst',
            'job_description': 'Công ty: Tech Corp Vietnam Địa điểm: Hà Nội Ngày cập nhật: 15/02/2026 Requirements: Python, SQL',
            'platform': 'test_platform',
            'job_url': 'https://example.com/job/1',
            'posting_date': '2026-02-01',
            'company': '',
            'location': ''
        }
        result = self.transformer.transform_job(raw_job)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['company_name'], 'tech corp vietnam')
        self.assertEqual(result['location'], 'hà nội')
        self.assertEqual(result['posted_date'], '2026-02-15')
        self.assertTrue(result['clean_description'].islower())


if __name__ == '__main__':
    unittest.main()
