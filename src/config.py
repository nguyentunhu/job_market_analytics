"""
Centralized configuration for the job market analytics pipeline.

This file defines various static data used across the project,
such as skill keywords, seniority levels, and categories for transformation.
"""

from typing import Dict, List, Any

class SkillConfig:
    """
    Defines skills, tools, and their categorization for extraction.
    """
    
    # Define primary skill categories and their keywords
    PROGRAMMING_LANGUAGES = {
        "Python": ["python", "py"],
        "SQL": ["sql", "mysql", "postgresql", "mssql", "sqlite"],
        "R": ["r", "rstudio"],
        "Java": ["java"],
        "Scala": ["scala"],
        "JavaScript": ["javascript", "js", "node.js"],
    }

    BI_TOOLS = {
        "Power BI": ["power bi", "powerbi"],
        "Tableau": ["tableau"],
        "Looker": ["looker"],
        "Qlik Sense": ["qlik sense", "qliksense"],
        "Google Data Studio": ["google data studio", "looker studio"],
    }

    CLOUD_PLATFORMS = {
        "AWS": ["aws", "amazon web services"],
        "Azure": ["azure", "microsoft azure"],
        "GCP": ["gcp", "google cloud platform"],
        "Snowflake": ["snowflake"],
        "Databricks": ["databricks"],
        "Redshift": ["redshift"],
        "BigQuery": ["bigquery"],
    }

    DATABASES = {
        "PostgreSQL": ["postgresql", "postgres"],
        "MySQL": ["mysql"],
        "SQL Server": ["sql server", "mssql"],
        "MongoDB": ["mongodb", "mongo db"],
        "Oracle DB": ["oracle db", "oracle database"],
        "Elasticsearch": ["elasticsearch"],
    }

    BIG_DATA_TECHNOLOGIES = {
        "Spark": ["spark", "apache spark"],
        "Hadoop": ["hadoop", "apache hadoop"],
        "Kafka": ["kafka", "apache kafka"],
        "Flink": ["flink", "apache flink"],
    }

    ETL_TOOLS = {
        "Airflow": ["airflow", "apache airflow"],
        "Luigi": ["luigi"],
        "Talend": ["talend"],
        "SSIS": ["ssis"],
    }

    VERSION_CONTROL = {
        "Git": ["git", "github", "gitlab", "bitbucket"],
    }

    STATISTICS_ML = {
        "Pandas": ["pandas"],
        "NumPy": ["numpy", "num py"],
        "Scikit-learn": ["scikit-learn", "sklearn"],
        "TensorFlow": ["tensorflow"],
        "Keras": ["keras"],
        "PyTorch": ["pytorch"],
        "Statsmodels": ["statsmodels"],
        "A/B Testing": ["a/b testing", "ab testing"],
        "Regression": ["regression"],
        "Classification": ["classification"],
        "Clustering": ["clustering"],
    }
    
    SOFT_SKILLS = {
        "Communication": ["communication", "communicating"],
        "Problem Solving": ["problem solving", "problem-solving"],
        "Teamwork": ["teamwork", "team player"],
        "Critical Thinking": ["critical thinking"],
        "Attention to Detail": ["attention to detail"],
        "Adaptability": ["adaptability", "adaptable"],
    }

    @classmethod
    def get_all_skills(cls) -> Dict[str, Dict[str, List[str]]]:
        """Aggregates all defined skills and their keywords by category."""
        return {
            "programming_languages": cls.PROGRAMMING_LANGUAGES,
            "bi_tools": cls.BI_TOOLS,
            "cloud_platforms": cls.CLOUD_PLATFORMS,
            "databases": cls.DATABASES,
            "big_data_technologies": cls.BIG_DATA_TECHNOLOGIES,
            "etl_tools": cls.ETL_TOOLS,
            "version_control": cls.VERSION_CONTROL,
            "statistics_ml": cls.STATISTICS_ML,
            "soft_skills": cls.SOFT_SKILLS,
        }

    @classmethod
    def build_keyword_set(cls) -> Dict[str, str]:
        """
        Builds a flattened dictionary of all keywords mapped back to their primary skill name.
        Used for efficient lookup during skill extraction.
        """
        keyword_to_skill = {}
        for category, skills_dict in cls.get_all_skills().items():
            for skill_name, keywords in skills_dict.items():
                for keyword in keywords:
                    keyword_to_skill[keyword.lower()] = skill_name
        return keyword_to_skill

class SeniorityConfig:
    """
    Defines seniority levels and associated keywords and salary ranges.
    """
    SENIORITY_LEVELS: Dict[str, List[str]] = {
        "intern": ["intern", "thực tập sinh"],
        "junior": ["junior", "fresher", "entry-level"],
        "mid_level": ["mid", "experienced", "chuyên viên"],
        "senior": ["senior", "lead", "trưởng nhóm"],
        "manager_lead": ["manager", "lead", "quản lý", "trưởng phòng"],
        "director_vp": ["director", "vice president", "phó giám đốc", "giám đốc"],
    }

    # Salary ranges for validation (in VND, example values)
    # This should be updated with actual market data
    SENIORITY_SALARY_RANGES: Dict[str, tuple[int, int]] = {
        "intern": (1_000_000, 5_000_000),  # 1-5 million VND
        "junior": (5_000_000, 15_000_000), # 5-15 million VND
        "mid_level": (12_000_000, 25_000_000), # 12-25 million VND
        "senior": (20_000_000, 40_000_000), # 20-40 million VND
        "manager_lead": (30_000_000, 60_000_000), # 30-60 million VND
        "director_vp": (50_000_000, 100_000_000), # 50-100 million VND
    }

    @classmethod
    def map_level(cls, text: str) -> str:
        """Maps detected text to a standardized seniority level."""
        text_lower = text.lower()
        for level, keywords in cls.SENIORITY_LEVELS.items():
            if any(k in text_lower for k in keywords):
                return level.replace('_', ' ').title()
        return "Not Specified"

