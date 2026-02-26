"""
centralized configuration for the job market analytics pipeline.
"""

from typing import Dict, List, Any

class SkillConfig:
    """
    defines skills, tools, and their categorization for extraction.
    """
    
    # define primary skill categories and their keywords
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
        "Excel": ["excel", "ms excel"],
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
        "Regression": ["regression", "hồi quy"],
        "Classification": ["classification", "phân loại"],
        "Clustering": ["clustering"],
    }
    
    SOFT_SKILLS = {
        "Communication": ["communication", "communicating", "communicator", "giao tiếp"],
        "Problem Solving": ["problem solving", "problem-solving", "problem-solver"],
        "Teamwork": ["teamwork", "team player"],
        "Critical Thinking": ["critical thinking"],
        "Attention to Detail": ["attention to detail", "tỉ mỉ"],
        "Adaptability": ["adaptability", "adaptable", "học hỏi"],
    }

    @classmethod
    def get_all_skills(cls) -> Dict[str, Dict[str, List[str]]]:
        """aggregates all defined skills and their keywords by category."""
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
        builds a flattened dictionary of all keywords mapped back to their primary skill name.
        used for efficient lookup during skill extraction.
        """
        keyword_to_skill = {}
        for category, skills_dict in cls.get_all_skills().items():
            for skill_name, keywords in skills_dict.items():
                for keyword in keywords:
                    keyword_to_skill[keyword.lower()] = skill_name
        return keyword_to_skill

class SeniorityConfig:
    """
    defines seniority levels and associated keywords and salary ranges.
    """
    SENIORITY_LEVELS: Dict[str, List[str]] = {
        "intern": ["intern", "thực tập sinh"],
        "junior": ["junior", "fresher", "entry"],
        "mid_level": ["mid", "experienced", "chuyên viên", "3 năm", "4 năm", "3+"],
        "senior": ["senior", "lead", "trưởng nhóm", "5+", "5 năm"],
        "manager_lead": ["manager", "lead", "quản lý", "trưởng phòng"],
        "director_vp": ["director", "vice president", "phó giám đốc", "giám đốc"],
    }

    # salary ranges for validation (in vnd)
    SENIORITY_SALARY_RANGES: Dict[str, tuple[int, int]] = {
        "intern": (1_000_000, 5_000_000),  # 1-5 million vnd
        "junior": (5_000_000, 15_000_000), # 5-15 million vnd
        "mid_level": (12_000_000, 25_000_000), # 12-25 million vnd
        "senior": (20_000_000, 40_000_000), # 20-40 million vnd
        "manager_lead": (30_000_000, 60_000_000), # 30-60 million vnd
        "director_vp": (50_000_000, 100_000_000), # 50-100 million vnd
    }

    @classmethod
    def map_level(cls, level_key: str) -> str:
        """maps the internal level key to its standardized string format."""
        return level_key
