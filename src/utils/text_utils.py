"""
Text processing utilities for job market analytics scrapers.

Provides common text normalization and extraction functions
used across multiple scraper platforms.
"""

import re
from typing import Optional
from bs4 import BeautifulSoup, element


def normalize_text(text: str) -> str:
    """
    Normalize whitespace and remove zero-width characters.
    
    Removes non-breaking spaces, zero-width spaces, and collapses
    multiple whitespace characters into single spaces.
    
    Args:
        text: Text to normalize
        
    Returns:
        Normalized text
        
    Example:
        >>> normalize_text("Hello  \\u00a0 World\\u200b")
        'Hello World'
    """
    if not text:
        return ""
    
    # Replace non-breaking spaces and zero-width characters
    text = text.replace("\u00a0", " ").replace("\u200b", "")
    
    # Collapse multiple whitespace characters
    text = re.sub(r"\s+", " ", text)
    
    # Strip leading/trailing whitespace
    return text.strip()


def extract_text_from_element(
    element_obj: Optional[element.Tag],
    class_name: Optional[str] = None,
    tag_name: Optional[str] = None,
) -> str:
    """
    Extract and normalize text from an HTML element.
    
    Args:
        element_obj: BeautifulSoup element or None
        class_name: Optional class name to search for within the element
        tag_name: Optional tag name to search for within the element
        
    Returns:
        Extracted and normalized text, or empty string if not found
        
    Example:
        >>> soup = BeautifulSoup(html, "html.parser")
        >>> extract_text_from_element(soup, class_name="title")
        'Job Title'
    """
    if not element_obj:
        return ""
    
    # If class or tag specified, search within the element
    if class_name:
        target = element_obj.find(class_=class_name)
        if target:
            element_obj = target
        else:
            return ""
    
    if tag_name:
        target = element_obj.find(tag_name)
        if target:
            element_obj = target
        else:
            return ""
    
    text = element_obj.get_text(separator=" ", strip=True)
    return normalize_text(text)


def extract_text_by_selector(
    soup: BeautifulSoup,
    tag: str,
    class_name: str,
    separator: str = " ",
) -> str:
    """
    Extract and normalize text by tag and class selector.
    
    Convenience function combining BeautifulSoup search with normalization.
    
    Args:
        soup: BeautifulSoup parsed HTML
        tag: HTML tag name (e.g., "div", "h1")
        class_name: CSS class name to search for
        separator: Separator to use when extracting text (default: space)
        
    Returns:
        Extracted and normalized text, or empty string if not found
        
    Example:
        >>> soup = BeautifulSoup(html, "html.parser")
        >>> extract_text_by_selector(soup, "h1", "job-title")
        'Data Analyst Job'
    """
    element_obj = soup.find(tag, class_=class_name)
    if not element_obj:
        return ""
    
    text = element_obj.get_text(separator=separator, strip=True)
    return normalize_text(text)


def extract_all_text_from_block(
    soup: BeautifulSoup,
    tag: str,
    class_name: str,
    separator: str = " ",
) -> str:
    """
    Extract all text from a block element (e.g., job description).
    
    Useful for extracting multi-paragraph descriptions while
    preserving logical structure.
    
    Args:
        soup: BeautifulSoup parsed HTML
        tag: HTML tag name
        class_name: CSS class name
        separator: Separator between text blocks (default: space)
        
    Returns:
        Extracted and normalized text block
        
    Example:
        >>> description = extract_all_text_from_block(
        ...     soup, "div", "job-description"
        ... )
    """
    element_obj = soup.find(tag, class_=class_name)
    if not element_obj:
        return ""
    
    text = element_obj.get_text(separator=separator, strip=True)
    return normalize_text(text)


def clean_html(html_text: str) -> str:
    """
    Remove HTML tags and normalize resulting text.
    
    Useful when you have HTML content mixed with text.
    
    Args:
        html_text: Text potentially containing HTML
        
    Returns:
        Cleaned text with HTML tags removed
    """
    # Remove HTML tags
    text = re.sub(r"<[^>]+>", "", html_text)
    return normalize_text(text)


def truncate_text(text: str, max_length: int = 500, suffix: str = "...") -> str:
    """
    Truncate text to a maximum length.
    
    Args:
        text: Text to truncate
        max_length: Maximum length (default: 500)
        suffix: Suffix to append if truncated (default: "...")
        
    Returns:
        Truncated text
    """
    if len(text) <= max_length:
        return text
    
    return text[:max_length - len(suffix)] + suffix
