"""
text processing utilities for job market analytics scrapers.
"""

import re
from typing import Optional
from bs4 import BeautifulSoup, element


def normalize_text(text: str) -> str:
    """
    normalize whitespace and remove zero-width characters.
    
    removes non-breaking spaces, zero-width spaces, and collapses
    multiple whitespace characters into single spaces.
    
    args:
        text: text to normalize
        
    returns:
        normalized text
        
    example:
        >>> normalize_text("hello  \\u00a0 world\\u200b")
        'hello world'
    """
    if not text:
        return ""
    
    # replace non-breaking spaces and zero-width characters
    text = text.replace("\u00a0", " ").replace("\u200b", "")
    
    # collapse multiple whitespace characters
    text = re.sub(r"\s+", " ", text)
    
    # strip leading/trailing whitespace
    return text.strip()


def extract_text_from_element(
    element_obj: Optional[element.Tag],
    class_name: Optional[str] = None,
    tag_name: Optional[str] = None,
) -> str:
    """
    extract and normalize text from an html element.
    
    args:
        element_obj: beautifulsoup element or none
        class_name: optional class name to search for within the element
        tag_name: optional tag name to search for within the element
        
    returns:
        extracted and normalized text, or empty string if not found
        
    example:
        >>> soup = beautifulsoup(html, "html.parser")
        >>> extract_text_from_element(soup, class_name="title")
        'job title'
    """
    if not element_obj:
        return ""
    
    # if class or tag specified, search within the element
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
    extract and normalize text by tag and class selector.
    
    convenience function combining beautifulsoup search with normalization.
    
    args:
        soup: beautifulsoup parsed html
        tag: html tag name (e.g., "div", "h1")
        class_name: css class name to search for
        separator: separator to use when extracting text (default: space)
        
    returns:
        extracted and normalized text, or empty string if not found
        
    example:
        >>> soup = beautifulsoup(html, "html.parser")
        >>> extract_text_by_selector(soup, "h1", "job-title")
        'data analyst job'
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
    extract all text from a block element (e.g., job description).
    
    useful for extracting multi-paragraph descriptions while
    preserving logical structure.
    
    args:
        soup: beautifulsoup parsed html
        tag: html tag name
        class_name: css class name
        separator: separator between text blocks (default: space)
        
    returns:
        extracted and normalized text block
        
    example:
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
    remove html tags and normalize resulting text.
    
    useful when you have html content mixed with text.
    
    args:
        html_text: text potentially containing html
        
    returns:
        cleaned text with html tags removed
    """
    # remove html tags
    text = re.sub(r"<[^>]+>", "", html_text)
    return normalize_text(text)


def truncate_text(text: str, max_length: int = 500, suffix: str = "...") -> str:
    """
    truncate text to a maximum length.
    
    args:
        text: text to truncate
        max_length: maximum length (default: 500)
        suffix: suffix to append if truncated (default: "...")
        
    returns:
        truncated text
    """
    if len(text) <= max_length:
        return text
    
    return text[:max_length - len(suffix)] + suffix
