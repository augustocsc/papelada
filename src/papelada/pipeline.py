"""
pdf_pipeline.py
Contains all logic for finding, loading, and processing PDF files.
This module is intended to be imported by main.py.
"""

import re
import json
import unicodedata
import pdfplumber
from pathlib import Path
from typing import List, Dict, Any, Optional

# --- Text Processing Functions ---

def extract(pdf_path: str) -> str:
    """
    Extracts raw text from a PDF file.
    
    Args:
        pdf_path: Path to the PDF file.

    Returns:
        A string containing the extracted text.
    """
    extracted_text = ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    extracted_text += page_text + "\n"
    except Exception as e:
        print(f"Error extracting data from PDF {pdf_path}: {e}")
        return "" # Return empty string on error
        
    return extracted_text

def clean(text: str) -> str:
    """
    Cleans the text by removing extra whitespace and normalizing line breaks.
    
    - Removes leading/trailing whitespace from each line.
    - Reduces multiple internal spaces to a single space.
    - Removes spaces before common punctuation (,.!?:;).
    - Normalizes multiple newlines to a single newline.

    Args:
        text: The input text.

    Returns:
        The cleaned text, preserving paragraph structure.
    """
    # 1. Normalize line endings (DOS/Mac -> Unix)
    text = re.sub(r'\r\n?', '\n', text)
    
    # 2. Process line by line
    lines = text.split('\n')
    treated_lines = []
    for line in lines:
        line = line.strip() # Remove leading/trailing space
        line = re.sub(r'\s+', ' ', line) # Remove internal duplicate spaces
        line = re.sub(r'\s+([,.;:!?)}\]])', r'\1', line) # Remove space before punctuation
        
        # Add only if the line is not empty after cleaning
        if line: 
            treated_lines.append(line)
    
    # 3. Re-join with a single newline, preserving paragraphs
    return '\n'.join(treated_lines)

def normalize(text: str, options: dict = None) -> str:
    """
    Normalizes the text content based on configuration options.
    
    Args:
        text: The input text (ideally already cleaned by clean_text)
        options: Dictionary containing normalization options:
            - flat: If True, collapses all newlines into spaces (default: True)
            - accents: Removes accent marks from characters (default: True) 
            - lowercase: Converts the text to lowercase (default: False)

    Returns:
        The normalized text.
    """
    # Use default options if none provided
    if options is None:
        options = {
            "flat": True,
            "accents": True,
            "lowercase": False
        }
    
    if options.get("flat", True):
        # Collapse all newlines (and surrounding space) into a single space
        text = re.sub(r'\s*\n+\s*', ' ', text).strip()
    
    if options.get("accents", True):
        # Remove accents
        text = ''.join(
            c for c in unicodedata.normalize('NFD', text)
            if unicodedata.category(c) != 'Mn'
        )
    
    if options.get("lowercase", False):
        text = text.lower()

    return text
# --- File Loading & Orchestration Functions ---

def load_json(json_path: str) -> Any:
    """
    Loads and parses a JSON file.
    Raises FileNotFoundError if the file does not exist.
    """
    p = Path(json_path)
    if not p.is_file():
        raise FileNotFoundError(f"JSON file not found: {json_path}")
    return json.loads(p.read_text(encoding="utf-8"))

