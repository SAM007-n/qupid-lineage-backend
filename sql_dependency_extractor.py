import json
import time
import re
import yaml
import os
import hashlib
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import threading
import random
import asyncio
from openai import OpenAI
from pydantic import BaseModel, Field
from enum import Enum
import argparse
import shutil
from pathlib import Path

# Pydantic models for structured output
class Column(BaseModel):
    name: str
    data_type: Optional[str] = None

class TableEdge(BaseModel):
    from_table: str
    to_table: str
    transformation_type: str
    transformation_lines: Dict[str, int]

class ColumnEdge(BaseModel):
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    transformation_type: str
    transformation_lines: Dict[str, int]

class Table(BaseModel):
    id: str
    name: str
    role: str
    columns: List[str]

class Lineage(BaseModel):
    table_edges: List[TableEdge]
    column_edges: List[ColumnEdge]

class SQLDependencies(BaseModel):
    tables: List[Table]
    lineage: Lineage

# New models for reporting layer
class RunMode(str, Enum):
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"

class ExtractionPhase(str, Enum):
    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class ProgressStatus(BaseModel):
    run_id: str
    total_files: int
    processed_files: int
    current_file: Optional[str] = None
    estimated_time_remaining: Optional[float] = None  # in seconds
    processing_speed: Optional[float] = None  # files per second
    current_phase: str
    error_count: int = 0
    last_error: Optional[str] = None
    timestamp: str

class RepositoryDto(BaseModel):
    url: str
    branch: str
    commit_hash: str
    commit_timestamp: str

class StatsDto(BaseModel):
    total_files: int = 0
    processed: int = 0
    succeeded: int = 0
    failed: int = 0

class FileMetaDto(BaseModel):
    id: str
    path: str
    file_type: str
    url: str
    last_modified_at: str

class JobMetaDto(BaseModel):
    name: str
    template_type: str
    platform_key: Optional[str] = None

class TableBlockDto(BaseModel):
    raw: str
    role: str
    columns: List[str]

class CodeLinesDto(BaseModel):
    start_line: int
    end_line: int

class TableEdgeDto(BaseModel):
    from_table: str
    to_table: str
    transformation_type: str
    transformation_code: str
    code_lines: CodeLinesDto

class ColumnEdgeDto(BaseModel):
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    transformation_type: str
    transformation_code: str
    code_lines: CodeLinesDto

class ExtractionRunRequestDto(BaseModel):
    run_id: str
    run_mode: RunMode
    phase: ExtractionPhase
    repository: RepositoryDto
    triggered_by: str
    extractor_version: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    stats: Optional[StatsDto] = None

class FileExtractionRequestDto(BaseModel):
    run_id: str
    file: FileMetaDto
    job: Optional[JobMetaDto] = None
    tables: List[TableBlockDto]
    table_edges: List[TableEdgeDto]
    column_edges: List[ColumnEdgeDto]
    raw_sql_snippet: Optional[str] = None
    extracted_at: str

# Thread-safe print function
print_lock = threading.Lock()
def safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

def read_sql_file(file_path):
    """Read the contents of a SQL file."""
    with open(file_path, 'r') as file:
        return file.read()

def get_file_hash(file_path):
    """Calculate SHA-256 hash of file contents."""
    with open(file_path, 'rb') as f:
        return hashlib.sha256(f.read()).hexdigest()

def load_cache(cache_dir):
    """Load the cache of processed files."""
    cache_file = os.path.join(cache_dir, 'processed_files.yaml')
    if os.path.exists(cache_file):
        with open(cache_file, 'r') as f:
            try:
                return yaml.safe_load(f) or {}
            except yaml.YAMLError:
                return {}
    return {}

def save_cache(cache_dir, cache_data):
    """Save the cache of processed files."""
    cache_file = os.path.join(cache_dir, 'processed_files.yaml')
    os.makedirs(cache_dir, exist_ok=True)
    with open(cache_file, 'w') as f:
        yaml.dump(dict(cache_data), f)

def safe_yaml_load(yaml_content: str) -> dict:
    """
    Safely load YAML content with extensive error handling
    
    Args:
        yaml_content (str): YAML content that may have various formats or issues
        
    Returns:
        dict: Parsed YAML content or empty dict if parsing fails
    """
    if not yaml_content or not isinstance(yaml_content, str):
        return {}
        
    # Strip any leading/trailing whitespace
    yaml_content = yaml_content.strip()
    
    # Check if content is potentially not YAML
    if not any(indicator in yaml_content for indicator in 
              ['tables:', 'lineage:', 'table:', 'columns:', 'column_edges:']):
        safe_print("Warning: Content doesn't appear to be YAML, attempting to parse anyway")
        
    try:
        # First try normal YAML loading
        result = yaml.safe_load(yaml_content)
        if result is None:
            return {}
        if not isinstance(result, dict):
            safe_print(f"Warning: YAML loaded but result is not a dictionary, got {type(result)}")
            if isinstance(result, str):
                safe_print(f"YAML loaded as string: {result[:100]}...")
            return {}
        return result
    except yaml.YAMLError as ye:
        try:
            # Replace Jinja2 expressions with placeholders
            safe_print(f"Error in initial YAML parsing: {str(ye)}")
            safe_print("Attempting to handle Jinja2 templates in YAML...")
            
            placeholder_map = {}
            placeholder_counter = [0]
            
            def replace_jinja(match):
                placeholder = f"JINJA_PLACEHOLDER_{placeholder_counter[0]}"
                placeholder_map[placeholder] = match.group(0)
                placeholder_counter[0] += 1
                return placeholder
            
            # Replace {{ ... }} expressions
            modified_content = re.sub(r'{{\s*.*?\s*}}', replace_jinja, yaml_content)
            
            # Also replace {%...%} expressions
            modified_content = re.sub(r'{%\s*.*?\s*%}', replace_jinja, modified_content)
            
            # Parse the modified YAML
            parsed_yaml = yaml.safe_load(modified_content)
            if parsed_yaml is None:
                return {}
                
            # Restore Jinja2 expressions
            def restore_placeholders(obj):
                if isinstance(obj, dict):
                    return {k: restore_placeholders(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [restore_placeholders(item) for item in obj]
                elif isinstance(obj, str):
                    for placeholder, original in placeholder_map.items():
                        obj = obj.replace(placeholder, original)
                    return obj
                return obj
            
            result = restore_placeholders(parsed_yaml)
            if not isinstance(result, dict):
                safe_print(f"Warning: Jinja-processed YAML is not a dictionary, got {type(result)}")
                return {}
            return result
            
        except Exception as e:
            safe_print(f"Error in safe_yaml_load after Jinja2 processing: {str(e)}")
            safe_print(f"Problematic YAML content preview: {yaml_content[:200]}...")
            return {}

def add_line_numbers(content: str) -> str:
    """Add line numbers as prefixes (L1, L2, etc.) to each line of content."""
    lines = content.splitlines()
    numbered_lines = [f"L{i+1}: {line}" for i, line in enumerate(lines)]
    return '\n'.join(numbered_lines)

def extract_yaml_between_codeblocks(content: str) -> str:
    """
    Extract YAML content from various formats:
    1. Between ```yaml and ``` markers
    2. Between ``` and ``` markers (assuming YAML content)
    3. Plain YAML content (no markers)
    
    Args:
        content (str): Content that may contain YAML
        
    Returns:
        str: Extracted YAML content
    """
    if not content or not isinstance(content, str):
        return ""
        
    # If it already appears to be valid YAML (starts with typical YAML indicators)
    if content.lstrip().startswith(('tables:', 'lineage:', 'table:', 'column_edges:')):
        return content
    
    # Try to extract content between ```yaml and ``` markers
    yaml_pattern = r'```yaml\s*(.*?)\s*```'
    match = re.search(yaml_pattern, content, re.DOTALL)
    if match:
        return match.group(1).strip()
    
    # Try to extract content between ``` and ``` markers (assuming it's YAML)
    code_pattern = r'```\s*(.*?)\s*```'
    match = re.search(code_pattern, content, re.DOTALL)
    if match:
        return match.group(1).strip()
    
    # Check if the content has any YAML indicators but no code blocks
    if 'tables:' in content or 'lineage:' in content or 'table:' in content or 'column_edges:' in content:
        return content
        
    # No YAML content found
    return content

def save_yaml_output(content: str, source_file_path: str, is_repo_analysis: bool = False) -> str:
    """
    Save YAML output to an organized directory structure.
    
    Args:
        content: YAML content to save
        source_file_path: Original SQL/Jinja2 file path
        is_repo_analysis: Whether this is part of a repository analysis
        
    Returns:
        str: Path where the YAML file was saved
    """
    # Create base output directory
    output_base = os.path.join(os.getcwd(), 'lineage_output')
    os.makedirs(output_base, exist_ok=True)
    
    # Create cache directory inside lineage_output
    cache_dir = os.path.join(output_base, '.cache')
    os.makedirs(cache_dir, exist_ok=True)
    
    if is_repo_analysis:
        # For repository analysis, maintain the original directory structure
        repo_dir = os.path.dirname(source_file_path)
        relative_path = os.path.relpath(repo_dir, start=os.getcwd())
        output_dir = os.path.join(output_base, 'repositories', relative_path)
    else:
        # For individual file analysis, use a flat structure
        output_dir = os.path.join(output_base, 'individual_files')
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate output filename
    base_name = os.path.basename(source_file_path)
    yaml_name = base_name.replace('.sql', '_dependencies.yaml').replace('.jinja2', '_dependencies.yaml')
    output_path = os.path.join(output_dir, yaml_name)
    
    # Save the YAML file
    with open(output_path, 'w') as f:
        f.write(content)
    
    return output_path

def get_cache_dir() -> str:
    """Get the cache directory path."""
    output_base = os.path.join(os.getcwd(), 'lineage_output')
    cache_dir = os.path.join(output_base, '.cache')
    os.makedirs(cache_dir, exist_ok=True)
    return cache_dir

def get_yaml_output_path(source_file_path: str, is_repo_analysis: bool = False) -> str:
    """
    Get the expected YAML output path for a source file.
    
    Args:
        source_file_path: Original SQL/Jinja2 file path
        is_repo_analysis: Whether this is part of a repository analysis
        
    Returns:
        str: Expected YAML output path
    """
    # Create base output directory
    output_base = os.path.join(os.getcwd(), 'lineage_output')
    os.makedirs(output_base, exist_ok=True)
    
    if is_repo_analysis:
        # For repository analysis, maintain the original directory structure
        repo_dir = os.path.dirname(source_file_path)
        relative_path = os.path.relpath(repo_dir, start=os.getcwd())
        output_dir = os.path.join(output_base, 'repositories', relative_path)
    else:
        # For individual file analysis, use a flat structure
        output_dir = os.path.join(output_base, 'individual_files')
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate output filename
    base_name = os.path.basename(source_file_path)
    yaml_name = base_name.replace('.sql', '_dependencies.yaml').replace('.jinja2', '_dependencies.yaml')
    output_path = os.path.join(output_dir, yaml_name)
    
    return output_path



def extract_dependencies(file_path: str, api_key: str, max_retries: int = 3, concurrent: bool = False) -> str:
    """Synchronous wrapper for multi-pass extraction with chunking for complex files."""
    # Read file content to assess complexity
    file_content = read_sql_file(file_path)
    complexity_score, should_chunk, _ = assess_file_complexity(file_content)
    
    if should_chunk:
        # Process with chunking for complex files
        result = process_chunked_file_sync(file_path, api_key, max_retries, True, concurrent)
        # Convert dictionary result to YAML string if needed
        if isinstance(result, dict):
            return yaml.dump(result, sort_keys=False, indent=2)
        return result
    else:
        # Use multi-pass without chunking for simpler files
        result = extract_dependencies_multi_pass_sync(file_path, api_key, max_retries, concurrent)
        # Convert dictionary result to YAML string if needed
        if isinstance(result, dict):
            return yaml.dump(result, sort_keys=False, indent=2)
        return result

def extract_transformation_snippets(yaml_content: str, sql_content: str) -> str:
    """
    Extract transformation snippets from SQL content and add line count information to YAML content.
    
    Args:
        yaml_content (str): The YAML content containing lineage information
        sql_content (str): The original SQL/Jinja content
        
    Returns:
        str: Modified YAML content with transformation snippets and line count information
    """
    try:
        # Parse YAML content using the safe parser
        data = safe_yaml_load(yaml_content)
        if not data or not isinstance(data, dict):
            return yaml_content
            
        # Rest of the function remains the same
        sql_lines = sql_content.splitlines()
        total_lines = len(sql_lines)
        
        # Process table edges
        if 'lineage' in data and 'table_edges' in data['lineage']:
            for edge in data['lineage']['table_edges']:
                if 'transformation_lines' in edge:
                    lines = edge['transformation_lines']
                    start_line = lines.get('start_line', 0)
                    end_line = lines.get('end_line', 0)
                    
                    # Calculate line counts
                    count_before = min(2, start_line - 1)
                    count_after = min(2, total_lines - end_line)
                    
                    # Extract snippet with context
                    snippet_start = max(0, start_line - 1 - count_before)
                    snippet_end = min(total_lines, end_line + count_after)
                    transformation_snippet = '\n'.join(sql_lines[snippet_start:snippet_end])
                    
                    # Update edge information
                    edge['transformation_snippet'] = transformation_snippet
                    edge['transformation_lines'].update({
                        'count_line_before_start_line': count_before,
                        'count_line_after_end_line': count_after
                    })
        
        # Process column edges
        if 'lineage' in data and 'column_edges' in data['lineage']:
            for edge in data['lineage']['column_edges']:
                if 'transformation_lines' in edge:
                    lines = edge['transformation_lines']
                    start_line = lines.get('start_line', 0)
                    end_line = lines.get('end_line', 0)
                    
                    # Calculate line counts
                    count_before = min(2, start_line - 1)
                    count_after = min(2, total_lines - end_line)
                    
                    # Extract snippet with context
                    snippet_start = max(0, start_line - 1 - count_before)
                    snippet_end = min(total_lines, end_line + count_after)
                    transformation_snippet = '\n'.join(sql_lines[snippet_start:snippet_end])
                    
                    # Update edge information
                    edge['transformation_snippet'] = transformation_snippet
                    edge['transformation_lines'].update({
                        'count_line_before_start_line': count_before,
                        'count_line_after_end_line': count_after
                    })
        
        # Convert back to YAML
        return yaml.dump(data, sort_keys=False, indent=2)
        
    except Exception as e:
        safe_print(f"Error processing transformation snippets: {str(e)}")
        return yaml_content

def assess_file_complexity(file_content: str) -> Tuple[float, bool, int]:
    """
    Assess the complexity of a SQL file to determine if chunking is needed.
    
    Args:
        file_content (str): The SQL file content
        
    Returns:
        Tuple[float, bool, int]: (complexity_score, should_chunk, insert_count)
    """
    try:
        lines = file_content.splitlines()
        total_lines = len(lines)
        
        # Count various complexity indicators
        insert_count = len([line for line in lines if 'insert' in line.lower() and 'into' in line.lower()])
        select_count = len([line for line in lines if 'select' in line.lower()])
        join_count = len([line for line in lines if 'join' in line.lower()])
        cte_count = len([line for line in lines if 'with' in line.lower() and 'as' in line.lower()])
        
        # Calculate complexity score
        complexity_score = (
            (insert_count * 2) +  # Inserts are complex
            (select_count * 0.5) +  # Selects are moderate
            (join_count * 1.5) +  # Joins are complex
            (cte_count * 1.0) +  # CTEs are moderate
            (total_lines * 0.1)  # Base complexity from line count
        )
        
        # Determine if chunking is needed
        should_chunk = (
            complexity_score > 50 or  # High complexity score
            total_lines > 200 or  # Very long file
            insert_count > 5 or  # Many inserts
            join_count > 10  # Many joins
        )
        
        return complexity_score, should_chunk, insert_count
        
    except Exception as e:
        safe_print(f"Error assessing file complexity: {str(e)}")
        return 0.0, False, 0

async def process_chunked_file(file_path: str, api_key: str, max_retries: int, use_multi_pass: bool, concurrent: bool) -> str:
    """
    Process a complex file by breaking it into chunks and analyzing each chunk.
    
    Args:
        file_path (str): Path to the SQL file
        api_key (str): API key for LLM
        max_retries (int): Maximum retries for API calls
        use_multi_pass (bool): Whether to use multi-pass analysis
        concurrent (bool): Whether to use concurrent processing
        
    Returns:
        str: YAML content with dependencies
    """
    try:
        file_content = read_sql_file(file_path)
        lines = file_content.splitlines()
        
        # For now, use the multi-pass approach on the entire file
        # In a more sophisticated implementation, we would chunk the file
        safe_print(f"Processing complex file {file_path} with multi-pass analysis")
        
        result = await extract_dependencies_multi_pass(file_path, api_key, max_retries, concurrent)
        
        if isinstance(result, dict):
            return yaml.dump(result, sort_keys=False, indent=2)
        return result
        
    except Exception as e:
        safe_print(f"Error processing chunked file {file_path}: {str(e)}")
        return ""

def make_llm_api_call(client: OpenAI, prompt: str, max_retries: int = 3) -> str:
    """Make a simple synchronous API call to Groq."""
    last_error = None
    
    for retry_count in range(max_retries):
        try:
            safe_print(f"Making Groq API request (attempt {retry_count + 1}/{max_retries})...")
            
            # Make the API call for YAML output
            response = client.chat.completions.create(
                model="deepseek-r1-distill-llama-70b",
                messages=[
                    {"role": "system", "content": "You are a SQL analysis expert. Output ONLY valid YAML. NO explanations, NO thinking, NO tags, NO markdown formatting. DO NOT use <think> tags. DO NOT explain your reasoning."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=64000
            )
            
            content = response.choices[0].message.content.strip()
            safe_print(f"✓ API call successful")
            
            # Clean the response to extract only YAML content
            cleaned_content = clean_llm_response(content)
            if cleaned_content:
                safe_print(f"✓ Response cleaned successfully")
                return cleaned_content
            else:
                safe_print(f"✗ No valid YAML content found in response")
                return ""
            
        except Exception as e:
            last_error = e
            safe_print(f"✗ API call failed (attempt {retry_count + 1}): {str(e)}")
            if retry_count < max_retries - 1:
                time.sleep(2 ** retry_count)  # Exponential backoff
    
    if last_error:
        safe_print(f"Failed after {max_retries} attempts. Last error: {str(last_error)}")
    return ""  # Return empty string instead of None

async def process_table_columns(client, numbered_content, table, idx, tables_length, max_retries):
    """Process columns for a single table"""
    table_name = table.get('name')
    if not table_name:
        safe_print(f"  Skipping table at index {idx} with no name")
        return None
        
    safe_print(f"  Analyzing columns for table: {table_name} ({idx+1}/{tables_length})")
    
    pass2_prompt = f"""### TASK: PASS 2 - EXTRACT COLUMNS FOR TABLE '{table_name}'
IMPORTANT: You must output ONLY valid YAML. DO NOT include explanations, thoughts, or text outside the YAML.

Analyze this SQL file and identify ALL columns for the table '{table_name}'.
Focus EXCLUSIVELY on these elements in your YAML output:
1. ALL columns that appear in '{table_name}'

The SQL file below has line numbers prefixed (L1, L2, etc.).

SQL FILE:

{numbered_content}

Output MUST follow this EXACT structure, starting immediately with the "table:" key:
table:
  name: {table_name}
  columns:
    - name: column1
      data_type: string
    - name: column2
      data_type: integer"""

    pass2_result = make_llm_api_call(client, pass2_prompt, max_retries)
    if not pass2_result:
        safe_print(f"  Failed to get columns for table {table_name}, skipping")
        return None
        
    if not pass2_result.strip().startswith('table:'):
        pass2_result = 'table:\n' + pass2_result
        
    try:
        pass2_yaml = extract_yaml_between_codeblocks(pass2_result)
        pass2_data = safe_yaml_load(pass2_yaml)
        
        if not pass2_data or not isinstance(pass2_data, dict) or 'table' not in pass2_data:
            safe_print(f"  Failed to parse Pass 2 results for table {table_name}, attempting fallback")
            fallback_result = await extract_columns_fallback(client, numbered_content, table_name, max_retries)
            if fallback_result:
                return fallback_result
            safe_print(f"  Fallback also failed for table {table_name}, skipping")
            return None
            
        table_info = pass2_data.get('table', {})
        columns = table_info.get('columns', [])
        
        if not isinstance(columns, list):
            safe_print(f"  Columns is not a list for table {table_name}, got: {type(columns)}")
            columns = []
            
        column_names = []
        for col in columns:
            if isinstance(col, dict) and 'name' in col:
                column_names.append(col['name'])
            elif isinstance(col, str):
                column_names.append(col)
        
        # This return should be after the loop
        return {
            'table_name': table_name,
            'columns': column_names
        }
    except Exception as e:
        safe_print(f"  Error processing columns for table {table_name}: {str(e)}")
        return None

async def extract_columns_fallback(client, numbered_content, table_name, max_retries):
    """Fallback method to extract columns when structured YAML fails"""
    fallback_prompt = f"""### TASK: EXTRACT COLUMNS FOR TABLE '{table_name}'
IMPORTANT: Output ONLY a comma-separated list of column names. 
NO EXPLANATIONS. NO THINKING OUT LOUD. NO YAML STRUCTURE.

Look at the SQL file and identify ALL column names in table '{table_name}'.
Output ONLY a simple comma-separated list of column names, nothing else.

For example: column1,column2,column3,column4

SQL FILE:
{numbered_content}"""

    fallback_result = make_llm_api_call(client, fallback_prompt, max_retries)
    if not fallback_result:
        return None
        
    # Parse the comma-separated list
    try:
        # Remove any non-column text and cleanup
        column_text = fallback_result.strip()
        
        # Remove any explanation text (assume columns don't contain periods or explanation phrases)
        column_text = re.sub(r'.*?(?=\w+,\w+|$)', '', column_text, flags=re.DOTALL)
        
        # Additional filtering to get only the comma-separated part
        if ',' in column_text:
            # Find the part with the most commas, likely the actual list
            parts = re.split(r'[\n.:]', column_text)
            column_text = max(parts, key=lambda x: x.count(','))
            
        # Parse columns
        columns = [col.strip() for col in column_text.split(',') if col.strip()]
        
        if columns:
            return {
                'table_name': table_name,
                'columns': columns
            }
        return None
    except Exception as e:
        safe_print(f"  Error parsing fallback column list for {table_name}: {str(e)}")
        return None

async def extract_dependencies_multi_pass(file_path: str, api_key: str, max_retries: int = 3, concurrent: bool = False) -> Dict:
    """Extract dependencies from a SQL file using a refined four-pass approach."""
    empty_result_structure = {'tables': [], 'lineage': {'table_edges': [], 'column_edges': []}}
    try:
        file_content = read_sql_file(file_path)
        numbered_content = add_line_numbers(file_content)
        
        client = OpenAI(
            base_url="https://api.groq.com/openai/v1",
            api_key=api_key
        )
        
        safe_print(f"Starting four-pass analysis for {file_path}")
        
        # --- PASS 1: Extract tables ---
        tables_from_pass1 = await _run_pass1_table_extraction(client, numbered_content, max_retries, file_path)
        if not tables_from_pass1:
            safe_print(f"Pass 1 failed or found no tables for {file_path}")
            return empty_result_structure

        combined_results = {
            'tables': tables_from_pass1,
            'lineage': {'table_edges': [], 'column_edges': []}
        }
        
        # --- PASS 2: Extract columns for each table ---
        tables_with_columns = await _run_pass2_column_extraction(
            client, numbered_content, combined_results['tables'], concurrent, max_retries, file_path
        )
        combined_results['tables'] = tables_with_columns

        # --- PASS 3: Extract table relationships ---
        table_edges_from_pass3 = await _run_pass3_table_relationship_extraction(
            client, numbered_content, combined_results['tables'], max_retries, file_path
        )
        combined_results['lineage']['table_edges'] = table_edges_from_pass3

        # --- PASS 4: Extract column-level lineage ---
        column_edges_from_pass4 = await _run_pass4_column_lineage_extraction(
            client, numbered_content, combined_results['tables'], table_edges_from_pass3, concurrent, max_retries, file_path
        )
        combined_results['lineage']['column_edges'] = column_edges_from_pass4
        
        safe_print(f"Four-pass analysis complete for {file_path}")
        safe_print(f"  Tables: {len(combined_results['tables'])}")
        safe_print(f"  Table relationships: {len(combined_results['lineage']['table_edges'])}")
        safe_print(f"  Column relationships: {len(combined_results['lineage']['column_edges'])}")
        
        return combined_results
        
    except Exception as e:
        safe_print(f"Error in multi-pass analysis for {file_path}: {str(e)}")
        return empty_result_structure

async def _run_pass1_table_extraction(
    client: OpenAI, numbered_content: str, max_retries: int, file_path: str
) -> Optional[List[Dict[str, Any]]]:
    """Runs Pass 1 of multi-pass: Extract tables only."""
    safe_print(f"Pass 1: Extracting tables only for {file_path}...")
    pass1_prompt = f"""### TASK: PASS 1 - EXTRACT TABLES ONLY
IMPORTANT: Output ONLY valid YAML. DO NOT include ANY explanations, thoughts, or text outside the YAML.
NO EXPLANATIONS. NO THINKING OUT LOUD. NO MARKDOWN CODE BLOCKS.

EXTRACT ALL TABLES THAT APPEAR IN THE SQL FILE.
EXCLUDE CTEs (Common Table Expressions defined with WITH clause) and any intermediate temporary results.
ONLY include actual database tables that are read from (FROM/JOIN clauses) or written to (INSERT/CREATE statements).
EXCLUDE jinja's variables.
DO NOT include any relationships between tables in this pass.

The SQL file below has line numbers prefixed (L1, L2, etc.).

SQL FILE:

{numbered_content}

Output MUST follow this EXACT structure, starting immediately with the "tables:" key:
tables:
  - id: source_table_name
    name: source_table_name
    role: source
  - id: target_table_name
    name: target_table_name
    role: target"""

    pass1_result_str = make_llm_api_call(client, pass1_prompt, max_retries)
    if not pass1_result_str:
        safe_print(f"Pass 1 LLM call failed for {file_path}")
        return None
    
    pass1_yaml = extract_yaml_between_codeblocks(pass1_result_str)
    pass1_data = safe_yaml_load(pass1_yaml)
    
    if not pass1_data or not isinstance(pass1_data, dict):
        safe_print(f"Failed to parse Pass 1 YAML for {file_path}. Raw: {pass1_result_str[:200]}...")
        return None
        
    tables = pass1_data.get('tables', [])
    if not isinstance(tables, list):
        safe_print(f"Tables is not a list in Pass 1 results for {file_path}, got: {type(tables)}")
        return None 
    
    safe_print(f"Pass 1 complete for {file_path}: Found {len(tables)} tables")
    return tables

async def _run_pass2_column_extraction(
    client: OpenAI, 
    numbered_content: str, 
    tables: List[Dict[str, Any]], 
    concurrent: bool, 
    max_retries: int,
    file_path: str
) -> List[Dict[str, Any]]:
    """Runs Pass 2 of multi-pass: Extract columns for each table."""
    safe_print(f"Pass 2: Extracting columns for {len(tables)} tables in {file_path}...")
    
    if not tables:
        return []

    tables_dict = {t.get('name'): t for t in tables if t.get('name')}
    processed_tables_count = 0

    if concurrent:
        table_batch_size = 8
        num_table_batches = (len(tables) + table_batch_size - 1) // table_batch_size
        safe_print(f"  Processing {len(tables)} tables in {num_table_batches} batches of up to {table_batch_size} concurrently for {file_path}.")

        for i in range(0, len(tables), table_batch_size):
            current_batch_tables = tables[i:i + table_batch_size]
            tasks = []
            for table_obj in current_batch_tables:
                if table_obj.get('name'): 
                    tasks.append(process_table_columns(client, numbered_content, table_obj, 
                                                       processed_tables_count + len(tasks), # Adjust index for logging
                                                       len(tables), max_retries))
            
            if tasks:
                safe_print(f"    Gathering columns for batch {i//table_batch_size + 1}/{num_table_batches} ({len(tasks)} tables) for {file_path}")
                table_column_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result_or_exc in table_column_results:
                    if isinstance(result_or_exc, Exception):
                        safe_print(f"    Error processing a table in batch: {str(result_or_exc)}")
                        continue # Skip this table's result
                    
                    if result_or_exc and result_or_exc.get('table_name') in tables_dict:
                        table_name = result_or_exc['table_name']
                        columns = result_or_exc['columns']
                        tables_dict[table_name]['columns'] = columns
                        safe_print(f"    Added {len(columns)} columns to table {table_name} in {file_path}")
                processed_tables_count += len(tasks)
            
            if i + table_batch_size < len(tables):
                safe_print(f"    Delaying before next batch of tables for {file_path}...")
                await asyncio.sleep(5) # Delay between batches to manage load

    else: # Sequential processing (remains unchanged, no explicit batching needed)
        for idx, table_obj in enumerate(tables):
            if table_obj.get('name'): 
                result = await process_table_columns(client, numbered_content, table_obj, idx, len(tables), max_retries)
                if result and result.get('table_name') in tables_dict:
                    table_name = result['table_name']
                    columns = result['columns']
                    tables_dict[table_name]['columns'] = columns
                    safe_print(f"  Added {len(columns)} columns to table {table_name} in {file_path}")
    
    updated_tables = [tables_dict[t.get('name')] for t in tables if t.get('name') and t.get('name') in tables_dict]
    return updated_tables

async def _run_pass3_table_relationship_extraction(
    client: OpenAI, 
    numbered_content: str, 
    tables_with_columns: List[Dict[str, Any]], 
    max_retries: int,
    file_path: str
) -> List[Dict[str, Any]]: 
    """Runs Pass 3 of multi-pass: Extract table relationships."""
    safe_print(f"Pass 3: Extracting table relationships for {file_path}...")
    table_names = [t.get('name') for t in tables_with_columns if t.get('name')]
    if not table_names:
        safe_print(f"No table names available for Pass 3 in {file_path}, skipping.")
        return [] 

    table_names_list_str = ", ".join([f"'{name}'" for name in table_names])
    
    pass3_prompt = f"""### TASK: PASS 3 - EXTRACT TABLE RELATIONSHIPS
IMPORTANT: Output ONLY valid YAML. DO NOT include ANY explanations, thoughts, or text outside the YAML.

Analyze this SQL file and identify how the following tables are related to each other:
TABLE LIST: {table_names_list_str}

CRITICAL CONSTRAINT: ONLY use tables from the TABLE LIST above. 
DO NOT include CTEs (Common Table Expressions defined with WITH clause) or intermediate temporary results in your relationships.

Specifically focus on:
1. Which tables are source tables (read from) - ONLY from the TABLE LIST
2. Which tables are target tables (written to) - ONLY from the TABLE LIST
3. How these tables relate to each other (joins, inserts, etc.)
4. The exact line numbers where these relationships appear

DO NOT extract column-level relationships in this pass.

The SQL file below has line numbers prefixed (L1, L2, etc.).

SQL FILE:

{numbered_content}

Output MUST follow this EXACT structure, starting immediately with the "table_edges:" key:
table_edges:
  - from_table: source_table_name
    to_table: target_table_name
    transformation_type: join|filter|aggregate|insert|etc
    transformation_lines:
      start_line: 10
      end_line: 20"""

    pass3_result_str = make_llm_api_call(client, pass3_prompt, max_retries)
    if not pass3_result_str:
        safe_print(f"Pass 3 LLM call failed for {file_path}")
        return []
    
    pass3_yaml = extract_yaml_between_codeblocks(pass3_result_str)
    pass3_data = safe_yaml_load(pass3_yaml)
    
    if not pass3_data or not isinstance(pass3_data, dict):
        safe_print(f"Failed to parse Pass 3 YAML for {file_path}. Raw: {pass3_result_str[:200]}...")
        return []
        
    table_edges = pass3_data.get('table_edges', [])
    if not isinstance(table_edges, list):
        safe_print(f"Table edges is not a list in Pass 3 results for {file_path}, got: {type(table_edges)}")
        return []
    
    # Filter out any relationships that include tables not in our validated table list
    valid_table_names = set(table_names)
    filtered_edges = []
    
    for edge in table_edges:
        if not isinstance(edge, dict):
            continue
            
        from_table = edge.get('from_table', '').strip()
        to_table = edge.get('to_table', '').strip()
        
        # Only include relationships where both tables are in our validated list
        if from_table in valid_table_names and to_table in valid_table_names:
            filtered_edges.append(edge)
        else:
            safe_print(f"  Filtered out relationship: {from_table} -> {to_table} (contains CTE or invalid table)")
    
    safe_print(f"Pass 3 complete for {file_path}: Found {len(filtered_edges)} valid table relationships (filtered {len(table_edges) - len(filtered_edges)} invalid ones)")
    return filtered_edges

async def _run_pass4_column_lineage_extraction(
    client: OpenAI, 
    numbered_content: str, 
    tables_with_columns: List[Dict[str, Any]], 
    table_edges: List[Dict[str, Any]], 
    concurrent: bool, 
    max_retries: int,
    file_path: str
) -> List[Dict[str, Any]]:
    """Runs Pass 4 of multi-pass: Extract column-level lineage."""
    safe_print(f"Pass 4: Extracting column-level lineage for {file_path}...")
    
    if not table_edges:
        safe_print(f"No table relationships found for Pass 4 in {file_path}, skipping.")
        return []
    
    # Collect all tables and their columns
    all_tables_info = {}
    for table in tables_with_columns:
        table_name = table.get('name')
        if table_name and 'columns' in table:
            all_tables_info[table_name] = table.get('columns', [])
    
    # Generate table-column pairs that need lineage analysis
    column_lineage_pairs = []
    
    for edge in table_edges:
        from_table = edge.get('from_table') or edge.get('from')
        to_table = edge.get('to_table') or edge.get('to')
        
        if from_table in all_tables_info and to_table in all_tables_info:
            column_lineage_pairs.append((from_table, to_table))
    
    safe_print(f"Found {len(column_lineage_pairs)} table pairs for column lineage analysis")
    
    if not column_lineage_pairs:
        return []
    
    # Process column lineage in batches
    batch_size = 5
    from_tables = []
    to_tables = []
    for edge in table_edges:
        from_table = edge.get('from_table') or edge.get('from')
        to_table = edge.get('to_table') or edge.get('to')
        if from_table in all_tables_info and to_table in all_tables_info:
            from_tables.append(from_table)
            to_tables.append(to_table)
    
    total_pairs = len(from_tables)
    batches = [(from_tables[i:i + batch_size], to_tables[i:i + batch_size]) 
               for i in range(0, len(from_tables), batch_size)]
    
    safe_print(f"Processing {total_pairs} table pairs in {len(batches)} batches")
    
    all_column_edges = []
    
    if concurrent:
        # Process batches concurrently
        tasks = []
        for idx, (batch_from, batch_to) in enumerate(batches):
            tasks.append(_process_column_lineage_batch(
                client, numbered_content, all_tables_info, batch_from, batch_to, idx, len(batches), max_retries
            ))
        
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Add all column edges to results
        for result_or_exc in batch_results:
            if isinstance(result_or_exc, Exception):
                safe_print(f"Error processing column lineage batch: {str(result_or_exc)}")
                continue
            if isinstance(result_or_exc, list):
                all_column_edges.extend(result_or_exc)
    else:
        # Process batches sequentially
        for idx, (batch_from, batch_to) in enumerate(batches):
            edges = await _process_column_lineage_batch(
                client, numbered_content, all_tables_info, batch_from, batch_to, idx, len(batches), max_retries
            )
            if edges:
                all_column_edges.extend(edges)
    
    safe_print(f"Pass 4 complete for {file_path}: Found {len(all_column_edges)} column relationships")
    return all_column_edges

async def _process_column_lineage_batch(
    client: OpenAI, 
    numbered_content: str, 
    all_tables_info: Dict[str, List[str]],
    from_tables: List[str], 
    to_tables: List[str], 
    idx: int, 
    total_pairs: int, 
    max_retries: int
) -> List[Dict[str, Any]]:
    """Process column lineage for multiple table pairs in a single call"""
    # Initialize circuit breaker state if not exists
    if not hasattr(_process_column_lineage_batch, 'circuit_state'):
        _process_column_lineage_batch.circuit_state = {
            'failures': 0,
            'last_failure_time': 0,
            'is_open': False
        }
    
    # Get current time for circuit breaker
    now = time.time()
    
    try:
        safe_print(f"  Analyzing column lineage for batch {idx+1}/{total_pairs} ({len(from_tables)} pairs)")
        
        # Check circuit breaker
        if _process_column_lineage_batch.circuit_state['is_open']:
            if now - _process_column_lineage_batch.circuit_state['last_failure_time'] < 300:  # 5 minutes cooldown
                safe_print("  Circuit breaker is open, skipping batch")
                return []
            else:
                _process_column_lineage_batch.circuit_state['is_open'] = False
                _process_column_lineage_batch.circuit_state['failures'] = 0
        
        table_pairs_info = []
        for from_table, to_table in zip(from_tables, to_tables):
            from_columns = ", ".join([f"'{col}'" for col in all_tables_info.get(from_table, [])])
            to_columns = ", ".join([f"'{col}'" for col in all_tables_info.get(to_table, [])])
            table_pairs_info.append(f"""
SOURCE TABLE: '{from_table}'
COLUMNS: {from_columns}

TARGET TABLE: '{to_table}'
COLUMNS: {to_columns}
""")
        
        table_pairs_str = "\n---\n".join(table_pairs_info)
        
        pass4_prompt = f"""### TASK: PASS 4 - EXTRACT COLUMN LINEAGE FOR MULTIPLE TABLE PAIRS
IMPORTANT: Output ONLY valid YAML. DO NOT include ANY explanations, thoughts, or text outside the YAML.
NO EXPLANATIONS. NO THINKING OUT LOUD. NO MARKDOWN CODE BLOCKS.

Analyze this SQL file and identify how columns from each source table relate to columns in their respective target tables.

ANALYZE THE FOLLOWING TABLE PAIRS:
{table_pairs_str}

Focus EXCLUSIVELY on:
1. Which specific columns from source tables are used to create which specific columns in target tables
2. The exact transformation type applied (direct copy, case when, concat, math operation, etc.)
3. The exact line numbers where these column transformations occur

The SQL file below has line numbers prefixed (L1, L2, etc.).

SQL FILE:

{numbered_content}

Output MUST follow this EXACT structure, starting immediately with the "column_edges:" key:
column_edges:
  - from_table: source_table_name
    from_column: source_col_name
    to_table: target_table_name
    to_column: target_col_name
    transformation_type: direct|case_when|concat|math|etc
    transformation_lines:
      start_line: 10
      end_line: 20"""

        # Add delay to prevent rate limiting
        await asyncio.sleep(2)
        
        pass4_result_str = make_llm_api_call(client, pass4_prompt, max_retries)
        if not pass4_result_str:
            safe_print(f"  Failed to get column lineage for batch {idx+1}, skipping")
            return []
            
        # Parse pass 4 results
        pass4_yaml = extract_yaml_between_codeblocks(pass4_result_str)
        pass4_data = safe_yaml_load(pass4_yaml)
        
        if not pass4_data or not isinstance(pass4_data, dict):
            safe_print(f"  Failed to parse Pass 4 results for batch {idx+1}, skipping")
            return []
            
        # Extract column edges
        column_edges = pass4_data.get('column_edges', [])
        if not isinstance(column_edges, list):
            safe_print(f"  Column edges is not a list for batch {idx+1}, got: {type(column_edges)}")
            column_edges = []
            
        safe_print(f"  Added {len(column_edges)} column edges from batch {idx+1}")
        return column_edges
        
    except Exception as e:
        # Update circuit breaker state
        _process_column_lineage_batch.circuit_state['failures'] += 1
        _process_column_lineage_batch.circuit_state['last_failure_time'] = now
        
        # Open circuit breaker if too many failures
        if _process_column_lineage_batch.circuit_state['failures'] >= 3:
            _process_column_lineage_batch.circuit_state['is_open'] = True
            safe_print("  Circuit breaker opened due to multiple failures")
        
        safe_print(f"  Error processing column lineage batch {idx+1}: {str(e)}")
        return []

class LineageService:
    def __init__(self, use_cache: bool = True):
        self.api_key = os.getenv('GROQ_API_KEY')
        if not self.api_key:
            raise ValueError("GROQ_API_KEY environment variable is not set")
        
        # Cache control
        self.use_cache = use_cache
        
        # Initialize cache directory
        self.output_base = os.path.join(os.getcwd(), 'lineage_output')
        self.cache_dir = os.path.join(self.output_base, '.cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Load existing cache (only if cache is enabled)
        if self.use_cache:
            self.cache = load_cache(self.cache_dir)
            safe_print(f"Loaded cache with {len(self.cache)} entries")
        else:
            self.cache = {}
            safe_print("Cache disabled - processing all files fresh")

        # Initialize version
        self.version = "1.0.0"

        # Initialize progress tracking
        self.progress_data = {}

    def _should_process_file(self, file_path: str, file_hash: str, cache_key: Optional[str] = None) -> Tuple[bool, str]:
        """
        Determine if a file should be processed based on cache only.
        Uses cache_key if provided, otherwise defaults to file_path.
        """
        # If cache is disabled, always process files
        if not self.use_cache:
            safe_print(f"\nCache disabled - processing file: {cache_key or file_path}")
            return True, "Cache disabled (FULL mode)"
        
        key_to_check = cache_key if cache_key is not None else str(file_path)
        
        safe_print(f"\nChecking cache for key: {key_to_check}")
        safe_print(f"File hash (content-only): {file_hash}")
        
        if key_to_check in self.cache:
            cached_entry = self.cache[key_to_check]
            cached_hash = cached_entry.get('hash')
            safe_print(f"Cached hash: {cached_hash}")
            safe_print(f"Hash match: {cached_hash == file_hash}")
            
            if cached_hash == file_hash:
                return False, "File content unchanged (based on cache)"
            return True, "File content changed (based on cache)"
        else:
            safe_print(f"Key not in cache: {key_to_check}")
            return True, "File not in cache"

    async def report_extraction_run(self, run_id: str, repo_url: str, branch: str, commit_hash: str, 
                                  commit_timestamp: str, run_mode: RunMode, phase: ExtractionPhase, 
                                  triggered_by: str, stats: Optional[StatsDto] = None) -> Dict[str, Any]:
        """
        Report an extraction run to the lineage service.
        """
        try:
            extraction_run = ExtractionRunRequestDto(
                run_id=run_id,
                run_mode=run_mode,
                phase=phase,
                repository=RepositoryDto(
                    url=repo_url,
                    branch=branch,
                    commit_hash=commit_hash,
                    commit_timestamp=commit_timestamp
                ),
                triggered_by=triggered_by,
                extractor_version=self.version,
                started_at=datetime.utcnow().isoformat() if phase == ExtractionPhase.STARTED else None,
                finished_at=datetime.utcnow().isoformat() if phase in [ExtractionPhase.COMPLETED, ExtractionPhase.FAILED] else None,
                stats=stats
            )
            
            # Console logging for standalone use (API calls disabled)
            safe_print(f"=== EXTRACTION RUN REPORT ===")
            safe_print(f"Run ID: {run_id}")
            safe_print(f"Mode: {run_mode}")
            safe_print(f"Phase: {phase}")
            safe_print(f"Repository: {repo_url}")
            safe_print(f"Branch: {branch}")
            safe_print(f"Commit: {commit_hash}")
            safe_print(f"Triggered by: {triggered_by}")
            safe_print(f"Version: {self.version}")
            if stats:
                safe_print(f"Stats: {stats.dict()}")
            safe_print(f"===============================")
            
            # API call disabled for standalone use
            # async with aiohttp.ClientSession() as session:
            #     async with session.post(
            #         'http://127.0.0.1:8082/api/mock/extraction-run',
            #         json=extraction_run.dict(),
            #         headers={'Content-Type': 'application/json'}
            #     ) as response:
            #         response_data = await response.json()
            #         if response.status != 200:
            #             safe_print(f"Error reporting extraction run: {response_data}")
            #             return {"status": "error", "message": str(response_data)}
            #         return {"status": "success", "data": response_data}
            
            return {"status": "success", "data": "console_logged"}
            
        except Exception as e:
            safe_print(f"Error reporting extraction run: {str(e)}")
            return {"status": "error", "message": str(e)}

    async def report_file_extraction(self, run_id: str, file_path: str, file_url: str, 
                                   tables: List[TableBlockDto], table_edges: List[TableEdgeDto], 
                                   column_edges: List[ColumnEdgeDto], 
                                   job_meta: Optional[JobMetaDto] = None,
                                   raw_sql: Optional[str] = None) -> Dict[str, Any]:
        """
        Report a file extraction to the lineage service.
        """
        try:
            file_extraction = FileExtractionRequestDto(
                run_id=run_id,
                file=FileMetaDto(
                    id=f"sha256:{get_file_hash(file_path)}",
                    path=file_path,
                    file_type="SQL" if file_path.endswith('.sql') else "JINJA2",
                    url=file_url,
                    last_modified_at=datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
                ),
                job=job_meta,
                tables=tables,
                table_edges=table_edges,
                column_edges=column_edges,
                raw_sql_snippet=raw_sql,
                extracted_at=datetime.utcnow().isoformat()
            )
            
            # Console logging for standalone use (API calls disabled)
            safe_print(f"=== FILE EXTRACTION REPORT ===")
            safe_print(f"Run ID: {run_id}")
            safe_print(f"File: {file_path}")
            safe_print(f"File Type: {file_extraction.file.file_type}")
            safe_print(f"Tables found: {len(tables)}")
            safe_print(f"Table relationships: {len(table_edges)}")
            safe_print(f"Column relationships: {len(column_edges)}")
            safe_print(f"Extracted at: {file_extraction.extracted_at}")
            safe_print(f"===============================")
            
            # API call disabled for standalone use
            # async with aiohttp.ClientSession() as session:
            #     async with session.post(
            #         'http://127.0.0.1:8082/api/mock/file-extraction',
            #         json=file_extraction.dict(),
            #         headers={'Content-Type': 'application/json'}
            #     ) as response:
            #         response_data = await response.json()
            #         if response.status != 200:
            #             safe_print(f"Error reporting file extraction: {response_data}")
            #             return {"status": "error", "message": str(response_data)}
            #         return {"status": "success", "data": response_data}
            
            return {"status": "success", "data": "console_logged"}
            
        except Exception as e:
            safe_print(f"Error reporting file extraction: {str(e)}")
            return {"status": "error", "message": str(e)}

    async def report_progress(self, run_id: str, total_files: int, processed_files: int, 
                            current_file: Optional[str] = None, start_time: Optional[float] = None,
                            current_phase: str = "processing", error_count: int = 0,
                            last_error: Optional[str] = None) -> Dict[str, Any]:
        """
        Report progress to the lineage service.
        """
        try:
            now = time.time()
            
            # Calculate processing speed and estimated time remaining
            processing_speed = None
            estimated_time_remaining = None
            
            if start_time and processed_files > 0:
                elapsed_time = now - start_time
                processing_speed = processed_files / elapsed_time
                
                if processing_speed > 0:
                    remaining_files = total_files - processed_files
                    estimated_time_remaining = remaining_files / processing_speed
            
            progress_status = ProgressStatus(
                run_id=run_id,
                total_files=total_files,
                processed_files=processed_files,
                current_file=current_file,
                estimated_time_remaining=estimated_time_remaining,
                processing_speed=processing_speed,
                current_phase=current_phase,
                error_count=error_count,
                last_error=last_error,
                timestamp=datetime.utcnow().isoformat()
            )
            
            # Console logging for standalone use (API calls disabled)
            safe_print(f"=== PROGRESS UPDATE ===")
            safe_print(f"Run ID: {run_id}")
            safe_print(f"Progress: {processed_files}/{total_files} files")
            safe_print(f"Current file: {current_file or 'N/A'}")
            safe_print(f"Phase: {current_phase}")
            safe_print(f"Processing speed: {processing_speed:.2f} files/sec" if processing_speed else "Processing speed: N/A")
            safe_print(f"ETA: {estimated_time_remaining:.1f} seconds" if estimated_time_remaining else "ETA: N/A")
            safe_print(f"Errors: {error_count}")
            if last_error:
                safe_print(f"Last error: {last_error}")
            safe_print(f"=========================")
            
            # API call disabled for standalone use
            # async with aiohttp.ClientSession() as session:
            #     async with session.post(
            #         'http://127.0.0.1:8082/api/mock/progress-update',
            #         json=progress_status.dict(),
            #         headers={'Content-Type': 'application/json'}
            #     ) as response:
            #         response_data = await response.json()
            #         if response.status != 200:
            #             safe_print(f"Error reporting progress: {response_data}")
            #             return {"status": "error", "message": str(response_data)}
            #         return {"status": "success", "data": response_data}
            
            return {"status": "success", "data": "console_logged"}
            
        except Exception as e:
            safe_print(f"Error reporting progress: {str(e)}")
            return {"status": "error", "message": str(e)}

    async def process_single_file(self, file_path: str, cache_key: Optional[str] = None, 
                                run_id: Optional[str] = None, repo_url: Optional[str] = None,
                                file_url: Optional[str] = None) -> Dict[str, Any]:
        """
        Process a single file with enhanced caching and reporting.
        """
        try:
            str_file_path = str(file_path)
            file_hash = get_file_hash(str_file_path)
            yaml_output_file = get_yaml_output_path(str_file_path, is_repo_analysis=False)

            should_process, reason = self._should_process_file(str_file_path, file_hash, cache_key=cache_key)
            
            if not should_process:
                safe_print(f"Skipping: {reason} for key {cache_key} (file: {str_file_path})")
                yaml_path_from_cache = self.cache.get(cache_key, {}).get('yaml_path', yaml_output_file)
                return {
                    "status": "skipped",
                    "message": reason,
                    "file_path": str_file_path,
                    "cache_key": cache_key,
                    "yaml_output_path": yaml_path_from_cache
                }
            
            safe_print(f"Processing: {reason} for key {cache_key} (file: {str_file_path})")
            
            dependencies = await extract_dependencies_multi_pass(
                file_path=str_file_path,
                api_key=self.api_key,
                max_retries=3,
                concurrent=False
            )
            
            if not dependencies or not dependencies.get('tables'):
                safe_print(f"No tables/dependencies extracted for {str_file_path}")
                self.cache[cache_key] = {
                    'hash': file_hash,
                    'yaml_path': None,
                    'status': 'no_dependencies_found'
                }
                save_cache(self.cache_dir, self.cache)
                return {"status": "warning", "message": "No tables/dependencies found or extracted", "file_path": str_file_path, "cache_key": cache_key}
            
            # Convert to YAML and save
            yaml_content = yaml.dump(dependencies, sort_keys=False, indent=2)
            yaml_output_path = save_yaml_output(yaml_content, str_file_path, is_repo_analysis=False)
            
            # Update cache
            self.cache[cache_key] = {
                'hash': file_hash,
                'yaml_path': yaml_output_path,
                'status': 'success'
            }
            save_cache(self.cache_dir, self.cache)
            
            # Report file extraction if run_id is provided
            if run_id:
                try:
                    # Convert dependencies to DTOs for reporting
                    tables_dto = []
                    for table in dependencies.get('tables', []):
                        tables_dto.append(TableBlockDto(
                            raw=table.get('name', ''),
                            role=table.get('role', ''),
                            columns=table.get('columns', [])
                        ))
                    
                    table_edges_dto = []
                    for edge in dependencies.get('lineage', {}).get('table_edges', []):
                        table_edges_dto.append(TableEdgeDto(
                            from_table=edge.get('from_table', ''),
                            to_table=edge.get('to_table', ''),
                            transformation_type=edge.get('transformation_type', ''),
                            transformation_code='',  # Could extract from transformation_lines
                            code_lines=CodeLinesDto(
                                start_line=edge.get('transformation_lines', {}).get('start_line', 0),
                                end_line=edge.get('transformation_lines', {}).get('end_line', 0)
                            )
                        ))
                    
                    column_edges_dto = []
                    for edge in dependencies.get('lineage', {}).get('column_edges', []):
                        column_edges_dto.append(ColumnEdgeDto(
                            from_table=edge.get('from_table', ''),
                            from_column=edge.get('from_column', ''),
                            to_table=edge.get('to_table', ''),
                            to_column=edge.get('to_column', ''),
                            transformation_type=edge.get('transformation_type', ''),
                            transformation_code='',  # Could extract from transformation_lines
                            code_lines=CodeLinesDto(
                                start_line=edge.get('transformation_lines', {}).get('start_line', 0),
                                end_line=edge.get('transformation_lines', {}).get('end_line', 0)
                            )
                        ))
                    
                    await self.report_file_extraction(
                        run_id=run_id,
                        file_path=str_file_path,
                        file_url=file_url or f"file://{str_file_path}",
                        tables=tables_dto,
                        table_edges=table_edges_dto,
                        column_edges=column_edges_dto,
                        raw_sql=read_sql_file(str_file_path)
                    )
                except Exception as e:
                    safe_print(f"Error reporting file extraction: {str(e)}")
            
            # Prepare tables and lineage edges for webhook
            tables_data = []
            for table in dependencies.get('tables', []):
                tables_data.append({
                    "name": table.get('name', ''),
                    "role": table.get('role', ''),
                    "schema": table.get('schema', ''),
                    "columns": table.get('columns', [])
                })
            
            lineage_edges_data = []
            # Add table edges
            for edge in dependencies.get('lineage', {}).get('table_edges', []):
                lineage_edges_data.append({
                    "from_table": edge.get('from_table', ''),
                    "to_table": edge.get('to_table', ''),
                    "edge_type": "table_edge",
                    "transformation_type": edge.get('transformation_type', ''),
                    "transformation_lines": edge.get('transformation_lines', {}),
                    "transformation_code": ""
                })
            
            # Add column edges
            for edge in dependencies.get('lineage', {}).get('column_edges', []):
                lineage_edges_data.append({
                    "from_table": edge.get('from_table', ''),
                    "from_column": edge.get('from_column', ''),
                    "to_table": edge.get('to_table', ''),
                    "to_column": edge.get('to_column', ''),
                    "edge_type": "column_edge",
                    "transformation_type": edge.get('transformation_type', ''),
                    "transformation_lines": edge.get('transformation_lines', {}),
                    "transformation_code": edge.get('transformation_code', '')
                })
            
            return {
                "status": "success",
                "message": "File processed successfully",
                "file_path": str_file_path,
                "cache_key": cache_key,
                "yaml_output_path": yaml_output_path,
                "tables_count": len(dependencies.get('tables', [])),
                "table_edges_count": len(dependencies.get('lineage', {}).get('table_edges', [])),
                "column_edges_count": len(dependencies.get('lineage', {}).get('column_edges', [])),
                "tables": tables_data,
                "lineage_edges": lineage_edges_data
            }
            
        except Exception as e:
            safe_print(f"Error processing file {file_path}: {str(e)}")
            return {
                "status": "error",
                "message": str(e),
                "file_path": str_file_path,
                "cache_key": cache_key
            }



def extract_dependencies_multi_pass_sync(file_path: str, api_key: str, max_retries: int = 3, concurrent: bool = False) -> str:
    """Synchronous wrapper for the proper 4-pass multi-pass extraction."""
    try:
        # Use the actual async multi-pass function
        result = asyncio.run(extract_dependencies_multi_pass(file_path, api_key, max_retries, concurrent))
        
        # Convert dictionary result to YAML string
        if isinstance(result, dict):
            return yaml.dump(result, sort_keys=False, indent=2)
        return result
        
    except Exception as e:
        safe_print(f"Error in multi-pass analysis for {file_path}: {str(e)}")
        return ""

def process_chunked_file_sync(file_path: str, api_key: str, max_retries: int = 3, use_multi_pass: bool = True, concurrent: bool = False) -> str:
    """Synchronous chunked file processing."""
    try:
        file_content = read_sql_file(file_path)
        if not file_content:
            return ""
        
        # Use multi-pass approach for chunked files
        return extract_dependencies_multi_pass_sync(file_path, api_key, max_retries, concurrent)
        
    except Exception as e:
        safe_print(f"Error in chunked processing for {file_path}: {str(e)}")
        return ""

def clean_llm_response(response_text: str) -> str:
    """Clean LLM response by removing think tags and extracting only YAML content."""
    if not response_text:
        return ""
    
    # Remove <think> tags and their content
    response_text = re.sub(r'<think>.*?</think>', '', response_text, flags=re.DOTALL)
    
    # Remove markdown code blocks but keep their content
    response_text = re.sub(r'^```ya?ml\s*', '', response_text, flags=re.MULTILINE)
    response_text = re.sub(r'^```\s*$', '', response_text, flags=re.MULTILINE)
    
    # Remove any HTML-like tags
    response_text = re.sub(r'<[^>]*>', '', response_text)
    
    # Find the YAML content by looking for key indicators
    lines = response_text.split('\n')
    yaml_lines = []
    in_yaml = False
    
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
            
        # If we see YAML indicators, we're in YAML territory
        if stripped.startswith(('tables:', 'lineage:', 'table_edges:', 'column_edges:', '-', '  ')):
            in_yaml = True
            yaml_lines.append(line)
        # If line contains a colon (potential YAML key-value), include it
        elif ':' in stripped and not stripped.startswith(('#', '//', '/*')):
            in_yaml = True
            yaml_lines.append(line)
        # If we're already in YAML section and this looks like continued content, include it
        elif in_yaml and (stripped.startswith(' ') or stripped.startswith('-')):
            yaml_lines.append(line)
        # Otherwise it's probably an explanation/thinking, skip it
    
    return '\n'.join(yaml_lines)

if __name__ == "__main__":
    # Setup command-line interface
    parser = argparse.ArgumentParser(description='Extract SQL dependencies with multi-pass analysis')
    parser.add_argument('folder_path', nargs='?', default=".", help='Path to folder containing SQL files')
    parser.add_argument('--api-key', default=os.environ.get('GROQ_API_KEY'), 
                        help='Groq API key (defaults to GROQ_API_KEY env variable)')
    parser.add_argument('--output', default='accumulated_dependencies.yaml', 
                        help='Output file for accumulated dependencies')
    parser.add_argument('--workers', type=int, default=4, 
                        help='Number of parallel workers (default: 4)')

    parser.add_argument('--sequential', action='store_true', 
                        help='Use sequential processing within multi-pass (slower but uses less API quota)')
    parser.add_argument('--file', help='Process a single file instead of a folder')
    
    args = parser.parse_args()
    
    # Validate API key
    if not args.api_key:
        raise ValueError("Please provide a Groq API key using --api-key or set the GROQ_API_KEY environment variable")
    
    # Process options
    concurrent_multi_pass = not args.sequential
    
    # Print configuration
    print("\n========== SQL Dependency Extractor ==========")
    print(f"API Key: {args.api_key[:5]}...{args.api_key[-5:]}")
    if args.file:
        print(f"Processing single file: {args.file}")
    else:
        print(f"Processing folder: {args.folder_path}")
        print(f"Output file: {args.output}")
        print(f"Workers: {args.workers}")
    print(f"Analysis mode: Multi-pass")
    print(f"Multi-pass concurrency: {'Enabled' if concurrent_multi_pass else 'Disabled'}")
    print("==============================================\n")
    
    # Process either a single file or a folder
    if args.file:
        # Process a single file using LineageService for enhanced functionality
        try:
            # Set environment variable for LineageService
            os.environ['GROQ_API_KEY'] = args.api_key
            
            lineage_service = LineageService()
            
            async def main_single_file():
                # Generate a run_id for standalone use
                run_id = f"standalone_{int(time.time())}"
                
                # Report extraction run started
                await lineage_service.report_extraction_run(
                    run_id=run_id,
                    repo_url="standalone",
                    branch="standalone",
                    commit_hash="standalone",
                    commit_timestamp=datetime.utcnow().isoformat(),
                    run_mode=RunMode.FULL,
                    phase=ExtractionPhase.STARTED,
                    triggered_by="command_line"
                )
                
                # Report progress started
                await lineage_service.report_progress(
                    run_id=run_id,
                    total_files=1,
                    processed_files=0,
                    current_file=args.file,
                    start_time=time.time(),
                    current_phase="processing"
                )
                
                # Process the file with run_id
                result = await lineage_service.process_single_file(
                    file_path=args.file,
                    run_id=run_id,
                    file_url=f"file://{os.path.abspath(args.file)}"
                )
                
                # Report progress completed
                await lineage_service.report_progress(
                    run_id=run_id,
                    total_files=1,
                    processed_files=1,
                    current_file=args.file,
                    start_time=time.time(),
                    current_phase="completed"
                )
                
                # Report extraction run completed
                stats = StatsDto(
                    total_files=1,
                    processed=1,
                    succeeded=1 if result.get("status") == "success" else 0,
                    failed=0 if result.get("status") == "success" else 1
                )
                
                await lineage_service.report_extraction_run(
                    run_id=run_id,
                    repo_url="standalone",
                    branch="standalone",
                    commit_hash="standalone",
                    commit_timestamp=datetime.utcnow().isoformat(),
                    run_mode=RunMode.FULL,
                    phase=ExtractionPhase.COMPLETED,
                    triggered_by="command_line",
                    stats=stats
                )
                
                if result.get("status") == "success" or result.get("status") == "skipped":
                    safe_print(f"\nProcessing complete! YAML output at: {result.get('yaml_output_path')}")
                    if result.get("status") == "success":
                        safe_print(f"Extracted {result.get('tables_count', 0)} tables")
                        safe_print(f"Found {result.get('table_edges_count', 0)} table relationships")
                        safe_print(f"Found {result.get('column_edges_count', 0)} column relationships")
                else:
                    safe_print(f"\nProcessing failed for {args.file}: {result.get('message')}")
            
            asyncio.run(main_single_file())
            
        except Exception as e:
            safe_print(f"Error initializing LineageService: {str(e)}")
            # Fallback to original method
            safe_print("Falling back to original processing method...")
            result = extract_dependencies(
                args.file, 
                args.api_key, 
                concurrent=concurrent_multi_pass
            )
            
            # Save the result to a YAML file
            output_file = args.file.replace('.sql', '_dependencies.yaml').replace('.jinja2', '_dependencies.yaml')
            with open(output_file, 'w') as f:
                f.write(result)
            
            safe_print(f"\nProcessing complete! Saved results to {output_file}")

    else:
        # Process a folder (keeps existing functionality)
        process_folder(
            args.folder_path, 
            args.api_key, 
            output_file=args.output,
            max_workers=args.workers, 
            use_multi_pass=True,
            concurrent=concurrent_multi_pass
        )
        
        safe_print("\nFolder processing complete!")
        safe_print("Individual dependency files are saved in their respective directories.") 
