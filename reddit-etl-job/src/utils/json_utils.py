import json
import logging
import datetime
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

def safe_json_loads(json_str: Optional[str], default_value: Any = None) -> Any:
    """
    Safely loads a JSON string without raising exceptions.
    
    Args:
        json_str: JSON string to parse
        default_value: Value to return if parsing fails
        
    Returns:
        Parsed JSON or default value
    """
    if not json_str:
        return default_value
        
    try:
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning(f"Failed to parse JSON: {e}")
        return default_value


def date_serializer(obj: Any) -> str:
    """
    Custom serializer for handling date objects in JSON.
    
    Args:
        obj: Object to serialize
        
    Returns:
        String representation of the object
    """
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def safe_json_dumps(obj: Any) -> str:
    """
    Safely dumps an object to a JSON string without raising exceptions.
    Handles date/datetime objects automatically.
    
    Args:
        obj: Object to serialize
        
    Returns:
        JSON string
    """
    try:
        return json.dumps(obj, default=date_serializer)
    except TypeError as e:
        logger.warning(f"Failed to serialize object to JSON: {e}")
        return "{}"


def merge_json_objects(obj1: Dict[str, Any], obj2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge two JSON-serializable dictionary objects.
    
    Args:
        obj1: First object
        obj2: Second object
        
    Returns:
        Merged object
    """
    if not isinstance(obj1, dict) or not isinstance(obj2, dict):
        return obj1 if obj1 is not None else obj2
        
    result = obj1.copy()
    
    for key, value in obj2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            # Recursively merge nested dictionaries
            result[key] = merge_json_objects(result[key], value)
        elif key in result and isinstance(result[key], list) and isinstance(value, list):
            # Concatenate lists
            result[key] = result[key] + value
        elif key in result and isinstance(result[key], (int, float)) and isinstance(value, (int, float)):
            # Sum numbers
            result[key] += value
        else:
            # For all other types, use the value from the second object
            result[key] = value
            
    return result


def merge_count_dictionaries(dict1: Dict[str, int], dict2: Dict[str, int]) -> Dict[str, int]:
    """
    Merge two dictionaries with integer counts.
    
    Args:
        dict1: First dictionary with counts
        dict2: Second dictionary with counts
        
    Returns:
        Merged dictionary with summed counts
    """
    result = dict1.copy()
    
    for key, count in dict2.items():
        result[key] = result.get(key, 0) + count
        
    return result 