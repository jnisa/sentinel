# Script that contains the auxiliar functions for the attributes modules

from typing import Optional

def get_id(prefix: str, name: str, suffix: Optional[str] = None) -> str:
    """
    Get the span_id from the current span. 

    Example:
        >>> get_id('service', 'df', 'var')
        'service.df.var'
        >>> get_id('service', 'df')
        'service.df'

    :return get an id combining the prefix, name and suffix.
    """

    return '.'.join(filter(lambda x: x is not None, [prefix, name, suffix]))
