"""GolemBase connection string parsing utilities."""

import os
import re
from typing import Dict, Any, Optional
from urllib.parse import urlparse, parse_qs
from .exceptions import InterfaceError


class GolemBaseConnectionParams:
    """Container for GolemBase connection parameters."""
    
    def __init__(
        self,
        rpc_url: str,
        ws_url: str,
        private_key: str,
        app_id: str = "default",
        schema_id: str = "default",
        **kwargs
    ):
        """Initialize connection parameters.
        
        Args:
            rpc_url: HTTPS RPC endpoint URL
            ws_url: WebSocket URL for real-time events
            private_key: Hex private key for authentication
            app_id: Application/Project identifier (used as projectId)
            schema_id: Schema configuration identifier
            **kwargs: Additional parameters
        """
        self.rpc_url = rpc_url
        self.ws_url = ws_url
        self.private_key = private_key
        self.app_id = app_id  # This serves as projectId
        self.schema_id = schema_id
        self.extra_params = kwargs
        
        # Validate required parameters
        self._validate()
    
    def _validate(self) -> None:
        """Validate connection parameters."""
        if not self.rpc_url:
            raise InterfaceError("rpc_url is required")
        
        if not self.ws_url:
            raise InterfaceError("ws_url is required")
            
        if not self.private_key:
            raise InterfaceError("private_key is required")
            
        # Validate URLs
        if not (self.rpc_url.startswith('http://') or self.rpc_url.startswith('https://')):
            raise InterfaceError("rpc_url must be HTTP or HTTPS URL")
            
        if not (self.ws_url.startswith('ws://') or self.ws_url.startswith('wss://')):
            raise InterfaceError("ws_url must be WebSocket URL")
            
        # Validate private key format
        self._validate_private_key()
    
    def _validate_private_key(self) -> None:
        """Validate private key format."""
        key = self.private_key
        
        # Remove 0x prefix if present
        if key.startswith('0x'):
            key = key[2:]
            
        # Check if it's valid hex
        if not re.match(r'^[0-9a-fA-F]+$', key):
            raise InterfaceError("private_key must be a valid hex string")
            
        # Check length (should be 64 hex chars = 32 bytes)
        if len(key) != 64:
            raise InterfaceError("private_key must be 32 bytes (64 hex characters)")
    
    def get_private_key_bytes(self) -> bytes:
        """Get private key as bytes.
        
        Returns:
            Private key as bytes
        """
        key = self.private_key
        if key.startswith('0x'):
            key = key[2:]
        return bytes.fromhex(key)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for SDK consumption.
        
        Returns:
            Dictionary with connection parameters
        """
        return {
            'rpc_url': self.rpc_url,
            'ws_url': self.ws_url,
            'private_key': self.get_private_key_bytes(),
            'app_id': self.app_id,
            'schema_id': self.schema_id,
            **self.extra_params
        }


def parse_connection_string(connection_string: str) -> GolemBaseConnectionParams:
    """Parse GolemBase connection string.
    
    Supports multiple formats:
    1. URL format: golembase://private_key@rpc_host/app_id?ws_url=ws://...&schema_id=...
    2. Key-value format: rpc_url=... ws_url=... private_key=... app_id=... schema_id=...
    3. Environment variable substitution: ${VAR_NAME}
    
    Args:
        connection_string: Connection string to parse
        
    Returns:
        Parsed connection parameters
        
    Raises:
        InterfaceError: If connection string is invalid
    """
    # Expand environment variables
    expanded = _expand_env_vars(connection_string)
    
    # Try URL format first
    if expanded.startswith('golembase://'):
        return _parse_url_format(expanded)
    
    # Try key-value format
    elif '=' in expanded and ' ' in expanded:
        return _parse_keyvalue_format(expanded)
    
    else:
        raise InterfaceError(f"Invalid connection string format: {connection_string}")


def _expand_env_vars(text: str) -> str:
    """Expand environment variables in text.
    
    Supports ${VAR_NAME} format.
    
    Args:
        text: Text with potential environment variables
        
    Returns:
        Text with environment variables expanded
    """
    def replace_env_var_brace(match):
        var_name = match.group(1)
        return os.environ.get(var_name, match.group(0))
    
    def replace_env_var_simple(match):
        var_name = match.group(1)
        return os.environ.get(var_name, match.group(0))
    
    # Support both ${VAR_NAME} and $VAR_NAME
    text = re.sub(r'\$\{([^}]+)\}', replace_env_var_brace, text)
    text = re.sub(r'\$([A-Z_][A-Z0-9_]*)', replace_env_var_simple, text)
    return text


def _parse_url_format(url: str) -> GolemBaseConnectionParams:
    """Parse URL format connection string.
    
    Format: golembase://private_key@rpc_host:port/app_id?ws_url=ws://...&schema_id=...
    
    Args:
        url: URL format connection string
        
    Returns:
        Parsed connection parameters
    """
    try:
        parsed = urlparse(url)
        
        if parsed.scheme != 'golembase':
            raise InterfaceError("URL scheme must be 'golembase'")
        
        # Extract private key from username
        private_key = parsed.username
        if not private_key:
            raise InterfaceError("Private key must be specified in URL username part")
        
        # Build RPC URL from host/port
        rpc_scheme = 'https'  # Default to HTTPS
        if parsed.port and parsed.port != 443:
            rpc_url = f"{rpc_scheme}://{parsed.hostname}:{parsed.port}/rpc"
        else:
            rpc_url = f"{rpc_scheme}://{parsed.hostname}/rpc"
        
        # Extract app_id from path
        app_id = parsed.path.lstrip('/') or 'default'
        
        # Parse query parameters
        query_params = parse_qs(parsed.query)
        
        # Extract ws_url (required)
        ws_url_list = query_params.get('ws_url', [])
        if not ws_url_list:
            raise InterfaceError("ws_url query parameter is required")
        ws_url = ws_url_list[0]
        
        # Extract optional parameters
        schema_id = query_params.get('schema_id', ['default'])[0]
        
        # Build extra parameters
        extra_params = {}
        for key, values in query_params.items():
            if key not in ('ws_url', 'schema_id'):
                extra_params[key] = values[0] if len(values) == 1 else values
        
        return GolemBaseConnectionParams(
            rpc_url=rpc_url,
            ws_url=ws_url,
            private_key=private_key,
            app_id=app_id,
            schema_id=schema_id,
            **extra_params
        )
        
    except Exception as e:
        raise InterfaceError(f"Failed to parse connection URL: {e}")


def _parse_keyvalue_format(connection_string: str) -> GolemBaseConnectionParams:
    """Parse key-value format connection string.
    
    Format: rpc_url=https://... ws_url=wss://... private_key=0x... app_id=... schema_id=...
    
    Args:
        connection_string: Key-value format connection string
        
    Returns:
        Parsed connection parameters
    """
    try:
        params = {}
        
        # Split by spaces and parse key=value pairs
        for part in connection_string.split():
            if '=' in part:
                key, value = part.split('=', 1)
                params[key.strip()] = value.strip()
        
        # Extract required parameters
        rpc_url = params.get('rpc_url')
        ws_url = params.get('ws_url')
        private_key = params.get('private_key')
        
        if not rpc_url:
            raise InterfaceError("rpc_url parameter is required")
        if not ws_url:
            raise InterfaceError("ws_url parameter is required")
        if not private_key:
            raise InterfaceError("private_key parameter is required")
        
        # Extract optional parameters
        app_id = params.get('app_id', 'default')
        schema_id = params.get('schema_id', 'default')
        
        # Build extra parameters
        extra_params = {k: v for k, v in params.items() 
                       if k not in ('rpc_url', 'ws_url', 'private_key', 'app_id', 'schema_id')}
        
        return GolemBaseConnectionParams(
            rpc_url=rpc_url,
            ws_url=ws_url,
            private_key=private_key,
            app_id=app_id,
            schema_id=schema_id,
            **extra_params
        )
        
    except Exception as e:
        raise InterfaceError(f"Failed to parse connection string: {e}")


def parse_connection_kwargs(**kwargs: Any) -> GolemBaseConnectionParams:
    """Parse connection parameters from keyword arguments.
    
    Args:
        **kwargs: Connection parameters
        
    Returns:
        Parsed connection parameters
    """
    # Handle connection_string parameter
    if 'connection_string' in kwargs:
        return parse_connection_string(kwargs['connection_string'])
    
    # Direct parameter extraction with environment variable expansion
    rpc_url = _expand_env_vars(kwargs.get('rpc_url', '')) if kwargs.get('rpc_url') else None
    ws_url = _expand_env_vars(kwargs.get('ws_url', '')) if kwargs.get('ws_url') else None
    private_key = _expand_env_vars(kwargs.get('private_key', '')) if kwargs.get('private_key') else None
    
    # Try to build from individual components
    if not rpc_url and 'host' in kwargs:
        host = kwargs['host']
        port = kwargs.get('port', 443)
        scheme = 'https' if port == 443 else 'http'
        rpc_url = f"{scheme}://{host}:{port}/rpc"
    
    if not ws_url and 'host' in kwargs:
        host = kwargs['host']  
        ws_port = kwargs.get('ws_port', 443)
        ws_scheme = 'wss' if ws_port == 443 else 'ws'
        ws_url = f"{ws_scheme}://{host}:{ws_port}/ws"
    
    # Extract other parameters
    app_id = kwargs.get('app_id', kwargs.get('database', 'default'))
    schema_id = kwargs.get('schema_id', 'default')
    
    # Build extra parameters
    extra_params = {k: v for k, v in kwargs.items() 
                   if k not in ('rpc_url', 'ws_url', 'private_key', 'app_id', 'schema_id', 
                               'connection_string', 'host', 'port', 'ws_port', 'database')}
    
    return GolemBaseConnectionParams(
        rpc_url=rpc_url or '',
        ws_url=ws_url or '',
        private_key=private_key or '',
        app_id=app_id,
        schema_id=schema_id,
        **extra_params
    )