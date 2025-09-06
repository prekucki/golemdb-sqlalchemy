"""GolemBase connection string parsing utilities."""

import os
import re
from dataclasses import dataclass, field
from typing import Dict, Any, Optional
from urllib.parse import urlparse, parse_qs
from .exceptions import InterfaceError


@dataclass
class GolemBaseConnectionParams:
    """Container for GolemBase connection parameters."""
    
    rpc_url: str
    ws_url: str
    private_key: str
    app_id: str = "default"
    schema_id: str = "default"
    extra_params: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate connection parameters after initialization."""
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
    
    @classmethod
    def from_env(
        cls, 
        rpc_url_env: str = 'RPC_URL',
        ws_url_env: str = 'WS_URL',
        private_key_env: str = 'PRIVATE_KEY',
        app_id_env: str = 'APP_ID',
        schema_id_env: str = 'SCHEMA_ID',
        **defaults
    ) -> 'GolemBaseConnectionParams':
        """Create connection parameters from environment variables.
        
        Args:
            rpc_url_env: Environment variable name for RPC URL
            ws_url_env: Environment variable name for WebSocket URL
            private_key_env: Environment variable name for private key
            app_id_env: Environment variable name for app ID
            schema_id_env: Environment variable name for schema ID
            **defaults: Default values for parameters
            
        Returns:
            GolemBaseConnectionParams instance
            
        Raises:
            InterfaceError: If required environment variables are missing
        """
        rpc_url = os.getenv(rpc_url_env, defaults.get('rpc_url'))
        ws_url = os.getenv(ws_url_env, defaults.get('ws_url'))
        private_key = os.getenv(private_key_env, defaults.get('private_key'))
        app_id = os.getenv(app_id_env, defaults.get('app_id', 'default'))
        schema_id = os.getenv(schema_id_env, defaults.get('schema_id', 'default'))
        
        if not rpc_url:
            raise InterfaceError(f"Environment variable {rpc_url_env} is required")
        if not ws_url:
            raise InterfaceError(f"Environment variable {ws_url_env} is required") 
        if not private_key:
            raise InterfaceError(f"Environment variable {private_key_env} is required")
        
        # Filter out reserved parameters from defaults
        extra_params = {k: v for k, v in defaults.items() 
                       if k not in ['rpc_url', 'ws_url', 'private_key', 'app_id', 'schema_id']}
        
        return cls(
            rpc_url=rpc_url,
            ws_url=ws_url,
            private_key=private_key,
            app_id=app_id,
            schema_id=schema_id,
            extra_params=extra_params
        )
    
    @classmethod
    def from_url(cls, url: str, **overrides) -> 'GolemBaseConnectionParams':
        """Create connection parameters from a GolemBase URL.
        
        Expected URL format:
        golembase://private_key@host:port/app_id?schema_id=schema&ws_port=ws_port
        
        Args:
            url: GolemBase connection URL
            **overrides: Override specific parameters
            
        Returns:
            GolemBaseConnectionParams instance
            
        Raises:
            InterfaceError: If URL format is invalid
        """
        if not url.startswith('golembase://'):
            raise InterfaceError("URL must start with 'golembase://'")
        
        try:
            parsed = urlparse(url)
            
            # Extract private key from username
            private_key = overrides.get('private_key', parsed.username)
            if not private_key:
                raise InterfaceError("Private key must be specified in URL username")
            
            # Extract host and port
            host = parsed.hostname
            port = parsed.port or 443
            
            # Build URLs
            scheme = 'https' if port == 443 else 'http'
            ws_scheme = 'wss' if port == 443 else 'ws'
            
            rpc_url = overrides.get('rpc_url', f"{scheme}://{host}:{port}/rpc")
            
            # Extract WebSocket port from query params or use same port
            query_params = parse_qs(parsed.query)
            ws_port = query_params.get('ws_port', [str(port)])[0]
            ws_url = overrides.get('ws_url', f"{ws_scheme}://{host}:{ws_port}/rpc/ws")
            
            # Extract app_id from path
            app_id = overrides.get('app_id', parsed.path.strip('/') or 'default')
            
            # Extract schema_id from query params
            schema_id = overrides.get('schema_id', query_params.get('schema_id', ['default'])[0])
            
            # Build extra parameters
            extra_params = {}
            for k, v in query_params.items():
                if k not in ['schema_id', 'ws_port']:
                    extra_params[k] = v[0] if len(v) == 1 else v
            
            # Add any override parameters that aren't standard
            for k, v in overrides.items():
                if k not in ['rpc_url', 'ws_url', 'private_key', 'app_id', 'schema_id']:
                    extra_params[k] = v
            
            return cls(
                rpc_url=rpc_url,
                ws_url=ws_url,
                private_key=private_key,
                app_id=app_id,
                schema_id=schema_id,
                extra_params=extra_params
            )
            
        except Exception as e:
            raise InterfaceError(f"Invalid GolemBase URL format: {e}")


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
            extra_params=extra_params
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
            extra_params=extra_params
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
        extra_params=extra_params
    )