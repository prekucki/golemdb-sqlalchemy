"""Tests for connection string parsing."""

import os
import pytest
from unittest.mock import patch
from golemdb_sql.connection_parser import (
    parse_connection_kwargs,
    parse_connection_string, 
    GolemBaseConnectionParams
)
from golemdb_sql.exceptions import ProgrammingError


class TestConnectionParser:
    """Test connection string parsing functionality."""
    
    def test_parse_individual_parameters(self):
        """Test parsing individual connection parameters."""
        params = parse_connection_kwargs(
            rpc_url="https://rpc.golembase.com",
            ws_url="wss://ws.golembase.com",
            private_key="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            app_id="testapp",
            schema_id="testschema"
        )
        
        assert params.rpc_url == "https://rpc.golembase.com"
        assert params.ws_url == "wss://ws.golembase.com"
        assert params.private_key == "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        assert params.app_id == "testapp"
        assert params.schema_id == "testschema"
    
    def test_parse_host_based_connection(self):
        """Test parsing host-based connection parameters."""
        params = parse_connection_kwargs(
            host="golembase.com",
            port=443,
            private_key="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            database="myapp",
            schema_id="myschema"
        )
        
        assert params.rpc_url == "https://golembase.com:443/rpc"
        assert params.ws_url == "wss://golembase.com:443/ws"
        assert params.private_key == "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        assert params.app_id == "myapp"
        assert params.schema_id == "myschema"
    
    def test_parse_golembase_url_connection_string(self):
        """Test parsing GolemBase URL format connection string."""
        connection_string = "golembase://0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef@rpc.golembase.com/testapp?ws_url=wss://ws.golembase.com&schema_id=testschema"
        
        params = parse_connection_string(connection_string)
        
        assert params.rpc_url == "https://rpc.golembase.com/rpc"
        assert params.ws_url == "wss://ws.golembase.com"
        assert params.private_key == "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        assert params.app_id == "testapp"
        assert params.schema_id == "testschema"
    
    def test_parse_key_value_connection_string(self):
        """Test parsing key-value connection string."""
        connection_string = "rpc_url=https://rpc.golembase.com ws_url=wss://ws.golembase.com private_key=0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef app_id=testapp schema_id=testschema"
        
        params = parse_connection_string(connection_string)
        
        assert params.rpc_url == "https://rpc.golembase.com"
        assert params.ws_url == "wss://ws.golembase.com"
        assert params.private_key == "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        assert params.app_id == "testapp"
        assert params.schema_id == "testschema"
    
    def test_parse_connection_string_parameter(self):
        """Test parsing connection string from connection_string parameter."""
        params = parse_connection_kwargs(
            connection_string="rpc_url=https://rpc.test.com ws_url=wss://ws.test.com private_key=0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef app_id=test"
        )
        
        assert params.rpc_url == "https://rpc.test.com"
        assert params.ws_url == "wss://ws.test.com"
        assert params.private_key == "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        assert params.app_id == "test"
        assert params.schema_id == "default"  # default value
    
    @patch.dict(os.environ, {'GOLEMBASE_PRIVATE_KEY': '0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef'})
    def test_environment_variable_expansion(self):
        """Test environment variable expansion in connection parameters."""
        params = parse_connection_kwargs(
            rpc_url="https://rpc.golembase.com",
            ws_url="wss://ws.golembase.com",
            private_key="$GOLEMBASE_PRIVATE_KEY",
            app_id="testapp"
        )
        
        assert params.private_key == "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
    
    def test_missing_required_parameters(self):
        """Test error handling for missing required parameters."""
        with pytest.raises(ProgrammingError, match="Missing required parameter: rpc_url"):
            parse_connection_kwargs(
                ws_url="wss://ws.golembase.com",
                private_key="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            )
    
    def test_invalid_connection_string_format(self):
        """Test error handling for invalid connection string format."""
        with pytest.raises(ProgrammingError, match="Unsupported connection string format"):
            parse_connection_string("invalid://format")
    
    def test_default_values(self):
        """Test default values for optional parameters."""
        params = parse_connection_kwargs(
            rpc_url="https://rpc.golembase.com",
            ws_url="wss://ws.golembase.com",
            private_key="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            app_id="testapp"
        )
        
        assert params.schema_id == "default"
    
    def test_private_key_bytes_conversion(self):
        """Test private key bytes conversion."""
        params = GolemBaseConnectionParams(
            rpc_url="https://rpc.golembase.com",
            ws_url="wss://ws.golembase.com",
            private_key="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            app_id="testapp",
            schema_id="testschema"
        )
        
        key_bytes = params.get_private_key_bytes()
        assert isinstance(key_bytes, bytes)
        assert len(key_bytes) == 32  # 64 hex chars = 32 bytes
    
    def test_private_key_without_0x_prefix(self):
        """Test private key without 0x prefix."""
        params = GolemBaseConnectionParams(
            rpc_url="https://rpc.golembase.com",
            ws_url="wss://ws.golembase.com",
            private_key="1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            app_id="testapp",
            schema_id="testschema"
        )
        
        key_bytes = params.get_private_key_bytes()
        assert isinstance(key_bytes, bytes)
        assert len(key_bytes) == 32
    
    def test_invalid_private_key_format(self):
        """Test error handling for invalid private key format."""
        params = GolemBaseConnectionParams(
            rpc_url="https://rpc.golembase.com",
            ws_url="wss://ws.golembase.com",
            private_key="invalid_key",
            app_id="testapp",
            schema_id="testschema"
        )
        
        with pytest.raises(ValueError):
            params.get_private_key_bytes()
    
    def test_params_to_dict(self):
        """Test conversion of parameters to dictionary."""
        params = GolemBaseConnectionParams(
            rpc_url="https://rpc.golembase.com",
            ws_url="wss://ws.golembase.com",
            private_key="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            app_id="testapp",
            schema_id="testschema"
        )
        
        params_dict = params.to_dict()
        expected = {
            'rpc_url': "https://rpc.golembase.com",
            'ws_url': "wss://ws.golembase.com",
            'private_key': "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            'app_id': "testapp",
            'schema_id': "testschema"
        }
        
        assert params_dict == expected