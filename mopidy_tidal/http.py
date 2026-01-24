import json
import logging
import tornado.web

logger = logging.getLogger(__name__)

class TidalRpcHandler(tornado.web.RequestHandler):
    """JSON-RPC 2.0 handler for Tidal-specific methods"""
    
    def initialize(self, core):
        self.core = core
    
    def set_default_headers(self):
        self.set_header("Content-Type", "application/json")
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.set_header("Access-Control-Allow-Headers", "Content-Type")
    
    def options(self):
        self.set_status(204)
        self.finish()
    
    def _get_tidal_backend(self):
        """Helper to get Tidal backend"""
        try:
            # Get the actual backends list from the proxy
            backends = self.core.backends.get(timeout=1)
            
            for backend in backends:
                uri_schemes = backend.uri_schemes.get(timeout=1)
                if uri_schemes and 'tidal' in uri_schemes:
                    return backend
        except Exception as e:
            logger.warning(f"Error finding Tidal backend: {e}")
        
        return None
        
    def _jsonrpc_error(self, request_id, code, message):
        """Create JSON-RPC error response"""
        return {
            'jsonrpc': '2.0',
            'id': request_id,
            'error': {'code': code, 'message': message}
        }
    
    def _jsonrpc_success(self, request_id, result):
        """Create JSON-RPC success response"""
        return {
            'jsonrpc': '2.0',
            'id': request_id,
            'result': result
        }
    
    async def post(self):
        try:
            request = json.loads(self.request.body.decode('utf-8'))
            request_id = request.get('id')
            
            # Validate JSON-RPC
            if request.get('jsonrpc') != '2.0':
                self.write(self._jsonrpc_error(
                    request_id, -32600, 'Invalid Request'
                ))
                return
            
            method = request.get('method')
            params = request.get('params', {})
            
            logger.info(f"Tidal RPC call: {method}")
            
            # Get backend
            backend = self._get_tidal_backend()
            if not backend:
                self.write(self._jsonrpc_error(
                    request_id, -32603, 'Tidal backend not available'
                ))
                return
            
            # Route methods
            if method == 'tidal.refresh_playlists':
                playlist_uris = params.get('uris')
                if not playlist_uris:
                    self.write(self._jsonrpc_error(
                        request_id,
                        -32602,
                        'Missing required parameter: uris. '
                        'Use the core playlists.refresh() method if you want to refresh all playlists.'
                    ))
                    return

                backend.playlists.refresh(*playlist_uris).get()
                self.write(self._jsonrpc_success(request_id, {
                    'refreshed': playlist_uris
                }))

            elif method == 'tidal.describe':
                # List available methods
                result = {
                    'methods': [
                        'tidal.refresh_playlists',
                        'tidal.describe'
                    ],
                    'tidal.refresh_playlists': {
                        'description': 'Refresh specific playlists by URI',
                        'params': {
                            'uris': 'List of playlist URIs to refresh (required)'
                        }
                    }
                }
                self.write(self._jsonrpc_success(request_id, result))
            
            else:
                self.write(self._jsonrpc_error(
                    request_id, -32601, f'Method not found: {method}'
                ))
        
        except json.JSONDecodeError:
            self.write(self._jsonrpc_error(
                None, -32700, 'Parse error'
            ))
        except Exception as e:
            logger.exception("Tidal RPC error")
            self.write(self._jsonrpc_error(
                request.get('id'), -32603, str(e)
            ))