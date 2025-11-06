from __future__ import unicode_literals
import json

import logging
import time
from concurrent.futures import Future
from pathlib import Path
from typing import Optional, Union, Dict

from mopidy import backend
from pykka import ThreadingActor
from tidalapi import Config, Quality, Session
from tidalapi import __version__ as tidalapi_ver

from mopidy_tidal import Extension
from mopidy_tidal import __version__ as mopidy_tidal_ver
from mopidy_tidal import context, library, playback, playlists
from mopidy_tidal.web_auth_server import WebAuthServer

logger = logging.getLogger(__name__)


class TidalBackend(ThreadingActor, backend.Backend):
    def __init__(self, config, audio):
        super().__init__()
        # Mopidy cfg
        self._config = config
        context.set_config(self._config)
        self._tidal_config = config[Extension.ext_name]

        # Backend
        self.playback = playback.TidalPlaybackProvider(audio=audio, backend=self)
        self.library = library.TidalLibraryProvider(backend=self)
        self.playlists = playlists.TidalPlaylistsProvider(backend=self)

        # Session parameters
        self._active_session: Optional[Session] = None
        self._logged_in: bool = False
        self.uri_schemes: tuple[str] = ("tidal",)
        self._login_future: Optional[Future] = None
        self._login_url: Optional[str] = None
        self.data_dir: Path = Path(Extension.get_data_dir(self._config))
        self.session_file_path: Path = Path("")
        self.web_auth_server: WebAuthServer = WebAuthServer()

        # Token refresh
        self._token_refresh_thread = None
        self._token_refresh_running = False

        # Config parameters
        # Lazy: Connect lazily, i.e. login only when user starts browsing TIDAL directories
        self.lazy_connect: bool = False
        # Login Method:
        #   BLOCK:       Immediately prompt user for login (This will block mopidy startup!)
        #   HACK/AUTO:   Display dummy track with login URL. When clicked, QR code and TTS is generated
        self.login_method: str = "BLOCK"
        # pkce_enabled: If true, TIDAL session will use PKCE auth. Otherwise OAuth2 is used
        self.auth_method: str = "OAUTH"
        self.pkce_enabled: bool = False
        # login_server_port: Port to use for login HTTP server, eg. <host_ip>:<port>. Default <host_ip>:8989
        self.login_server_port: int = 8989

    @property
    def session(self):
        if not self.logged_in:
            self._login()
        return self._active_session

    @property
    def logged_in(self):
        if not self._logged_in:
            if self._active_session.load_session_from_file(self.session_file_path):
                logger.info("Loaded TIDAL session from file %s", self.session_file_path)
                self._logged_in = True
        return self._logged_in

    @property
    def session_valid(self):
        """
        Returns true when session is logged in and valid.

        Automatically attempts token refresh if session is invalid.
        """
        # First check if session is valid
        is_valid = self._active_session.check_login()

        if not is_valid and hasattr(self._active_session, 'refresh_token') and self._active_session.refresh_token:
            # Session is invalid but we have a refresh token - try to refresh
            if self.auth_logging_enabled:
                logger.info("[TIDAL BACKEND] Session invalid, attempting automatic token refresh...")

            try:
                success = self._active_session.token_refresh(self._active_session.refresh_token)

                if success:
                    logger.info("[TIDAL BACKEND] ✓ Automatic token refresh successful")

                    # Save the refreshed session
                    self._active_session.save_session_to_file(self.session_file_path)
                    logger.info("[TIDAL BACKEND] ✓ Refreshed session saved to: %s", self.session_file_path)

                    # Update credentials via API
                    try:
                        self._update_credentials_via_api()
                        self._update_credential_status('active')
                    except Exception as e:
                        logger.warning("[TIDAL BACKEND] Could not update credentials via API: %s", e)

                    # Re-check validity
                    is_valid = self._active_session.check_login()

                    if is_valid:
                        logger.info("[TIDAL BACKEND] ✓ Session is now valid after refresh")
                    else:
                        logger.warning("[TIDAL BACKEND] ⚠ Session still invalid after refresh")
                        self._update_credential_status('failed', 'Token refresh succeeded but session still invalid')
                else:
                    logger.error("[TIDAL BACKEND] ✗ Automatic token refresh failed")
                    self._update_credential_status('failed', 'Token refresh failed - refresh token may be invalid')

            except Exception as e:
                logger.error("[TIDAL BACKEND] ✗ Exception during automatic token refresh: %s", e, exc_info=True)
                self._update_credential_status('failed', f'Token refresh exception: {str(e)[:100]}')

        elif not is_valid:
            # No refresh token available
            logger.warning("[TIDAL BACKEND] Session invalid and no refresh token available")
            self._update_credential_status('failed', 'Session invalid - no refresh token available')

        return is_valid

    def on_start(self):
        logger.info("=" * 80)
        logger.info("[TIDAL BACKEND] Starting Tidal backend")
        logger.info("[TIDAL BACKEND] Mopidy-Tidal version: v%s", mopidy_tidal_ver)
        logger.info("[TIDAL BACKEND] Python-Tidal (tidalapi) version: v%s", tidalapi_ver)

        quality = self._tidal_config["quality"]
        client_id = self._tidal_config["client_id"]
        client_secret = self._tidal_config["client_secret"]
        self.auth_method = self._tidal_config["auth_method"]
        if self.auth_method == "PKCE":
            self.pkce_enabled = True

        # Get auth logging preference
        self.auth_logging_enabled = self._tidal_config.get("auth_logging_enabled", False)

        self.auth_token = self._tidal_config.get("auth_token")
        if self.auth_token:
            if self.auth_logging_enabled:
                logger.info("[TIDAL BACKEND] Using auth_token from config: %s", str(self.auth_token)[:50] + "...")
            else:
                logger.info("[TIDAL BACKEND] Using auth_token from config (***)")
            try:
                self.auth_token = json.loads(self.auth_token)
            except json.decoder.JSONDecodeError:
                logger.warning("[TIDAL BACKEND] Failed to parse auth_token from config")
        self.login_server_port = self._tidal_config["login_server_port"]
        logger.info("[TIDAL BACKEND] PKCE login web server port: %s", self.login_server_port)
        self.login_method = self._tidal_config["login_method"]
        if self.login_method == "AUTO":
            # Add AUTO as alias to HACK login method
            self.login_method = "HACK"
        self.lazy_connect = self._tidal_config["lazy"]
        logger.info("[TIDAL BACKEND] Quality: %s", quality)
        logger.info("[TIDAL BACKEND] Authentication: %s", "PKCE" if self.pkce_enabled else "OAuth")
        logger.info("[TIDAL BACKEND] Auth Logging: %s", "ENABLED" if self.auth_logging_enabled else "DISABLED")
        config = Config(quality=quality)

        # Set the session filename, depending on the type of session
        if self.pkce_enabled:
            self.session_file_path = Path(self.data_dir, "tidal-pkce.json")
        else:
            self.session_file_path = Path(self.data_dir, "tidal-oauth.json")

        if (self.login_method == "HACK") and not self._tidal_config["lazy"]:
            logger.warning("AUTO login implies lazy connection, setting lazy=True.")
            self.lazy_connect = True
        logger.info(
            "Login method: %s", "BLOCK" if self.pkce_enabled == "BLOCK" else "AUTO"
        )

        if client_id and client_secret:
            logger.info("Using client id & client secret from config")
            config.client_id = client_id
            config.api_token = client_id
            config.client_secret = client_secret
        elif (client_id and not client_secret) or (client_secret and not client_id):
            logger.warning("Always provide both client_id and client_secret")
            logger.info("Using default client id & client secret from python-tidal")
        else:
            logger.info("Using default client id & client secret from python-tidal")

        self._active_session = Session(config)
        if not self.lazy_connect:
            try:
                self._login()
            except Exception as e:
                logger.error("[TIDAL BACKEND] Login failed during startup: %s", e, exc_info=True)
                self._update_credential_status('failed',
                    f'Login failed: {str(e)[:100]}')
                # Don't re-raise - let backend continue without Tidal

        # Log token expiration status after login
        if self.session_valid:
            self._log_token_status()
            self._log_user_metadata()

        # Start token refresh service if using OAuth/PKCE
        if self.session_valid and (self.pkce_enabled or self.auth_method == "OAUTH"):
            refresh_enabled = self._tidal_config.get("token_refresh_enabled", True)
            if refresh_enabled:
                try:
                    self._start_token_refresh_thread()
                except Exception as e:
                    logger.error("[TIDAL BACKEND] ✗ Failed to start token refresh service: %s", e, exc_info=True)
            else:
                logger.info("[TIDAL BACKEND] Token refresh disabled in config")
        else:
            logger.info("[TIDAL BACKEND] Not using OAuth/PKCE or not logged in - token refresh not applicable")

        logger.info("[TIDAL BACKEND] Backend initialization complete")
        logger.info("=" * 80)

    def _login(self):
        """Load session at startup or create a new session"""

        if self.auth_token:
            logger.info("Using provided PKCE access token")
            try:
                self._active_session.process_auth_token(self.auth_token)
                self._complete_login()
            except Exception as e:
                # Import the exception type we need
                import tidalapi.exceptions
                if isinstance(e, tidalapi.exceptions.AuthenticationError):
                    logger.error("[TIDAL BACKEND] Authentication failed: %s", e)
                    self._update_credential_status('failed',
                        'Authentication failed - token may be expired. Please re-authenticate.')
                else:
                    logger.error("[TIDAL BACKEND] Unexpected error during authentication: %s", e, exc_info=True)
                    self._update_credential_status('failed',
                        f'Authentication error: {str(e)[:100]}')
                # Don't re-raise - let backend continue in degraded mode
                return
        elif self._active_session.load_session_from_file(self.session_file_path):
            logger.info(
                "Loaded existing TIDAL session from file %s...", self.session_file_path
            )

        if not self.session_valid:
            if not self.login_server_port:
                # A. Default login, user must find login URL in Mopidy log
                logger.info("Creating new session (OAuth)...")
                self._active_session.login_oauth_simple(function=logger.info)
            else:
                # B. Interactive login, user must perform login using web auth
                logger.info(
                    "Creating new session (%s)...",
                    "PKCE" if self.pkce_enabled else "OAuth",
                )
                if self.pkce_enabled:
                    # PKCE Login
                    login_url = self._active_session.pkce_login_url()
                    logger.info(
                        "Please visit 'http://localhost:%s' to authenticate",
                        self.login_server_port,
                    )
                    # Enable web server for interactive login + callback on form Submit
                    self.web_auth_server.set_callback(self._web_auth_callback)
                    self.web_auth_server.start_oauth_daemon(
                        login_url, self.login_server_port, self.pkce_enabled
                    )
                else:
                    # OAuth login
                    login_url = self.login_url
                    logger.info(
                        "Please visit 'http://localhost:%s' or '%s' to authenticate",
                        self.login_server_port,
                        login_url,
                    )
                    # Enable web server for interactive login (no callback)
                    self.web_auth_server.start_oauth_daemon(
                        login_url, self.login_server_port, self.pkce_enabled
                    )

                # Wait for user to complete interactive login sequence
                max_time = time.time() + 300
                while time.time() < max_time:
                    if self._logged_in:
                        if not self.pkce_enabled:
                            self._complete_login()
                        return
                    logger.info(
                        "Time left to complete authentication: %s sec",
                        int(max_time - time.time()),
                    )
                    time.sleep(5)
                raise TimeoutError("You took too long to log in")

    def _web_auth_callback(self, url_redirect: str):
        """Callback triggered on web auth completion
        :param url_redirect: URL of the 'Ooops' page, where the user was redirected to after login.
        :type url_redirect: str
        """
        if self.pkce_enabled:
            try:
                # Query for auth tokens
                json: Dict[
                    str, Union[str, int]
                ] = self._active_session.pkce_get_auth_token(url_redirect)
                # Parse and set tokens.
                self._active_session.process_auth_token(json, is_pkce_token=True)
                self._logged_in = True
            except:
                raise ValueError("Response code is required for PKCE login!")
        # Store session after auth completion
        self._complete_login()

    def _complete_login(self):
        """Perform final steps of login sequence; save session to file"""
        if self.session_valid:
            # Only store current session if valid
            logger.info("TIDAL Login OK")
            self._active_session.save_session_to_file(self.session_file_path)
            self._logged_in = True
        else:
            logger.error("TIDAL Login Failed")
            raise ConnectionError("Failed to log in.")

    @property
    def logging_in(self) -> bool:
        """Are we currently waiting for user confirmation to log in?"""
        return bool(self._login_future and self._login_future.running())

    @property
    def login_url(self) -> Optional[str]:
        """Start a new login sequence (if not active) and get the latest login URL"""
        if not self.pkce_enabled:
            if not self._logged_in and not self.logging_in:
                login_url, self._login_future = self._active_session.login_oauth()
                self._login_future.add_done_callback(lambda *_: self._complete_login())
                self._login_url = login_url.verification_uri_complete
            return f"https://{self._login_url}" if self._login_url else None
        else:
            if not self._logged_in and not self.web_auth_server.is_daemon_running:
                login_url = self._active_session.pkce_login_url()
                self._login_url = "http://localhost:{}".format(self.login_server_port)
                # Enable web server for interactive login + callback on form Submit
                self.web_auth_server.set_callback(self._web_auth_callback)
                self.web_auth_server.start_oauth_daemon(
                    login_url, self.login_server_port, self.pkce_enabled
                )
            return f"{self._login_url}" if self._login_url else None

    def on_stop(self):
        """Clean shutdown of backend"""
        logger.info("[TIDAL BACKEND] Stopping Tidal backend...")

        # Stop token refresh thread
        self._stop_token_refresh_thread()

        logger.info("[TIDAL BACKEND] ✓ Tidal backend stopped")

    def _start_token_refresh_thread(self):
        """Start background thread for token refresh"""
        import threading

        logger.info("=" * 80)
        logger.info("[TIDAL TOKEN REFRESH] Starting token refresh service")

        refresh_interval = self._tidal_config.get("token_refresh_check_interval", 86400)  # Default: 24 hours
        refresh_threshold = self._tidal_config.get("token_refresh_threshold", 0.5)  # Default: 50%
        source_id = self._tidal_config.get("source_id")

        logger.info("[TIDAL TOKEN REFRESH] Check interval: %ss (%s hours)", refresh_interval, refresh_interval/3600)
        logger.info("[TIDAL TOKEN REFRESH] Refresh threshold: %s%% of token lifetime", refresh_threshold * 100)
        logger.info("[TIDAL TOKEN REFRESH] Source ID for credential updates: %s", source_id or "NOT SET (will auto-detect)")

        self._token_refresh_running = True
        self._token_refresh_thread = threading.Thread(
            target=self._token_refresh_loop,
            daemon=True,
            name="tidal-token-refresh"
        )
        self._token_refresh_thread.start()

        logger.info("[TIDAL TOKEN REFRESH] ✓ Token refresh service started")
        logger.info("=" * 80)

    def _stop_token_refresh_thread(self):
        """Stop background token refresh thread"""
        if self._token_refresh_thread:
            logger.info("[TIDAL TOKEN REFRESH] Stopping token refresh service...")
            self._token_refresh_running = False

            # Wait up to 5 seconds for thread to finish
            self._token_refresh_thread.join(timeout=5)

            if self._token_refresh_thread.is_alive():
                logger.warning("[TIDAL TOKEN REFRESH] Token refresh thread did not stop cleanly")
            else:
                logger.info("[TIDAL TOKEN REFRESH] ✓ Token refresh service stopped")

    def _token_refresh_loop(self):
        """Main loop - check and refresh token periodically"""
        import time as time_module

        refresh_interval = self._tidal_config.get("token_refresh_check_interval", 86400)

        while self._token_refresh_running:
            try:
                logger.debug("[TIDAL TOKEN REFRESH] Checking if token needs refresh...")
                self._check_and_refresh_token()
            except Exception as e:
                logger.error("[TIDAL TOKEN REFRESH] Error in refresh loop: %s", e, exc_info=True)

            # Sleep in 60-second chunks to allow clean shutdown
            for _ in range(refresh_interval // 60):
                if not self._token_refresh_running:
                    break
                time_module.sleep(60)

            # Sleep remaining seconds
            if self._token_refresh_running:
                remaining = refresh_interval % 60
                if remaining > 0:
                    time_module.sleep(remaining)

    def _check_and_refresh_token(self):
        """Check if token needs refresh and refresh if needed"""
        import datetime

        # First, check if session is valid (this will auto-retry with refresh token if needed)
        if not self.session_valid:
            logger.warning("[TIDAL TOKEN REFRESH] Session is not valid even after auto-retry")
            logger.warning("[TIDAL TOKEN REFRESH] This likely means the refresh token is also invalid/expired")
            logger.warning("[TIDAL TOKEN REFRESH] User will need to re-authenticate")
            # Status already updated by session_valid property
            return

        # Check if we have expiry_time
        if not hasattr(self._active_session, 'expiry_time') or not self._active_session.expiry_time:
            logger.debug("[TIDAL TOKEN REFRESH] No expiry_time available, assuming token is fresh")
            return

        # Calculate time until expiration
        now = datetime.datetime.utcnow()
        expires_at = self._active_session.expiry_time
        time_until_expiry = (expires_at - now).total_seconds()

        # Check if token is already expired
        if time_until_expiry <= 0:
            logger.warning("[TIDAL TOKEN REFRESH] Token is ALREADY EXPIRED! Refreshing immediately...")
            self._perform_token_refresh()
            return

        # Get original token lifetime
        if not hasattr(self, '_token_original_lifetime'):
            # First time - assume current expiry time IS the original lifetime
            # This prevents refresh loops when tokens have short lifetimes (e.g., 3 hours for PKCE)
            # If the token is actually partially expired, worst case we refresh slightly late
            self._token_original_lifetime = int(time_until_expiry)
            logger.info("[TIDAL TOKEN REFRESH] First check - assuming token is fresh, lifetime: %ss (%s hours)",
                       self._token_original_lifetime, self._token_original_lifetime/3600)

        original_lifetime = self._token_original_lifetime
        threshold = self._tidal_config.get("token_refresh_threshold", 0.5)
        threshold_time = original_lifetime * threshold

        if self.auth_logging_enabled:
            logger.debug("[TIDAL TOKEN REFRESH] Token status:")
            logger.debug("  - Expires in: %ss (%s days)", time_until_expiry, time_until_expiry/86400)
            logger.debug("  - Original lifetime: %ss (%s days)", original_lifetime, original_lifetime/86400)
            logger.debug("  - Refresh threshold: %ss (%s days)", threshold_time, threshold_time/86400)
            logger.debug("  - Needs refresh: %s", time_until_expiry < threshold_time)

        if time_until_expiry < threshold_time:
            logger.info("=" * 80)
            logger.info("[TIDAL TOKEN REFRESH] Token needs refresh!")
            logger.info("[TIDAL TOKEN REFRESH] Time until expiry: %ss (%s days)", time_until_expiry, time_until_expiry/86400)
            logger.info("[TIDAL TOKEN REFRESH] Threshold: %ss (%s days)", threshold_time, threshold_time/86400)
            self._perform_token_refresh()
            logger.info("=" * 80)
        else:
            logger.debug("[TIDAL TOKEN REFRESH] Token still fresh, no refresh needed")

    def _perform_token_refresh(self):
        """Actually refresh the token via Tidal API"""
        import datetime

        try:
            logger.info("[TIDAL TOKEN REFRESH] Calling Tidal token refresh API...")

            if not hasattr(self._active_session, 'refresh_token') or not self._active_session.refresh_token:
                logger.error("[TIDAL TOKEN REFRESH] ✗ No refresh token available!")
                self._update_credential_status('failed', 'No refresh token available')
                return False

            refresh_token = self._active_session.refresh_token

            if self.auth_logging_enabled:
                logger.info("[TIDAL TOKEN REFRESH] Using refresh_token: %s...", refresh_token[:30])

            # Call tidalapi's token_refresh method
            success = self._active_session.token_refresh(refresh_token)

            if success:
                logger.info("[TIDAL TOKEN REFRESH] ✓ Token refresh successful!")

                if self.auth_logging_enabled:
                    logger.info("[TIDAL TOKEN REFRESH] New access_token: %s...", self._active_session.access_token[:30])
                    logger.info("[TIDAL TOKEN REFRESH] Expires at: %s", self._active_session.expiry_time)

                # Calculate new expiry duration
                now = datetime.datetime.utcnow()
                expires_in = int((self._active_session.expiry_time - now).total_seconds())
                logger.info("[TIDAL TOKEN REFRESH] New token expires in: %ss (%s days)", expires_in, expires_in/86400)

                # Update token lifetime based on ACTUAL token duration (not assumed 7 days)
                # This fixes refresh loop when tokens have shorter lifetimes (e.g., 3 hours for PKCE)
                self._token_original_lifetime = expires_in
                logger.info("[TIDAL TOKEN REFRESH] Updated token lifetime to: %ss (%s days)", expires_in, expires_in/86400)

                # Save session to file
                self._active_session.save_session_to_file(self.session_file_path)
                logger.info("[TIDAL TOKEN REFRESH] ✓ Session saved to: %s", self.session_file_path)

                # Update credentials via API
                self._update_credentials_via_api()

                # Update credential status to active
                self._update_credential_status('active')

                return True
            else:
                logger.error("[TIDAL TOKEN REFRESH] ✗ Token refresh failed!")
                logger.error("[TIDAL TOKEN REFRESH] The refresh token may have expired")

                # Update credential status to failed
                self._update_credential_status('failed', 'Token refresh failed - refresh token may be expired')

                return False

        except Exception as e:
            logger.error("[TIDAL TOKEN REFRESH] ✗ Exception during token refresh: %s", e, exc_info=True)

            # Update credential status to failed
            error_msg = f"Token refresh exception: {str(e)[:100]}"
            self._update_credential_status('failed', error_msg)

            return False

    def _update_credentials_via_api(self):
        """
        Update credentials via external API

        This allows the API to handle file writes and RPI client notifications.
        The API endpoint will:
        - Write to /data/source_credentials.json
        - Notify RPI client via CallbackRegistry
        - Handle proper serialization and error handling
        """
        import json as json_module
        import time as time_module
        import datetime

        try:
            logger.info("[TIDAL TOKEN REFRESH] Updating credentials via API...")

            # Get credential API configuration
            api_url = self._tidal_config.get("credential_api_url", "http://localhost:8001")
            api_user = self._tidal_config.get("credential_api_auth_user", "Admin")
            api_pass = self._tidal_config.get("credential_api_auth_pass", "Admin")
            source_id = self._tidal_config.get("source_id")

            if not source_id:
                logger.error("[TIDAL TOKEN REFRESH] No source_id configured - cannot update via API")
                logger.error("[TIDAL TOKEN REFRESH] Tokens are refreshed in memory but won't persist across restarts")
                return False

            # Build new token JSON from current session
            token_data = {
                'access_token': self._active_session.access_token,
                'refresh_token': self._active_session.refresh_token,
                'token_type': self._active_session.token_type,
                'session_id': self._active_session.session_id,
                'is_pkce': getattr(self._active_session, 'is_pkce', self.pkce_enabled),
                'created_at': int(time_module.time()),
            }

            # Add expiry info if available
            if hasattr(self._active_session, 'expiry_time') and self._active_session.expiry_time:
                expires_in = int((self._active_session.expiry_time - datetime.datetime.utcnow()).total_seconds())
                token_data['expires_in'] = expires_in
                token_data['expires_at'] = int(time_module.time()) + expires_in

            token_json = json_module.dumps(token_data)

            # Call API credential update endpoint
            import requests
            url = f"{api_url}/inputs/{source_id}/credential/tidal"
            auth = (api_user, api_pass)
            payload = {
                "token": token_json,
                "auto_refresh": True  # Flag to prevent unnecessary Mopidy restart
            }

            logger.info(f"[TIDAL TOKEN REFRESH] API endpoint: {url}")
            response = requests.post(url, json=payload, auth=auth, timeout=10)

            if response.status_code == 200:
                logger.info("[TIDAL TOKEN REFRESH] ✓ Credentials updated via API successfully")
                logger.info("[TIDAL TOKEN REFRESH] API has written to credential file and notified RPI client")
                return True
            else:
                logger.error(f"[TIDAL TOKEN REFRESH] ✗ API credential update failed: HTTP {response.status_code}")
                logger.error(f"[TIDAL TOKEN REFRESH] Response: {response.text}")
                return False

        except Exception as e:
            logger.error("[TIDAL TOKEN REFRESH] ✗ Failed to update credentials via API: %s", e, exc_info=True)
            logger.warning("[TIDAL TOKEN REFRESH] Tokens are still refreshed and active for this session")
            return False

    def _update_credential_status(self, status, error_message=None):
        """
        Update credential status via external API

        This allows the UI to detect when OAuth tokens have expired or failed,
        so the credential can be shown as "unlinked" or "expired" in the UI.

        Args:
            status: Status string - 'active', 'expired', 'failed', 'token_expired'
            error_message: Optional user-friendly error message to include
        """
        import json as json_module
        import time as time_module

        try:
            logger.info("=" * 80)
            logger.info("[TIDAL CREDENTIAL STATUS] Updating credential status to: %s", status)
            if error_message:
                logger.info("[TIDAL CREDENTIAL STATUS] Error message: %s", error_message)

            # Get source_id from config or auto-detect
            source_id = self._tidal_config.get("source_id")

            # Auto-detect source_id if not configured
            if not source_id and self._active_session and hasattr(self._active_session, 'access_token'):
                # Read credential file to auto-detect source_id
                credential_file = '/data/source_credentials.json'
                try:
                    with open(credential_file, 'r') as f:
                        data = json_module.load(f)

                    current_access_token = self._active_session.access_token
                    credentials = data.get('credentials', {})

                    for sid, services in credentials.items():
                        tidal_cred = services.get('tidal', {})
                        if tidal_cred.get('cred_type') == 'tidal':
                            stored_token = tidal_cred.get('auth', {}).get('token', '{}')
                            try:
                                stored_token_obj = json_module.loads(stored_token)
                                if stored_token_obj.get('access_token') == current_access_token:
                                    source_id = sid
                                    logger.info("[TIDAL CREDENTIAL STATUS] ✓ Auto-detected source_id: %s", source_id)
                                    break
                            except json_module.JSONDecodeError:
                                continue
                except FileNotFoundError:
                    logger.warning("[TIDAL CREDENTIAL STATUS] Credential file not found - cannot auto-detect source_id")

            if not source_id:
                logger.warning("[TIDAL CREDENTIAL STATUS] Cannot update status - source_id not found")
                logger.info("=" * 80)
                return False

            # Get standard message if not provided
            if status != "active" and error_message is None:
                error_message = {
                    "failed": "Authentication failed. Please try re-authenticating.",
                    "expired": "Your session has expired. Please log in again.",
                    "token_expired": "Token refresh failed. Please log in again.",
                }.get(status)

            # Build auth token JSON from current session
            token_data = {
                'access_token': self._active_session.access_token if self._active_session and hasattr(self._active_session, 'access_token') else '',
                'refresh_token': self._active_session.refresh_token if self._active_session and hasattr(self._active_session, 'refresh_token') else '',
                'token_type': self._active_session.token_type if self._active_session and hasattr(self._active_session, 'token_type') else 'Bearer',
                'session_id': self._active_session.session_id if self._active_session and hasattr(self._active_session, 'session_id') else '',
                'is_pkce': getattr(self._active_session, 'is_pkce', self.pkce_enabled),
                'created_at': int(time_module.time()),
            }

            # Add expiry info if available
            if self._active_session and hasattr(self._active_session, 'expiry_time') and self._active_session.expiry_time:
                import datetime
                expires_in = int((self._active_session.expiry_time - datetime.datetime.utcnow()).total_seconds())
                token_data['expires_in'] = expires_in
                token_data['expires_at'] = int(time_module.time()) + expires_in

            auth_token = json_module.dumps(token_data)

            # Prepare credential update
            credential_data = {
                "token": auth_token
            }

            if error_message:
                credential_data["message"] = error_message

            # Call credential API update endpoint
            try:
                import requests

                # Get credential API configuration
                api_url = self._tidal_config.get("credential_api_url", "http://localhost:8001")
                api_user = self._tidal_config.get("credential_api_auth_user", "Admin")
                api_pass = self._tidal_config.get("credential_api_auth_pass", "Admin")

                url = f"{api_url}/inputs/{source_id}/credential/tidal"
                auth = (api_user, api_pass)
                response = requests.post(url, json=credential_data, auth=auth, timeout=5)

                if response.status_code == 200:
                    logger.info("[TIDAL CREDENTIAL STATUS] ✓ Status updated via API to: %s", status)
                    if error_message:
                        logger.info("[TIDAL CREDENTIAL STATUS] Message: %s", error_message)
                    logger.info("=" * 80)
                    return True
                else:
                    logger.error("[TIDAL CREDENTIAL STATUS] ✗ Failed to update via API: HTTP %s", response.status_code)
                    logger.error("[TIDAL CREDENTIAL STATUS] Response: %s", response.text)
                    logger.info("=" * 80)
                    return False

            except Exception as e:
                logger.error("[TIDAL CREDENTIAL STATUS] ✗ Exception calling API: %s", e, exc_info=True)
                logger.info("=" * 80)
                return False

        except Exception as e:
            logger.error("[TIDAL CREDENTIAL STATUS] ✗ Failed to update credential status: %s", e, exc_info=True)
            logger.info("=" * 80)
            return False

    def _log_token_status(self):
        """Log token expiration information at startup"""
        import datetime

        try:
            if not hasattr(self._active_session, 'expiry_time') or not self._active_session.expiry_time:
                logger.info("[TIDAL TOKEN STATUS] No expiry_time available - token is fresh or doesn't expire")
                return

            now = datetime.datetime.utcnow()
            expires_at = self._active_session.expiry_time
            time_until_expiry = (expires_at - now).total_seconds()

            logger.info("=" * 80)
            logger.info("[TIDAL TOKEN STATUS] Token Expiration Information")
            logger.info("[TIDAL TOKEN STATUS] Current time (UTC): %s", now.strftime("%Y-%m-%d %H:%M:%S"))
            logger.info("[TIDAL TOKEN STATUS] Token expires at (UTC): %s", expires_at.strftime("%Y-%m-%d %H:%M:%S"))
            logger.info("[TIDAL TOKEN STATUS] Time until expiration: %s seconds (%.2f hours, %.2f days)",
                       int(time_until_expiry),
                       time_until_expiry / 3600,
                       time_until_expiry / 86400)

            # Warn if token is expiring soon
            if time_until_expiry < 0:
                logger.error("[TIDAL TOKEN STATUS] ⚠ TOKEN IS ALREADY EXPIRED!")
                logger.error("[TIDAL TOKEN STATUS] Automatic refresh will be attempted")
            elif time_until_expiry < 3600:  # Less than 1 hour
                logger.warning("[TIDAL TOKEN STATUS] ⚠ Token expires in less than 1 hour!")
            elif time_until_expiry < 86400:  # Less than 1 day
                logger.warning("[TIDAL TOKEN STATUS] ⚠ Token expires in less than 1 day")
            else:
                logger.info("[TIDAL TOKEN STATUS] ✓ Token is valid and fresh")

            logger.info("=" * 80)

        except Exception as e:
            logger.error("[TIDAL TOKEN STATUS] Error logging token status: %s", e, exc_info=True)

    def _log_user_metadata(self):
        """Log Tidal user metadata at startup"""
        try:
            if not self._active_session or not hasattr(self._active_session, 'user'):
                logger.info("[TIDAL USER METADATA] User information not available")
                return

            user = self._active_session.user
            if not user:
                logger.info("[TIDAL USER METADATA] User object is None")
                return

            logger.info("=" * 80)
            logger.info("[TIDAL USER METADATA] User Information")

            # Log available user attributes
            if hasattr(user, 'id'):
                logger.info("[TIDAL USER METADATA] User ID: %s", user.id)

            if hasattr(user, 'username'):
                logger.info("[TIDAL USER METADATA] Username: %s", user.username)

            if hasattr(user, 'country_code'):
                logger.info("[TIDAL USER METADATA] Country: %s", user.country_code)

            if hasattr(user, 'subscription'):
                subscription = user.subscription
                if hasattr(subscription, 'type'):
                    logger.info("[TIDAL USER METADATA] Subscription Type: %s", subscription.type)
                if hasattr(subscription, 'payment_type'):
                    logger.info("[TIDAL USER METADATA] Payment Type: %s", subscription.payment_type)
                if hasattr(subscription, 'offline_grace_period'):
                    logger.info("[TIDAL USER METADATA] Offline Grace Period: %s days", subscription.offline_grace_period)

            logger.info("=" * 80)

        except Exception as e:
            logger.error("[TIDAL USER METADATA] Error logging user metadata: %s", e, exc_info=True)
