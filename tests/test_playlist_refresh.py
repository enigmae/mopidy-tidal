"""
Tests for playlist refresh behavior.

These tests cover the refactored refresh logic including:
- Refreshing specific URIs (fetches directly from API)
- Refreshing all playlists (uses _current_tidal_playlists)
- metadata_only parameter behavior
- _playlist_needs_refresh() timestamp comparison
- _do_periodic_refresh() behavior
- as_list() initial population
- _get_or_refresh_playlist() cache miss handling
- Thread management for periodic refresh
"""

import threading
from unittest.mock import Mock, MagicMock, patch, call

import pytest
from mopidy.models import Playlist as MopidyPlaylist, Ref, Track

from mopidy_tidal.playlists import (
    PlaylistCache,
    PlaylistMetadataCache,
    TidalPlaylistsProvider,
)


@pytest.fixture
def mock_backend(mocker):
    """Create a mock backend with session."""
    backend = mocker.Mock()
    backend._config = {"tidal": {"playlist_cache_refresh_secs": 0}}
    backend.session = mocker.Mock()
    return backend


@pytest.fixture
def playlist_provider(mock_backend):
    """Create a TidalPlaylistsProvider with mocked backend."""
    tpp = TidalPlaylistsProvider(mock_backend)
    tpp._playlists = PlaylistCache(persist=False)
    tpp._playlists_metadata = PlaylistMetadataCache(persist=False)
    return tpp


@pytest.fixture
def mock_tidal_playlist(mocker):
    """Create a mock TidalPlaylist."""
    def _make_playlist(playlist_id="12345", name="Test Playlist", num_tracks=5, last_updated=1000):
        pl = mocker.Mock()
        pl.id = playlist_id
        pl.name = name
        pl.num_tracks = num_tracks
        pl.last_updated = last_updated
        pl.tracks = mocker.Mock()
        pl.tracks.__name__ = "tracks"
        pl.tracks.return_value = []
        return pl
    return _make_playlist


@pytest.fixture
def mock_mopidy_playlist():
    """Create a mock MopidyPlaylist."""
    def _make_playlist(uri="tidal:playlist:12345", name="Test Playlist", last_modified=1000, tracks=None):
        return MopidyPlaylist(
            uri=uri,
            name=name,
            last_modified=last_modified,
            tracks=tracks or [],
        )
    return _make_playlist


@pytest.fixture
def mock_tracks(mocker):
    """Create mock tracks."""
    def _make_tracks(count=3):
        tracks = []
        for i in range(count):
            track = mocker.Mock()
            track.id = i
            track.uri = f"tidal:track:{i}:{i}:{i}"
            track.name = f"Track-{i}"
            track.full_name = f"Track-{i}"
            track.artist = mocker.Mock()
            track.artist.name = "Artist"
            track.artist.id = i
            track.album = mocker.Mock()
            track.album.name = "Album"
            track.album.id = i
            track.duration = 180
            track.track_num = i + 1
            track.disc_num = 1
            tracks.append(track)
        return tracks
    return _make_tracks


# =============================================================================
# Tests for refresh() with specific URIs
# =============================================================================

class TestRefreshSpecificUri:
    """Tests for refresh() when called with specific URIs."""

    def test_refresh_specific_uri_fetches_from_api(
        self, playlist_provider, mock_backend, mock_tidal_playlist, mocker
    ):
        """When refresh(uri) is called, it fetches the playlist directly from the API."""
        mocker.patch("mopidy_tidal.playlists.backend.BackendListener")

        tidal_pl = mock_tidal_playlist(playlist_id="12345", name="My Playlist")
        mock_backend.session.playlist.return_value = tidal_pl

        # Mock track retrieval
        mocker.patch.object(
            playlist_provider, "_retrieve_api_tracks", return_value=[]
        )
        mocker.patch(
            "mopidy_tidal.playlists.full_models_mappers.create_mopidy_tracks",
            return_value=[]
        )

        playlist_provider.refresh("tidal:playlist:12345")

        # Verify session.playlist was called with the correct ID
        mock_backend.session.playlist.assert_called_once_with("12345")

    def test_refresh_specific_uri_prunes_cache_before_fetch(
        self, playlist_provider, mock_backend, mock_tidal_playlist, mock_mopidy_playlist, mocker
    ):
        """Cache entries are pruned before fetching fresh data."""
        mocker.patch("mopidy_tidal.playlists.backend.BackendListener")

        # Pre-populate caches
        uri = "tidal:playlist:12345"
        playlist_provider._playlists[uri] = mock_mopidy_playlist(uri=uri)
        playlist_provider._playlists_metadata[uri] = mock_mopidy_playlist(uri=uri)

        tidal_pl = mock_tidal_playlist(playlist_id="12345")
        mock_backend.session.playlist.return_value = tidal_pl

        mocker.patch.object(playlist_provider, "_retrieve_api_tracks", return_value=[])
        mocker.patch(
            "mopidy_tidal.playlists.full_models_mappers.create_mopidy_tracks",
            return_value=[]
        )

        # Spy on prune methods
        prune_spy = mocker.spy(playlist_provider._playlists, "prune")
        prune_metadata_spy = mocker.spy(playlist_provider._playlists_metadata, "prune")

        playlist_provider.refresh(uri)

        prune_spy.assert_called_with(uri)
        prune_metadata_spy.assert_called_with(uri)

    def test_refresh_specific_uri_not_found(
        self, playlist_provider, mock_backend, mocker
    ):
        """When session.playlist() returns None, no cache update occurs."""
        mocker.patch("mopidy_tidal.playlists.backend.BackendListener")

        mock_backend.session.playlist.return_value = None

        playlist_provider.refresh("tidal:playlist:nonexistent")

        # Caches should remain empty
        assert len(playlist_provider._playlists) == 0
        assert len(playlist_provider._playlists_metadata) == 0

    def test_refresh_specific_uri_updates_both_caches(
        self, playlist_provider, mock_backend, mock_tidal_playlist, mocker
    ):
        """Both caches are updated with the new playlist data."""
        mocker.patch("mopidy_tidal.playlists.backend.BackendListener")

        uri = "tidal:playlist:12345"
        tidal_pl = mock_tidal_playlist(playlist_id="12345", name="My Playlist", num_tracks=3)
        mock_backend.session.playlist.return_value = tidal_pl

        # Create actual Track objects (MopidyPlaylist validates track types)
        mock_tracks = [
            Track(uri=f"tidal:track:{i}:{i}:{i}", name=f"Track {i}")
            for i in range(3)
        ]
        mocker.patch.object(playlist_provider, "_retrieve_api_tracks", return_value=[])
        mocker.patch(
            "mopidy_tidal.playlists.full_models_mappers.create_mopidy_tracks",
            return_value=mock_tracks
        )

        playlist_provider.refresh(uri)

        # Both caches should be updated
        assert uri in playlist_provider._playlists
        assert uri in playlist_provider._playlists_metadata
        assert playlist_provider._playlists[uri].name == "My Playlist"
        assert playlist_provider._playlists_metadata[uri].name == "My Playlist"


# =============================================================================
# Tests for refresh() for all playlists
# =============================================================================

class TestRefreshAllPlaylists:
    """Tests for refresh() when called without URIs (refresh all)."""

    def test_refresh_all_uses_current_tidal_playlists(
        self, playlist_provider, mock_backend, mock_tidal_playlist, mocker
    ):
        """When refresh() is called without URIs, it uses _current_tidal_playlists."""
        mocker.patch("mopidy_tidal.playlists.backend.BackendListener")

        pl1 = mock_tidal_playlist(playlist_id="111", name="Playlist 1")
        pl2 = mock_tidal_playlist(playlist_id="222", name="Playlist 2")

        # Mock _calculate_added_and_removed_playlist_ids to populate _current_tidal_playlists
        def populate_playlists():
            playlist_provider._current_tidal_playlists = [pl1, pl2]
            return set(), set()

        mocker.patch.object(
            playlist_provider,
            "_calculate_added_and_removed_playlist_ids",
            side_effect=populate_playlists
        )
        mocker.patch.object(playlist_provider, "_retrieve_api_tracks", return_value=[])
        mocker.patch(
            "mopidy_tidal.playlists.full_models_mappers.create_mopidy_tracks",
            return_value=[]
        )

        playlist_provider.refresh()

        playlist_provider._calculate_added_and_removed_playlist_ids.assert_called_once()
        assert "tidal:playlist:111" in playlist_provider._playlists
        assert "tidal:playlist:222" in playlist_provider._playlists

    def test_refresh_all_clears_both_caches(
        self, playlist_provider, mock_backend, mock_mopidy_playlist, mocker
    ):
        """Both caches are completely cleared before repopulating."""
        mocker.patch("mopidy_tidal.playlists.backend.BackendListener")

        # Pre-populate caches
        playlist_provider._playlists["tidal:playlist:old"] = mock_mopidy_playlist()
        playlist_provider._playlists_metadata["tidal:playlist:old"] = mock_mopidy_playlist()

        mocker.patch.object(
            playlist_provider,
            "_calculate_added_and_removed_playlist_ids",
            side_effect=lambda: setattr(playlist_provider, "_current_tidal_playlists", []) or (set(), set())
        )

        clear_spy = mocker.spy(playlist_provider._playlists, "clear")
        clear_metadata_spy = mocker.spy(playlist_provider._playlists_metadata, "clear")

        playlist_provider.refresh()

        clear_spy.assert_called_once()
        clear_metadata_spy.assert_called_once()


# =============================================================================
# Tests for metadata_only parameter
# =============================================================================

class TestMetadataOnly:
    """Tests for the metadata_only parameter."""

    def test_refresh_metadata_only_skips_track_fetch(
        self, playlist_provider, mock_backend, mock_tidal_playlist, mocker
    ):
        """When metadata_only=True, tracks are not fetched."""
        mocker.patch("mopidy_tidal.playlists.backend.BackendListener")

        uri = "tidal:playlist:12345"
        tidal_pl = mock_tidal_playlist(playlist_id="12345")
        mock_backend.session.playlist.return_value = tidal_pl

        retrieve_tracks_mock = mocker.patch.object(
            playlist_provider, "_retrieve_api_tracks"
        )

        playlist_provider.refresh(uri, metadata_only=True)

        # _retrieve_api_tracks should NOT be called
        retrieve_tracks_mock.assert_not_called()

        # Metadata cache should be updated
        assert uri in playlist_provider._playlists_metadata

        # Full playlist cache should NOT be updated
        assert uri not in playlist_provider._playlists

    def test_refresh_fetches_tracks_when_not_metadata_only(
        self, playlist_provider, mock_backend, mock_tidal_playlist, mocker
    ):
        """When metadata_only=False (default), tracks are fetched."""
        mocker.patch("mopidy_tidal.playlists.backend.BackendListener")

        uri = "tidal:playlist:12345"
        tidal_pl = mock_tidal_playlist(playlist_id="12345")
        mock_backend.session.playlist.return_value = tidal_pl

        retrieve_tracks_mock = mocker.patch.object(
            playlist_provider, "_retrieve_api_tracks", return_value=[]
        )
        mocker.patch(
            "mopidy_tidal.playlists.full_models_mappers.create_mopidy_tracks",
            return_value=[]
        )

        playlist_provider.refresh(uri, metadata_only=False)

        # _retrieve_api_tracks SHOULD be called
        retrieve_tracks_mock.assert_called_once()

        # Both caches should be updated
        assert uri in playlist_provider._playlists_metadata
        assert uri in playlist_provider._playlists


# =============================================================================
# Tests for _playlist_needs_refresh()
# =============================================================================

class TestPlaylistNeedsRefresh:
    """Tests for _playlist_needs_refresh() timestamp comparison."""

    def test_returns_true_when_cached_is_none(
        self, playlist_provider, mock_tidal_playlist
    ):
        """Returns True when there's no cached playlist."""
        tidal_pl = mock_tidal_playlist()

        result = playlist_provider._playlist_needs_refresh(tidal_pl, None)

        assert result is True

    def test_returns_true_when_upstream_newer(
        self, playlist_provider, mock_tidal_playlist, mock_mopidy_playlist
    ):
        """Returns True when upstream last_updated > cached last_modified."""
        tidal_pl = mock_tidal_playlist(last_updated=2000)
        cached = mock_mopidy_playlist(last_modified=1000)

        result = playlist_provider._playlist_needs_refresh(tidal_pl, cached)

        assert result is True

    def test_returns_false_when_cached_is_current(
        self, playlist_provider, mock_tidal_playlist, mock_mopidy_playlist
    ):
        """Returns False when cached timestamp matches upstream."""
        tidal_pl = mock_tidal_playlist(last_updated=1000)
        cached = mock_mopidy_playlist(last_modified=1000)

        result = playlist_provider._playlist_needs_refresh(tidal_pl, cached)

        assert result is False

    def test_returns_false_when_cached_is_newer(
        self, playlist_provider, mock_tidal_playlist, mock_mopidy_playlist
    ):
        """Returns False when cached is newer than upstream (edge case)."""
        tidal_pl = mock_tidal_playlist(last_updated=1000)
        cached = mock_mopidy_playlist(last_modified=2000)

        result = playlist_provider._playlist_needs_refresh(tidal_pl, cached)

        assert result is False

    def test_returns_true_when_no_upstream_timestamp(
        self, playlist_provider, mock_mopidy_playlist, mocker
    ):
        """Returns True when upstream has no last_updated attribute."""
        tidal_pl = mocker.Mock(spec=[])  # No last_updated attribute
        cached = mock_mopidy_playlist(last_modified=1000)

        result = playlist_provider._playlist_needs_refresh(tidal_pl, cached)

        assert result is True


# =============================================================================
# Tests for _do_periodic_refresh()
# =============================================================================

class TestDoPeriodicRefresh:
    """Tests for _do_periodic_refresh() behavior."""

    def test_periodic_refresh_only_updates_changed_playlists(
        self, playlist_provider, mock_backend, mock_tidal_playlist, mock_mopidy_playlist, mocker
    ):
        """Only playlists with newer last_updated have their tracks fetched."""
        mocker.patch("mopidy_tidal.playlists.backend.BackendListener")

        # Two playlists: one changed, one unchanged
        pl_changed = mock_tidal_playlist(playlist_id="111", name="Changed", last_updated=2000)
        pl_unchanged = mock_tidal_playlist(playlist_id="222", name="Unchanged", last_updated=1000)

        # Cache has the unchanged playlist with same timestamp
        playlist_provider._playlists["tidal:playlist:222"] = mock_mopidy_playlist(
            uri="tidal:playlist:222", last_modified=1000
        )

        mocker.patch.object(
            playlist_provider,
            "_calculate_added_and_removed_playlist_ids",
            side_effect=lambda: setattr(playlist_provider, "_current_tidal_playlists", [pl_changed, pl_unchanged]) or (set(), set())
        )

        retrieve_tracks_mock = mocker.patch.object(
            playlist_provider, "_retrieve_api_tracks", return_value=[]
        )
        mocker.patch(
            "mopidy_tidal.playlists.full_models_mappers.create_mopidy_tracks",
            return_value=[]
        )

        playlist_provider._do_periodic_refresh()

        # _retrieve_api_tracks should only be called for the changed playlist
        assert retrieve_tracks_mock.call_count == 1
        retrieve_tracks_mock.assert_called_with(mock_backend.session, pl_changed)

    def test_periodic_refresh_updates_all_metadata(
        self, playlist_provider, mock_backend, mock_tidal_playlist, mock_mopidy_playlist, mocker
    ):
        """All playlist metadata is updated regardless of change status."""
        mocker.patch("mopidy_tidal.playlists.backend.BackendListener")

        pl1 = mock_tidal_playlist(playlist_id="111", name="Playlist 1", last_updated=1000)
        pl2 = mock_tidal_playlist(playlist_id="222", name="Playlist 2", last_updated=1000)

        # Both cached with same timestamp (no changes)
        playlist_provider._playlists["tidal:playlist:111"] = mock_mopidy_playlist(
            uri="tidal:playlist:111", last_modified=1000
        )
        playlist_provider._playlists["tidal:playlist:222"] = mock_mopidy_playlist(
            uri="tidal:playlist:222", last_modified=1000
        )

        mocker.patch.object(
            playlist_provider,
            "_calculate_added_and_removed_playlist_ids",
            side_effect=lambda: setattr(playlist_provider, "_current_tidal_playlists", [pl1, pl2]) or (set(), set())
        )

        playlist_provider._do_periodic_refresh()

        # Both should have metadata updated
        assert "tidal:playlist:111" in playlist_provider._playlists_metadata
        assert "tidal:playlist:222" in playlist_provider._playlists_metadata

    def test_periodic_refresh_handles_exceptions(
        self, playlist_provider, mocker
    ):
        """Exceptions are caught and logged, don't crash."""
        mocker.patch.object(
            playlist_provider,
            "_calculate_added_and_removed_playlist_ids",
            side_effect=Exception("API Error")
        )

        # Should not raise
        playlist_provider._do_periodic_refresh()


# =============================================================================
# Tests for as_list() initial population
# =============================================================================

class TestAsListInitialPopulation:
    """Tests for as_list() triggering initial refresh."""

    def test_as_list_triggers_refresh_when_cache_empty(
        self, playlist_provider, mock_backend, mocker
    ):
        """First call to as_list() triggers metadata refresh when cache is empty."""
        mocker.patch("mopidy_tidal.playlists.backend.BackendListener")

        refresh_mock = mocker.patch.object(playlist_provider, "refresh")

        # Cache is empty
        assert len(playlist_provider._playlists_metadata) == 0

        playlist_provider.as_list()

        refresh_mock.assert_called_once_with(metadata_only=True)

    def test_as_list_returns_cached_data_without_refresh(
        self, playlist_provider, mock_mopidy_playlist, mocker
    ):
        """Returns playlist refs from cache without triggering refresh."""
        mocker.patch("mopidy_tidal.playlists.backend.BackendListener")

        # Pre-populate cache
        playlist_provider._playlists_metadata["tidal:playlist:123"] = mock_mopidy_playlist(
            uri="tidal:playlist:123", name="My Playlist"
        )

        refresh_mock = mocker.patch.object(playlist_provider, "refresh")

        result = playlist_provider.as_list()

        # Should not trigger refresh
        refresh_mock.assert_not_called()

        # Should return the cached playlist
        assert len(result) == 1
        assert result[0].uri == "tidal:playlist:123"
        assert result[0].name == "My Playlist"


# =============================================================================
# Tests for _get_or_refresh_playlist()
# =============================================================================

class TestGetOrRefreshPlaylist:
    """Tests for _get_or_refresh_playlist() cache miss handling."""

    def test_returns_cached_playlist(
        self, playlist_provider, mock_mopidy_playlist, mocker
    ):
        """Returns cached playlist when available."""
        uri = "tidal:playlist:12345"
        cached = mock_mopidy_playlist(uri=uri, name="Cached Playlist")
        playlist_provider._playlists[uri] = cached

        refresh_mock = mocker.patch.object(playlist_provider, "refresh")

        result = playlist_provider._get_or_refresh_playlist(uri)

        assert result == cached
        refresh_mock.assert_not_called()

    def test_triggers_refresh_on_cache_miss(
        self, playlist_provider, mock_backend, mock_mopidy_playlist, mocker
    ):
        """Triggers refresh when playlist not in cache."""
        mocker.patch("mopidy_tidal.playlists.backend.BackendListener")

        uri = "tidal:playlist:12345"

        # Set up refresh to populate the cache
        def populate_cache(*args, **kwargs):
            playlist_provider._playlists[uri] = mock_mopidy_playlist(uri=uri)

        refresh_mock = mocker.patch.object(
            playlist_provider, "refresh", side_effect=populate_cache
        )

        result = playlist_provider._get_or_refresh_playlist(uri)

        refresh_mock.assert_called_once_with(uri)
        assert result is not None

    def test_handles_mix_uri(
        self, playlist_provider, mock_backend, mocker
    ):
        """Handles mix:// URIs by looking up the mix."""
        mock_mix = mocker.Mock()
        mock_mix.items.return_value = []
        mock_backend.session.mix.return_value = mock_mix

        mocker.patch(
            "mopidy_tidal.playlists.full_models_mappers.create_mopidy_mix_playlist",
            return_value=MopidyPlaylist(uri="tidal:mix:abc", name="Mix")
        )

        result = playlist_provider._get_or_refresh_playlist("tidal:mix:abc")

        mock_backend.session.mix.assert_called_once_with("abc")
        assert result.uri == "tidal:mix:abc"


# =============================================================================
# Tests for thread management
# =============================================================================

class TestThreadManagement:
    """Tests for periodic refresh thread management."""

    def test_start_periodic_refresh_does_nothing_when_config_not_set(
        self, playlist_provider, mock_backend
    ):
        """Thread is not started when playlist_cache_refresh_secs is not configured."""
        mock_backend._config = {"tidal": {}}

        playlist_provider.start_periodic_refresh()

        assert playlist_provider._refresh_thread is None

    def test_start_periodic_refresh_does_nothing_when_config_zero(
        self, playlist_provider, mock_backend
    ):
        """Thread is not started when config is 0."""
        mock_backend._config = {"tidal": {"playlist_cache_refresh_secs": 0}}

        playlist_provider.start_periodic_refresh()

        assert playlist_provider._refresh_thread is None

    def test_start_periodic_refresh_does_nothing_when_config_negative(
        self, playlist_provider, mock_backend
    ):
        """Thread is not started when config is negative."""
        mock_backend._config = {"tidal": {"playlist_cache_refresh_secs": -1}}

        playlist_provider.start_periodic_refresh()

        assert playlist_provider._refresh_thread is None

    def test_start_periodic_refresh_starts_thread(
        self, playlist_provider, mock_backend, mocker
    ):
        """Thread is started with correct period when config is valid."""
        mock_backend._config = {"tidal": {"playlist_cache_refresh_secs": 300}}

        mock_thread_class = mocker.patch("mopidy_tidal.playlists.PeriodicThread")
        mock_thread_instance = mocker.Mock()
        mock_thread_class.return_value = mock_thread_instance

        playlist_provider.start_periodic_refresh()

        mock_thread_class.assert_called_once_with(
            target=playlist_provider._do_periodic_refresh,
            period=300,
            name="tidal-playlist-refresh",
            daemon=True
        )
        mock_thread_instance.start.assert_called_once()
        assert playlist_provider._refresh_thread is mock_thread_instance

    def test_stop_periodic_refresh_stops_thread(
        self, playlist_provider, mocker
    ):
        """Thread is stopped and joined cleanly."""
        mock_thread = mocker.Mock()
        mock_thread.is_alive.return_value = False
        playlist_provider._refresh_thread = mock_thread

        playlist_provider.stop_periodic_refresh()

        mock_thread.stop.assert_called_once()
        mock_thread.join.assert_called_once_with(timeout=5)
        assert playlist_provider._refresh_thread is None

    def test_stop_periodic_refresh_does_nothing_when_no_thread(
        self, playlist_provider
    ):
        """Does nothing when there's no thread running."""
        playlist_provider._refresh_thread = None

        # Should not raise
        playlist_provider.stop_periodic_refresh()

        assert playlist_provider._refresh_thread is None