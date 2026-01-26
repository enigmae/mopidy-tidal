from __future__ import unicode_literals

import difflib
import logging
import operator
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Collection, List, Optional, Tuple, Union

from continuous_threading import PeriodicThread
from mopidy import backend
from mopidy.models import Playlist as MopidyPlaylist
from mopidy.models import Ref, Track
from requests import HTTPError
from tidalapi.playlist import Playlist as TidalPlaylist

from mopidy_tidal import full_models_mappers
from mopidy_tidal.full_models_mappers import create_mopidy_playlist
from mopidy_tidal.helpers import to_timestamp
from mopidy_tidal.login_hack import login_hack
from mopidy_tidal.lru_cache import LruCache
from mopidy_tidal.utils import mock_track
from mopidy_tidal.workers import get_items

if TYPE_CHECKING:  # pragma: no cover
    from mopidy_tidal.backend import TidalBackend

logger = logging.getLogger(__name__)


class PlaylistCache(LruCache):
    def __getitem__(
        self, key: Union[str, TidalPlaylist], *args, **kwargs
    ) -> MopidyPlaylist:
        uri = key.id if isinstance(key, TidalPlaylist) else key
        assert uri
        uri = f"tidal:playlist:{uri}" if not uri.startswith("tidal:playlist:") else uri

        playlist = super().__getitem__(uri, *args, **kwargs)
        if (
            playlist
            and isinstance(key, TidalPlaylist)
            and to_timestamp(key.last_updated) > to_timestamp(playlist.last_modified)
        ):
            # The playlist has been updated since last time:
            # we should refresh the associated cache entry
            logger.info('The playlist "%s" has been updated: refresh forced', key.name)

            raise KeyError(uri)

        return playlist


class PlaylistMetadataCache(PlaylistCache):
    def cache_file(self, key: str) -> Path:
        return super().cache_file(key, Path("playlist_metadata"))


class TidalPlaylistsProvider(backend.PlaylistsProvider):
    backend: "TidalBackend"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._playlists_metadata = PlaylistMetadataCache()
        self._playlists = PlaylistCache()
        self._current_tidal_playlists = []
        self._cache_lock = threading.RLock()
        self._refresh_thread: Optional[PeriodicThread] = None

    def start_periodic_refresh(self):
        """Start the periodic refresh thread. Called by backend on_start."""
        refresh_secs = self.backend._config["tidal"].get("playlist_cache_refresh_secs")
        if not refresh_secs or refresh_secs <= 0:
            logger.info("Playlist periodic refresh disabled (playlist_cache_refresh_secs not set or <= 0)")
            return

        self._refresh_thread = PeriodicThread(
            target=self._do_periodic_refresh,
            period=refresh_secs,
            name="tidal-playlist-refresh",
            daemon=True
        )
        self._refresh_thread.start()
        logger.info("Started playlist periodic refresh thread (every %s seconds)", refresh_secs)

    def stop_periodic_refresh(self):
        """Stop the periodic refresh thread. Called by backend on_stop."""
        if self._refresh_thread:
            logger.info("Stopping playlist periodic refresh thread...")
            self._refresh_thread.stop()
            self._refresh_thread.join(timeout=5)
            if self._refresh_thread.is_alive():
                logger.warning("Playlist refresh thread did not stop cleanly")
            else:
                logger.info("Playlist periodic refresh thread stopped")
            self._refresh_thread = None

    def _do_periodic_refresh(self):
        """Perform a periodic refresh, only updating playlists that have changed."""
        try:
            logger.debug("Periodic playlist refresh starting...")

            # Fetch fresh playlist list from Tidal
            self._calculate_added_and_removed_playlist_ids()

            session = self.backend.session
            mapped_playlists = {}
            mapped_metadata = {}

            for pl in self._current_tidal_playlists:
                uri = "tidal:playlist:" + pl.id

                # Always update metadata
                mapped_metadata[uri] = MopidyPlaylist(
                    uri=uri,
                    name=pl.name,
                    tracks=[mock_track] * pl.num_tracks,
                    last_modified=to_timestamp(pl.last_updated),
                )

                # Check if full playlist needs refresh based on last_updated timestamp
                with self._cache_lock:
                    cached = self._playlists.get(uri)

                if self._playlist_needs_refresh(pl, cached):
                    logger.info('Playlist "%s" has changed, refreshing tracks...', pl.name)
                    pl_tracks = self._retrieve_api_tracks(session, pl)
                    tracks = full_models_mappers.create_mopidy_tracks(pl_tracks)
                    mapped_playlists[uri] = MopidyPlaylist(
                        uri=uri,
                        name=pl.name,
                        tracks=tracks,
                        last_modified=to_timestamp(pl.last_updated),
                    )

            # Update caches atomically
            with self._cache_lock:
                self._playlists_metadata.update(mapped_metadata)
                self._playlists.update(mapped_playlists)

            backend.BackendListener.send("playlists_loaded")
            logger.debug("Periodic playlist refresh complete")

        except Exception as e:
            logger.error("Error in periodic playlist refresh: %s", e, exc_info=True)

    def _playlist_needs_refresh(self, tidal_playlist: TidalPlaylist, cached: Optional[MopidyPlaylist]) -> bool:
        """Check if a playlist needs refresh by comparing timestamps."""
        if cached is None:
            return True

        upstream_last_updated = to_timestamp(getattr(tidal_playlist, "last_updated", None))
        local_last_updated = to_timestamp(cached.last_modified)

        if not upstream_last_updated:
            # Can't determine if changed, assume it needs refresh
            return True

        return upstream_last_updated > local_last_updated

    def _calculate_added_and_removed_playlist_ids(
        self,
    ) -> Tuple[Collection[str], Collection[str]]:
        logger.debug("Calculating playlist updates...")
        session = self.backend.session
        updated_playlists = []

        with ThreadPoolExecutor(
            2, thread_name_prefix="mopidy-tidal-playlists-refresh-"
        ) as pool:
            pool_res = pool.map(
                lambda func: get_items(func)
                if func == session.user.favorites.playlists
                else func(),
                [
                    session.user.favorites.playlists,
                    session.user.playlists,
                ],
            )

            for playlists in pool_res:
                updated_playlists += playlists

        self._current_tidal_playlists = updated_playlists
        updated_ids = set(pl.id for pl in updated_playlists)

        with self._cache_lock:
            if not self._playlists_metadata:
                return updated_ids, set()

            current_ids = set(uri.split(":")[-1] for uri in self._playlists_metadata.keys())
            added_ids = updated_ids.difference(current_ids)
            removed_ids = current_ids.difference(updated_ids)

            # Prune removed playlists from both caches
            uris_to_remove = [
                uri
                for uri in self._playlists_metadata.keys()
                if uri.split(":")[-1] in removed_ids
            ]
            self._playlists_metadata.prune(*uris_to_remove)
            self._playlists.prune(*uris_to_remove)

        return added_ids, removed_ids

    @login_hack(List[Ref.playlist])
    def as_list(self) -> List[Ref]:
        """Return list of playlists from cache. Read-only operation."""
        logger.debug("Listing TIDAL playlists from cache...")

        # On first call, if metadata cache is empty, populate it
        with self._cache_lock:
            is_empty = not self._playlists_metadata

        if is_empty:
            logger.info("Playlist metadata cache is empty, triggering initial refresh...")
            self.refresh(metadata_only=True)

        with self._cache_lock:
            refs = [
                Ref.playlist(uri=pl.uri, name=pl.name)
                for pl in self._playlists_metadata.values()
            ]
        return sorted(refs, key=operator.attrgetter("name"))

    def _lookup_mix(self, uri):
        mix_id = uri.split(":")[-1]
        session = self.backend.session
        return session.mix(mix_id)

    def _get_or_refresh_playlist(self, uri) -> Optional[MopidyPlaylist]:
        """Get playlist from cache. Only triggers refresh on cache miss."""
        parts = uri.split(":")
        if parts[1] == "mix":
            mix = self._lookup_mix(uri)
            return full_models_mappers.create_mopidy_mix_playlist(mix)

        with self._cache_lock:
            playlist = self._playlists.get(uri)

        if playlist is None:
            # Cache miss - trigger refresh for this specific playlist
            self.refresh(uri)
            with self._cache_lock:
                playlist = self._playlists.get(uri)

        return playlist

    def create(self, name):
        tidal_playlist = self.backend.session.user.create_playlist(name, "")
        pl = create_mopidy_playlist(tidal_playlist, [])

        self._current_tidal_playlists.append(tidal_playlist)
        self.refresh(pl.uri)
        return pl

    def delete(self, uri):
        playlist_id = uri.split(":")[-1]
        session = self.backend.session

        try:
            session.request.request(
                "DELETE",
                "playlists/{playlist_id}".format(
                    playlist_id=playlist_id,
                ),
            )
        except HTTPError as e:
            # If we got a 401, it's likely that the user is following
            # this playlist but they don't have permissions for removing
            # it. If that's the case, remove the playlist from the
            # favourites instead of deleting it.
            if e.response.status_code == 401 and uri in {
                f"tidal:playlist:{pl.id}" for pl in session.user.favorites.playlists()
            }:
                session.user.favorites.remove_playlist(playlist_id)
            else:
                raise e

        with self._cache_lock:
            self._playlists_metadata.prune(uri)
            self._playlists.prune(uri)

    @login_hack
    def lookup(self, uri) -> Optional[MopidyPlaylist]:
        return self._get_or_refresh_playlist(uri)

    @login_hack
    def refresh(self, *uris, metadata_only: bool = False):
        if uris:
            logger.info("Refreshing playlists: %r", uris)
        else:
            logger.info("Refreshing all TIDAL playlists...")

        session = self.backend.session
        mapped_playlists = {}
        mapped_metadata = {}

        if uris:
            # Refreshing specific URIs - fetch each playlist directly from API
            with self._cache_lock:
                for uri in uris:
                    self._playlists_metadata.prune(uri)
                    self._playlists.prune(uri)

            for uri in uris:
                playlist_id = uri.split(":")[-1]
                pl = session.playlist(playlist_id)
                if not pl:
                    logger.warning("Could not fetch playlist %s from Tidal API", uri)
                    continue

                # Create metadata entry with mock tracks
                mapped_metadata[uri] = MopidyPlaylist(
                    uri=uri,
                    name=pl.name,
                    tracks=[mock_track] * pl.num_tracks,
                    last_modified=to_timestamp(pl.last_updated),
                )

                # Fetch full playlist with tracks (skip if metadata_only)
                if not metadata_only:
                    pl_tracks = self._retrieve_api_tracks(session, pl)
                    tracks = full_models_mappers.create_mopidy_tracks(pl_tracks)

                    mapped_playlists[uri] = MopidyPlaylist(
                        uri=uri,
                        name=pl.name,
                        tracks=tracks,
                        last_modified=to_timestamp(pl.last_updated),
                    )
        else:
            # Refreshing all playlists - use the playlist list from user library
            self._calculate_added_and_removed_playlist_ids()

            with self._cache_lock:
                self._playlists_metadata.clear()
                self._playlists.clear()

            for pl in self._current_tidal_playlists:
                uri = "tidal:playlist:" + pl.id

                # Create metadata entry with mock tracks
                mapped_metadata[uri] = MopidyPlaylist(
                    uri=uri,
                    name=pl.name,
                    tracks=[mock_track] * pl.num_tracks,
                    last_modified=to_timestamp(pl.last_updated),
                )

                # Fetch full playlist with tracks (skip if metadata_only)
                if not metadata_only:
                    pl_tracks = self._retrieve_api_tracks(session, pl)
                    tracks = full_models_mappers.create_mopidy_tracks(pl_tracks)

                    mapped_playlists[uri] = MopidyPlaylist(
                        uri=uri,
                        name=pl.name,
                        tracks=tracks,
                        last_modified=to_timestamp(pl.last_updated),
                    )

        # Update caches atomically
        with self._cache_lock:
            self._playlists_metadata.update(mapped_metadata)
            if not metadata_only:
                self._playlists.update(mapped_playlists)

        backend.BackendListener.send("playlists_loaded")
        logger.info("TIDAL playlists refreshed")

    @login_hack
    def get_items(self, uri) -> Optional[List[Ref]]:
        """Get playlist items from cache. Read-only operation (triggers refresh on cache miss)."""
        playlist = self._get_or_refresh_playlist(uri)
        if not playlist:
            return None

        return [Ref.track(uri=t.uri, name=t.name) for t in playlist.tracks]

    def _retrieve_api_tracks(self, session, playlist):
        getter_args = tuple()
        return get_items(playlist.tracks, *getter_args)

    def save(self, playlist):
        old_playlist = self._get_or_refresh_playlist(playlist.uri)
        session = self.backend.session
        playlist_id = playlist.uri.split(":")[-1]
        assert old_playlist, f"No such playlist: {playlist.uri}"
        assert session, "No active session"
        upstream_playlist = session.playlist(playlist_id)

        # Playlist rename case
        if old_playlist.name != playlist.name:
            upstream_playlist.edit(title=playlist.name)

        additions = []
        removals = []
        remove_offset = 0
        diff_lines = difflib.ndiff(
            [t.uri for t in old_playlist.tracks], [t.uri for t in playlist.tracks]
        )

        for diff_line in diff_lines:
            if diff_line.startswith("+ "):
                additions.append(diff_line[2:].split(":")[-1])
            else:
                if diff_line.startswith("- "):
                    removals.append(remove_offset)
                remove_offset += 1

        # Process removals in descending order so we don't have to recalculate
        # the offsets while we remove tracks
        if removals:
            logger.info(
                'Removing %d tracks from the playlist "%s"',
                len(removals),
                playlist.name,
            )

            removals.reverse()
            for idx in removals:
                upstream_playlist.remove_by_index(idx)

        # tidalapi currently only supports appending tracks to the end of the
        # playlist
        if additions:
            logger.info(
                'Adding %d tracks to the playlist "%s"', len(additions), playlist.name
            )

            upstream_playlist.add(additions)

        # remove all defunct tracks from cache
        self._calculate_added_and_removed_playlist_ids()
        # force update the whole playlist so all state is good
        self.refresh(playlist.uri)
