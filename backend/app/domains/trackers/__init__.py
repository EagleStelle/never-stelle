"""Trackers: collection links listed on a schedule, their new entries queued as downloads.

- listing   : streams a link's entries from gallery-dl or yt-dlp, site-agnostic
- service   : tracker CRUD, one check (list, queue unseen, record) and the entries only seen
- scheduler : the on-demand thread that runs due checks one at a time
"""
