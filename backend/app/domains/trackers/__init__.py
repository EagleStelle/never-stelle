"""Trackers: collection links listed on a schedule, their new entries queued as downloads.

- listing   : streams a link's entries from gallery-dl or yt-dlp, site-agnostic
- service   : tracker CRUD and one check (list, queue unseen, record)
- scheduler : the on-demand thread that runs due checks one at a time
"""
