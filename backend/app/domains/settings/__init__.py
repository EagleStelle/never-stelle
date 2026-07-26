from backend.app.domains.settings.cookies import (
    clear_ytdlp_cookies_upload,
    detect_cookie_source,
    find_cookies_file_for_source,
    find_cookies_file_for_url,
    get_ytdlp_cookies_status,
    has_cookies_for_source,
    has_cookies_for_url,
    save_ytdlp_cookies_upload,
)
from backend.app.domains.settings.fields import (
    get_effective_creator_fields,
    get_effective_source_creator_fields_map,
    get_effective_title_cleaning,
    get_source_creator_field_defaults,
    is_scraper_creator_field,
    normalize_creator_field,
    normalize_source_creator_fields,
    normalize_source_title_cleaning,
    scraper_creator_field,
    scraper_token_from_creator_field,
)
from backend.app.domains.settings.formats import add_source_and_learn_format, set_learned_format_templates
from backend.app.domains.settings.locations import normalize_source_location_selection
from backend.app.domains.settings.profiles import (
    ensure_source_profile_for_url,
    get_effective_source_profiles,
    get_source_profile_by_key,
    get_source_profile_for_url,
    require_settings_managed_source,
)
from backend.app.domains.settings.scraping import (
    get_effective_scrape_rules,
    load_scrape_rules,
    normalize_source_scrape_rules,
)
from backend.app.domains.settings.service import build_settings_response, get_effective_saved_settings, persist_settings
from backend.app.domains.settings.slug_tokens import (
    get_effective_slug_tokens,
    load_slug_tokens,
    normalize_source_slug_tokens,
)
from backend.app.domains.settings.storage import load_saved_settings_file, save_saved_settings_file
from backend.app.domains.settings.templates import (
    BUILTIN_FILENAME_TEMPLATE,
    BUILTIN_FOLDER_TEMPLATE,
    get_effective_template_settings,
    normalize_source_template_selection,
    normalize_template_settings,
)
from backend.app.domains.settings.tokens import (
    get_effective_token_roles,
    load_token_roles,
    normalize_source_token_roles,
    normalize_token_name,
)

__all__ = [
    "BUILTIN_FILENAME_TEMPLATE",
    "BUILTIN_FOLDER_TEMPLATE",
    "add_source_and_learn_format",
    "build_settings_response",
    "clear_ytdlp_cookies_upload",
    "detect_cookie_source",
    "ensure_source_profile_for_url",
    "find_cookies_file_for_source",
    "find_cookies_file_for_url",
    "get_effective_creator_fields",
    "get_effective_saved_settings",
    "get_effective_scrape_rules",
    "get_effective_slug_tokens",
    "get_effective_source_creator_fields_map",
    "get_effective_source_profiles",
    "get_effective_template_settings",
    "get_effective_title_cleaning",
    "get_effective_token_roles",
    "get_source_creator_field_defaults",
    "get_source_profile_by_key",
    "get_source_profile_for_url",
    "get_ytdlp_cookies_status",
    "has_cookies_for_source",
    "has_cookies_for_url",
    "is_scraper_creator_field",
    "load_saved_settings_file",
    "load_scrape_rules",
    "load_slug_tokens",
    "load_token_roles",
    "normalize_creator_field",
    "normalize_source_creator_fields",
    "normalize_source_location_selection",
    "normalize_source_scrape_rules",
    "normalize_source_slug_tokens",
    "normalize_source_template_selection",
    "normalize_source_title_cleaning",
    "normalize_source_token_roles",
    "normalize_template_settings",
    "normalize_token_name",
    "persist_settings",
    "require_settings_managed_source",
    "save_saved_settings_file",
    "save_ytdlp_cookies_upload",
    "scraper_creator_field",
    "scraper_token_from_creator_field",
    "set_learned_format_templates",
]
