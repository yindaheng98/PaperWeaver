from .dataclass import Paper, Author, Venue, DataSrc, DataDst
from .iface import WeaverIface, WeaverCacheIface
from .iface_link import AuthorLinkWeaverCacheIface, PaperLinkWeaverCacheIface
from .iface_a2p import Author2PapersWeaverIface, Author2PapersWeaverCacheIface
from .iface_p2a import Paper2AuthorsWeaverIface, Paper2AuthorsWeaverCacheIface
from .iface_p2c import Paper2CitationsWeaverIface, Paper2CitationsWeaverCacheIface
from .iface_p2r import Paper2ReferencesWeaverIface, Paper2ReferencesWeaverCacheIface
from .iface_p2v import Paper2VenuesWeaverIface, Paper2VenuesWeaverCacheIface
from .iface_v2p import Venue2PapersWeaverIface, Venue2PapersWeaverCacheIface
from .weaver_a2p2v import Author2Paper2VenueCache, Author2Paper2VenueWeaver
from .weaver_p2r2a import Paper2Reference2AuthorCache, Paper2Reference2AuthorWeaver
from .weaver_p_only import PaperOnlyWeaver
