from .dataclass import DataSrc, DataDst
from .iface import SimpleWeaver
from .iface_init import WeaverInitializerIface, VenuesWeaverInitializerIface
from .iface_v2p import Venue2PapersWeaverCacheIface, Venue2PapersWeaverIface
from .iface_a2p import Author2PapersWeaverCacheIface, Author2PapersWeaverIface
from .iface_p2a import Paper2AuthorsWeaverCacheIface, Paper2AuthorsWeaverIface
from .iface_p2v import Paper2VenuesWeaverCacheIface, Paper2VenuesWeaverIface


class Author2Paper2VenueCache(Venue2PapersWeaverCacheIface, Author2PapersWeaverCacheIface, Paper2AuthorsWeaverCacheIface, Paper2VenuesWeaverCacheIface):
    pass


class Author2Paper2VenueWeaver(SimpleWeaver, Venue2PapersWeaverIface, Author2PapersWeaverIface, Paper2AuthorsWeaverIface, Paper2VenuesWeaverIface):

    def __init__(
        self,
        src: DataSrc,
        dst: DataDst,
        cache: Author2Paper2VenueCache,
        initializer: WeaverInitializerIface
    ):
        if not isinstance(initializer, VenuesWeaverInitializerIface):
            raise TypeError("Author2Paper2VenueWeaver requires VenuesWeaverInitializerIface")
        super().__init__(src=src, dst=dst, cache=cache, initializer=initializer)

    async def init(self) -> int:
        paper_succ_count = await super().init()  # the init in Venue2PapersWeaverIface
        author_succ_count = await self.all_paper_to_authors()
        return paper_succ_count + author_succ_count

    async def bfs_once(self):
        paper_succ_count = await self.all_author_to_papers()
        author_succ_count = await self.all_paper_to_authors()
        venue_succ_count = await self.all_paper_to_venues()
        return paper_succ_count + author_succ_count + venue_succ_count
