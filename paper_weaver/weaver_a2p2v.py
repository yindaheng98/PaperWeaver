from .dataclass import DataSrc, DataDst
from .iface import SimpleWeaver
from .iface_init import WeaverInitializerIface, AuthorsWeaverInitializerIface
from .iface_a2p import Author2PapersWeaverCacheIface, Author2PapersWeaverIface
from .iface_p2a import Paper2AuthorsWeaverCacheIface, Paper2AuthorsWeaverIface
from .iface_p2v import Paper2VenuesWeaverCacheIface, Paper2VenuesWeaverIface


class Author2Paper2VenueCache(Author2PapersWeaverCacheIface, Paper2AuthorsWeaverCacheIface, Paper2VenuesWeaverCacheIface):
    pass


class Author2Paper2VenueWeaver(SimpleWeaver, Author2PapersWeaverIface, Paper2AuthorsWeaverIface, Paper2VenuesWeaverIface):

    def __init__(
        self,
        src: DataSrc,
        dst: DataDst,
        cache: Author2Paper2VenueCache,
        initializer: WeaverInitializerIface
    ):
        if not isinstance(initializer, AuthorsWeaverInitializerIface):
            raise TypeError("Author2Paper2VenueWeaver requires AuthorsWeaverInitializerIface")
        super().__init__(src=src, dst=dst, cache=cache, initializer=initializer)

    @property
    def initializer(self) -> AuthorsWeaverInitializerIface:
        return self._initializer  # type: ignore

    async def bfs_once(self):
        paper_succ_count = await self.all_author_to_papers()
        author_succ_count = await self.all_paper_to_authors()
        venue_succ_count = await self.all_paper_to_venues()
        return paper_succ_count + author_succ_count + venue_succ_count
