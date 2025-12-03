from .dataclass import DataSrc, DataDst
from .iface import SimpleWeaver
from .iface_a2p import Author2PapersWeaverCacheIface
from .iface_p2a import Paper2AuthorsWeaverCacheIface
from .iface_p2v import Paper2VenuesWeaverCacheIface
from .weaver_impl import Author2PaperWeaver, Paper2AuthorWeaver, Paper2VenueWeaver


class Author2Paper2VenueCache(Author2PapersWeaverCacheIface, Paper2AuthorsWeaverCacheIface, Paper2VenuesWeaverCacheIface):
    pass


class Author2Paper2VenueWeaver(Author2PaperWeaver, Paper2AuthorWeaver, Paper2VenueWeaver, SimpleWeaver):

    def __init__(self, src: DataSrc, dst: DataDst, cache: Author2Paper2VenueCache):
        super().__init__(src=src, dst=dst, cache=cache)

    async def bfs_once(self):
        paper_succ_count = await self.all_author_to_papers()
        author_succ_count = await self.all_paper_to_authors()
        venue_succ_count = await self.all_paper_to_venues()
        return paper_succ_count + author_succ_count + venue_succ_count
