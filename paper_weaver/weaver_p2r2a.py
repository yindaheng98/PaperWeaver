from .dataclass import DataSrc, DataDst
from .iface import SimpleWeaver
from .iface_init import WeaverInitializerIface, PapersWeaverInitializerIface
from .iface_p2r import Paper2ReferencesWeaverCacheIface, Paper2ReferencesWeaverIface
from .iface_p2a import Paper2AuthorsWeaverCacheIface, Paper2AuthorsWeaverIface


class Paper2Reference2AuthorCache(Paper2ReferencesWeaverCacheIface, Paper2AuthorsWeaverCacheIface):
    pass


class Paper2Reference2AuthorWeaver(SimpleWeaver, Paper2ReferencesWeaverIface, Paper2AuthorsWeaverIface):

    def __init__(
        self,
        src: DataSrc,
        dst: DataDst,
        cache: Paper2Reference2AuthorCache,
        initializer: WeaverInitializerIface
    ):
        if not isinstance(initializer, PapersWeaverInitializerIface):
            raise TypeError("Paper2Reference2AuthorWeaver requires PapersWeaverInitializerIface")
        super().__init__(src=src, dst=dst, cache=cache, initializer=initializer)

    async def init(self) -> int:
        paper_succ_count = await super().init()  # the init in PapersWeaverInitializerIface
        ref_count = await self.all_paper_to_references()
        return paper_succ_count + ref_count

    async def bfs_once(self):
        ref_count = await self.all_paper_to_references()
        author_count = await self.all_paper_to_authors()
        return ref_count + author_count
