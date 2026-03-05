from .dataclass import DataSrc, DataDst
from .iface import SimpleWeaver
from .iface_init import WeaverInitializerIface, PapersWeaverInitializerIface
from .iface_p2v import Paper2VenuesWeaverCacheIface, Paper2VenuesWeaverIface


class PaperOnlyWeaver(SimpleWeaver, Paper2VenuesWeaverIface):

    def __init__(
        self,
        src: DataSrc,
        dst: DataDst,
        cache: Paper2VenuesWeaverCacheIface,
        initializer: WeaverInitializerIface,
    ):
        if not isinstance(initializer, PapersWeaverInitializerIface):
            raise TypeError("PaperOnlyWeaver requires PapersWeaverInitializerIface")
        super().__init__(src=src, dst=dst, cache=cache, initializer=initializer)

    async def bfs_once(self) -> int:
        raise NotImplementedError(
            "PaperOnlyWeaver only supports init(); bfs_once() is intentionally disabled"
        )

    async def bfs(self, max_iterations: int = 10) -> int:
        total_new = await self.init()
        self.logger.info(f"PaperOnlyWeaver completed init-only run with {total_new} new entities fetched.")
        return total_new
