
# TODO: 调用 https://github.com/lukasschwab/arxiv.py 获取 Arxiv 论文列表
# TODO: ArXiv没有准确的作者列表, 所以不获取作者
# TODO: 按照 https://info.arxiv.org/help/doi.html 规则构造DOI
# TODO: ArXiv的category作为paper_weaver.dataclass.Venue
# TODO: ArXiv的paper_info也是从搜索页面获取
# TODO: 每次获取一个lukasschwab/arxiv.py的论文列表就将所有论文分别构造成页面存入缓存以备之后使用, 避免重复查询, 缓存时间设置为永久
# TODO: ArXiv只有一个搜索页面, 所以只需要实现Paper和Venue之间的相关操作