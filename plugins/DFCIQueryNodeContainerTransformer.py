from __future__ import annotations
from matchengine.plugin_helpers.plugin_stub import QueryNodeContainerTransformer
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from matchengine.typing.matchengine_types import QueryNodeContainer


class DFCIQueryContainerTransformer(QueryNodeContainerTransformer):
    def query_node_container_transform(self, query_container: QueryNodeContainer):
        pass


__export__ = ["DFCIQueryContainerTransformer"]
