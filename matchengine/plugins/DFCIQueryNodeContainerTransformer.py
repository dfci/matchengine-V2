from __future__ import annotations

from typing import TYPE_CHECKING

from matchengine.internals.plugin_helpers.plugin_stub import QueryNodeContainerTransformer

if TYPE_CHECKING:
    from matchengine.internals.typing.matchengine_types import QueryNodeContainer


class DFCIQueryContainerTransformer(QueryNodeContainerTransformer):
    def query_node_container_transform(self, query_container: QueryNodeContainer):
        pass


__export__ = ["DFCIQueryContainerTransformer"]
