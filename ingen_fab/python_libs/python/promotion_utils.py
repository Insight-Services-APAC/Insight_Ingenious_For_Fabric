from pathlib import Path
from typing import Iterable, Optional, List

from fabric_cicd import (
    FabricWorkspace,
    publish_all_items,
    unpublish_all_orphan_items,
    constants,
)


class promotion_utils:
    """Utility class for promoting Fabric items between workspaces."""

    def __init__(
        self,
        workspace_id: str,
        repository_directory: str | str = "fabric_workspace_items",
        *,
        item_type_in_scope: Optional[Iterable[str]] = None,
        environment: str = "N/A",
    ) -> None:
        self.workspace_id = workspace_id
        self.repository_directory = repository_directory
        self.environment = environment
        if item_type_in_scope is None:
            self.item_type_in_scope = list(constants.ACCEPTED_ITEM_TYPES_UPN)
        else:
            self.item_type_in_scope = list(item_type_in_scope)

    def _workspace(self) -> FabricWorkspace:
        """Create a FabricWorkspace instance for the configured workspace."""
        return FabricWorkspace(
            workspace_id=self.workspace_id,
            repository_directory=str(self.repository_directory),
            item_type_in_scope=self.item_type_in_scope,
            environment=self.environment,
        )

    def publish_all(self) -> None:
        """Publish all items from the repository to the workspace."""
        ws = self._workspace()
        publish_all_items(fabric_workspace_obj=ws)

    def unpublish_orphans(self) -> None:
        """Remove items from the workspace that are not present in the repository."""
        ws = self._workspace()
        unpublish_all_orphan_items(fabric_workspace_obj=ws)

    def promote(self, *, delete_orphans: bool = False) -> None:
        """Publish repository items and optionally unpublish orphans."""
        self.publish_all()
        if delete_orphans:
            self.unpublish_orphans()
