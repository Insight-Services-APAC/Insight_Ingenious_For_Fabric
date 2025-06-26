import pathlib
import sys
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from ingen_fab.fabric_cicd.promotion_utils import promotion_utils


def test_promote_calls_publish_only():
    promoter = promotion_utils("ws1", "repo")
    with (
        mock.patch(
            "ingen_fab.python_libs.python.promotion_utils.FabricWorkspace"
        ) as fw_mock,
        mock.patch(
            "ingen_fab.python_libs.python.promotion_utils.publish_all_items"
        ) as pub_mock,
        mock.patch(
            "ingen_fab.python_libs.python.promotion_utils.unpublish_all_orphan_items"
        ) as unpub_mock,
    ):
        fw_mock.return_value = object()
        promoter.promote(delete_orphans=False)
        pub_mock.assert_called_once()
        unpub_mock.assert_not_called()


def test_promote_with_unpublish():
    promoter = promotion_utils("ws1", "repo")
    with (
        mock.patch(
            "ingen_fab.python_libs.python.promotion_utils.FabricWorkspace"
        ) as fw_mock,
        mock.patch(
            "ingen_fab.python_libs.python.promotion_utils.publish_all_items"
        ) as pub_mock,
        mock.patch(
            "ingen_fab.python_libs.python.promotion_utils.unpublish_all_orphan_items"
        ) as unpub_mock,
    ):
        fw_mock.return_value = object()
        promoter.promote(delete_orphans=True)
        pub_mock.assert_called_once()
        unpub_mock.assert_called_once()
