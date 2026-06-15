import main
import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# main imports xlsxwriter indirectly through taxman at module import time.
# Provide a minimal stub so orchestration tests can run without optional deps.
if "xlsxwriter" not in sys.modules:
    class _WorkbookStub:
        def __init__(self, *args, **kwargs):
            pass

    class _FormatStub:
        pass

    xlsxwriter_stub = types.SimpleNamespace(
        Workbook=_WorkbookStub,
        format=types.SimpleNamespace(Format=_FormatStub),
    )
    sys.modules["xlsxwriter"] = xlsxwriter_stub

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class MainExportTargetsTests(unittest.TestCase):
    @patch.object(main, "EXPORT_STEUERTIPPS_CSV", True)
    @patch.object(main, "EXPORT_WISO_CSV", True)
    @patch("main.patch_databases")
    @patch("main.log_config.shutdown")
    @patch("main._move_log_file")
    @patch("builtins.print")
    @patch("main.Taxman")
    @patch("main.Book")
    @patch("main.PriceData")
    def test_main_exports_both_cointracking_targets_when_enabled(
        self,
        price_data_cls: MagicMock,
        book_cls: MagicMock,
        taxman_cls: MagicMock,
        print_mock: MagicMock,
        _move_log_file_mock: MagicMock,
        _shutdown_mock: MagicMock,
        _patch_databases_mock: MagicMock,
    ) -> None:
        price_data = MagicMock()
        price_data_cls.return_value = price_data

        book = MagicMock()
        book.read_files.return_value = True
        book_cls.return_value = book

        taxman = MagicMock()
        evaluation_path = Path("export/2025_rev999.xlsx")
        steuertipps_path = Path("export/2025_rev999_steuertipps.csv")
        wiso_path = Path("export/2025_rev999_wiso.csv")
        taxman.export_evaluation_as_excel.return_value = evaluation_path
        taxman.export_evaluation_as_steuertipps_csv.return_value = steuertipps_path
        taxman.export_evaluation_as_wiso_csv.return_value = wiso_path
        taxman_cls.return_value = taxman

        main.main()

        taxman.export_evaluation_as_excel.assert_called_once_with()
        taxman.export_evaluation_as_steuertipps_csv.assert_called_once_with(
            evaluation_path
        )
        taxman.export_evaluation_as_wiso_csv.assert_called_once_with(evaluation_path)

        printed_lines = [
            call.args[0] for call in print_mock.call_args_list if call.args
        ]
        self.assertTrue(
            any("CoinTracking CSV for SteuerSparErklaerung saved at" in line for line in printed_lines)
        )
        self.assertTrue(
            any("CoinTracking CSV for WISO saved at" in line for line in printed_lines)
        )


if __name__ == "__main__":
    unittest.main()
