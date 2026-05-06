import ast
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class Top100V14ContextAdapterTest(unittest.TestCase):
    def test_adapter_reexports_v1_4_context_surface(self):
        import microcap_top100_mom16_biweekly_live_v1_4 as v14
        import top100_v14_base_context as adapter

        self.assertIs(adapter.v1_1_mod, v14.v1_1_mod)
        self.assertEqual(adapter.BASE_HEDGE_RATIO, v14.BASE_HEDGE_RATIO)
        self.assertIs(adapter.current_base_fingerprint, v14.current_base_fingerprint)
        self.assertIs(adapter._load_base_v1_1_context, v14._load_base_v1_1_context)
        self.assertIs(adapter._load_realtime_v1_1_context, v14._load_realtime_v1_1_context)
        self.assertIs(adapter._load_reference_summary, v14._load_reference_summary)
        self.assertIs(adapter.build_realtime_v1_4_outputs, v14.build_realtime_v1_4_outputs)

    def test_overlay_versions_depend_on_adapter_not_v1_4_script_directly(self):
        for filename in [
            "microcap_top100_mom16_biweekly_live_v1_6.py",
            "microcap_top100_mom16_biweekly_live_v1_7.py",
            "microcap_top100_mom16_biweekly_live_v1_8.py",
        ]:
            tree = ast.parse((ROOT / filename).read_text(encoding="utf-8-sig"), filename=filename)
            imports = [
                node
                for node in ast.walk(tree)
                if isinstance(node, (ast.Import, ast.ImportFrom))
            ]
            direct_v14_imports = [
                node
                for node in imports
                if (
                    isinstance(node, ast.Import)
                    and any(alias.name == "microcap_top100_mom16_biweekly_live_v1_4" for alias in node.names)
                )
                or (
                    isinstance(node, ast.ImportFrom)
                    and node.module == "microcap_top100_mom16_biweekly_live_v1_4"
                )
            ]
            adapter_imports = [
                node
                for node in imports
                if (
                    isinstance(node, ast.Import)
                    and any(alias.name == "top100_v14_base_context" for alias in node.names)
                )
                or (
                    isinstance(node, ast.ImportFrom)
                    and node.module == "top100_v14_base_context"
                )
            ]

            self.assertEqual(direct_v14_imports, [], filename)
            self.assertNotEqual(adapter_imports, [], filename)


if __name__ == "__main__":
    unittest.main()
