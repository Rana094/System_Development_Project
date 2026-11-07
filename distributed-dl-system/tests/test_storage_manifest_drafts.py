"""Validate draft schemas/examples without importing application or DB modules."""

import ast
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys
import unittest

try:
    from jsonschema import Draft202012Validator, ValidationError
except ImportError:
    Draft202012Validator = None
    ValidationError = None

ROOT = Path(__file__).resolve().parents[1]
DRAFTS = ROOT / "docs/storage"
SCHEMAS = {
    name: json.loads((DRAFTS / "schemas" / filename).read_text())
    for name, filename in (
        ("tree", "input-tree-v1.schema.json"),
        ("job", "job-inputs-v1.schema.json"),
    )
}
FIXTURE = json.loads((DRAFTS / "examples/v1-manifests.json").read_text())


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=True, allow_nan=False) + "\n").encode()


class FixtureTests(unittest.TestCase):
    def test_canonical_fixture_digests_and_artifact_bytes(self):
        payloads = {p["sha256"]: p["content_utf8"].encode() for p in FIXTURE["payloads"]}
        for name in ("source_tree", "dataset_tree", "job_inputs"):
            self.assertEqual(hashlib.sha256(canonical(FIXTURE[name])).hexdigest(), FIXTURE["manifest_sha256"][name])
        for tree in (FIXTURE["source_tree"], FIXTURE["dataset_tree"]):
            for entry in tree["files"]:
                data = payloads[entry["sha256"]]
                self.assertEqual(hashlib.sha256(data).hexdigest(), entry["sha256"])
                self.assertEqual(len(data), entry["size_bytes"])

    def test_fixture_references_and_all_rank_requirements_are_consistent(self):
        job = FIXTURE["job_inputs"]
        self.assertEqual(job["source_manifest_sha256"], FIXTURE["manifest_sha256"]["source_tree"])
        self.assertEqual(job["dataset_manifest_sha256"], FIXTURE["manifest_sha256"]["dataset_tree"])
        self.assertEqual([r["rank"] for r in job["rank_inputs"]], list(range(job["execution"]["world_size"])))
        for rank in job["rank_inputs"]:
            self.assertEqual(rank["source_paths"], [e["path"] for e in FIXTURE["source_tree"]["files"]])
            self.assertEqual(rank["data_paths"], [e["path"] for e in FIXTURE["dataset_tree"]["files"]])

    def test_schemas_need_no_external_ref_resolution(self):
        def inspect(value):
            if isinstance(value, dict):
                for key, item in value.items():
                    if key == "$ref":
                        self.assertTrue(item.startswith("#/$defs/"), item)
                    inspect(item)
            elif isinstance(value, list):
                for item in value:
                    inspect(item)
        for schema in SCHEMAS.values():
            inspect(schema)

    def test_path_definitions_agree_and_no_live_db_modules_were_imported(self):
        self.assertEqual(SCHEMAS["tree"]["$defs"]["path"], SCHEMAS["job"]["$defs"]["path"])
        self.assertNotIn("job_store", sys.modules)
        self.assertNotIn("job_storage", sys.modules)

    def test_preserved_state_groups_and_attempt_token_from_source_only(self):
        tree = ast.parse((ROOT / "controller/queue_policy.py").read_text())
        groups = {}
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id in ("ACTIVE_STATES", "PENDING_STATES"):
                        groups[target.id] = ast.literal_eval(node.value)
        self.assertEqual(groups["ACTIVE_STATES"], ("SCHEDULED", "RUNNING", "CANCELLING"))
        self.assertEqual(groups["PENDING_STATES"], ("RECEIVED", "STAGED", "READY", "QUEUED", "HELD"))
        # Execute only this pure function AST; never import distributed_dispatch.
        source = ast.parse((ROOT / "controller/distributed_dispatch.py").read_text())
        function = next(n for n in source.body if isinstance(n, ast.FunctionDef) and n.name == "attempt")
        isolated = ast.Module(body=[function], type_ignores=[])
        scope = {}
        exec(compile(isolated, "<existing-attempt-function>", "exec"), scope)
        self.assertEqual(scope["attempt"]({"job_id": "draft-job", "retry_count": 0}), "draft-job:0")
        self.assertEqual(scope["attempt"]({"job_id": "draft-job", "retry_count": 2}), "draft-job:2")


@unittest.skipUnless(Draft202012Validator, "Optional draft-test dependency jsonschema unavailable; use the documented system Python")
class SchemaTests(unittest.TestCase):
    def validate(self, name, document):
        Draft202012Validator(SCHEMAS[name]).validate(document)

    def invalid(self, name, document):
        with self.assertRaises(ValidationError):
            self.validate(name, document)

    def test_schemas_are_valid_draft_2020_12(self):
        for schema in SCHEMAS.values():
            Draft202012Validator.check_schema(schema)

    def test_supplied_three_rank_fixture_has_valid_shape(self):
        self.validate("tree", FIXTURE["source_tree"])
        self.validate("tree", FIXTURE["dataset_tree"])
        self.validate("job", FIXTURE["job_inputs"])

    def test_single_rank_and_no_uploaded_dataset_shape(self):
        job = deepcopy(FIXTURE["job_inputs"])
        job["execution"] = {"mode": "single", "world_size": 1}
        job["dataset_manifest_sha256"] = None
        job["rank_inputs"] = [job["rank_inputs"][0]]
        job["rank_inputs"][0]["data_paths"] = []
        self.validate("job", job)

    def test_unknown_versions_kinds_and_fields_are_rejected(self):
        for name, document in (("tree", FIXTURE["source_tree"]), ("job", FIXTURE["job_inputs"])):
            for field, value in (("schema_version", 2), ("kind", "unknown"), ("arbitrary_path", "/etc/passwd")):
                with self.subTest(schema=name, field=field):
                    changed = deepcopy(document)
                    changed[field] = value
                    self.invalid(name, changed)

    def test_unsafe_paths_are_rejected_in_trees_and_rank_declarations(self):
        for path in ("", ".", "..", "../x", "/x", "a/../x", "a//x", "a/./x", "./x",
                     "a/", "a\\x", "C:x", "a\0x", "a\nx", "a\tx", "a\x7fx", "a\ud800x"):
            with self.subTest(path=repr(path)):
                tree = deepcopy(FIXTURE["source_tree"])
                tree["files"][0]["path"] = path
                self.invalid("tree", tree)
                job = deepcopy(FIXTURE["job_inputs"])
                job["rank_inputs"][0]["source_paths"] = [path]
                self.invalid("job", job)

    def test_invalid_digests_sizes_and_executable_types_are_rejected(self):
        for field, values in (("sha256", ("a" * 63, "a" * 65, "A" * 64, "a" * 64 + "\n")),
                              ("size_bytes", (-1, True, "12", 1.5)), ("executable", (0, "false"))):
            for value in values:
                with self.subTest(field=field, value=value):
                    tree = deepcopy(FIXTURE["source_tree"])
                    tree["files"][0][field] = value
                    self.invalid("tree", tree)

    def test_existing_job_and_owner_syntax_is_preserved(self):
        for field, values in (("job_id", ("bad:id", "bad\n", "", "x" * 129)),
                              ("owner", ("/etc", "bad\n", "", "x" * 129))):
            for value in values:
                with self.subTest(field=field, value=value):
                    job = deepcopy(FIXTURE["job_inputs"])
                    job[field] = value
                    self.invalid("job", job)

    def test_invalid_execution_shape_and_rank_types_are_rejected(self):
        for mode, count in (("single", 2), ("ddp", 1), ("ddp", 257), ("single", True), ("other", 1)):
            with self.subTest(mode=mode, world_size=count):
                job = deepcopy(FIXTURE["job_inputs"])
                job["execution"] = {"mode": mode, "world_size": count}
                self.invalid("job", job)
        for rank in (-1, True, 256, "0"):
            job = deepcopy(FIXTURE["job_inputs"])
            job["rank_inputs"][0]["rank"] = rank
            self.invalid("job", job)

    def test_duplicate_directory_and_rank_path_entries_are_rejected(self):
        tree = deepcopy(FIXTURE["source_tree"])
        tree["directories"] = ["empty", "empty"]
        self.invalid("tree", tree)
        job = deepcopy(FIXTURE["job_inputs"])
        job["rank_inputs"][0]["data_paths"] *= 2
        self.invalid("job", job)

    def test_shape_validation_does_not_claim_cross_manifest_semantic_checks(self):
        # This deliberate limitation must stay visible: registration needs more
        # than JSON Schema before these documents can authorize real staging.
        job = deepcopy(FIXTURE["job_inputs"])
        job["rank_inputs"][1]["rank"] = 0
        job["rank_inputs"][0]["data_paths"] = ["not-in-the-dataset.csv"]
        self.validate("job", job)
        self.assertNotEqual([r["rank"] for r in job["rank_inputs"]], list(range(3)))
        self.assertNotIn("not-in-the-dataset.csv", [f["path"] for f in FIXTURE["dataset_tree"]["files"]])


if __name__ == "__main__":
    unittest.main(verbosity=2)
