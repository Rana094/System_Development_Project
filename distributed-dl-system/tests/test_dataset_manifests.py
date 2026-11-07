"""Dataset identity/codec/current-ACL checks; no controller imports or live DB."""

from dataclasses import FrozenInstanceError
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from hybrid_storage.artifacts import ArtifactRegistrar
from hybrid_storage.dataset_manifests import (
    DatasetACL, DatasetEntry, DatasetManifest, checked_entries, create_dataset, next_version, parse_manifest,
)

IDENTITY = "ds_" + "1" * 32
DATA = b"dataset bytes\n"
SHA = hashlib.sha256(DATA).hexdigest()


class ManifestTests(unittest.TestCase):
    def entries(self):
        return [DatasetEntry("data", "directory"), DatasetEntry("data/input.csv", "file", SHA, len(DATA))]

    def manifest(self, **changes):
        return DatasetManifest(**dict(dict(dataset_id=IDENTITY, version=1, owner="alice",
                                           entries=self.entries()), **changes))

    def encoded(self, document):
        return (json.dumps(document, sort_keys=True, separators=(",", ":"),
                           ensure_ascii=True, allow_nan=False) + "\n").encode()

    def test_canonical_roundtrip_has_real_content_hash_and_explicit_types(self):
        manifest = self.manifest()
        self.assertEqual(parse_manifest(manifest.canonical_bytes()), manifest)
        self.assertEqual(manifest.sha256, hashlib.sha256(manifest.canonical_bytes()).hexdigest())
        self.assertEqual(manifest.document()["entries"][1], {
            "path": "data/input.csv", "type": "file", "sha256": SHA, "size_bytes": len(DATA)})

    def test_entries_and_acl_are_immutable_defensive_snapshots(self):
        entries = self.entries()
        readers = ["bob"]
        manifest = self.manifest(entries=entries)
        acl = DatasetACL(IDENTITY, "alice", readers)
        before = manifest.canonical_bytes()
        entries.clear()
        readers.clear()
        self.assertEqual(manifest.canonical_bytes(), before)
        self.assertEqual(acl.readers, ("bob",))
        for obj, name, value in ((manifest, "version", 2), (manifest.entries[1], "path", "other"),
                                  (acl, "owner", "bob")):
            with self.assertRaises(FrozenInstanceError): setattr(obj, name, value)
        document = manifest.document()
        document["entries"].clear()
        self.assertEqual(manifest.canonical_bytes(), before)

    def test_sorting_has_one_canonical_manifest_independent_of_inventory_order(self):
        self.assertEqual(self.manifest().canonical_bytes(), self.manifest(entries=reversed(self.entries())).canonical_bytes())

    def test_next_version_preserves_owner_dataset_and_prior_bytes(self):
        manifest = self.manifest()
        before = manifest.canonical_bytes()
        changed = next_version(manifest, DatasetACL(IDENTITY, "alice"), "alice", [])
        self.assertEqual((changed.dataset_id, changed.owner, changed.version), (IDENTITY, "alice", 2))
        self.assertEqual(changed.previous_manifest_sha256, manifest.sha256)
        self.assertNotEqual(changed.sha256, manifest.sha256)
        self.assertEqual(manifest.canonical_bytes(), before)
        self.assertEqual(parse_manifest(changed.canonical_bytes()), changed)

    def test_creation_uses_server_identity_and_private_acl_by_default(self):
        first, acl = create_dataset("alice", [])
        second, _ = create_dataset("alice", [])
        self.assertRegex(first.dataset_id, r"^ds_[0-9a-f]{32}$")
        self.assertNotEqual(first.dataset_id, second.dataset_id)
        self.assertEqual((first.version, first.owner, acl.readers), (1, "alice", ()))
        acl.require_read("alice", first)
        with self.assertRaises(PermissionError): acl.require_read("bob", first)

    def test_reader_can_read_but_cannot_publish_or_change_acl(self):
        manifest = self.manifest()
        acl = DatasetACL(IDENTITY, "alice", ["bob"])
        acl.require_read("bob", manifest)
        with self.assertRaises(PermissionError): next_version(manifest, acl, "bob", [])
        with self.assertRaises(PermissionError): acl.replace_readers("bob", ["eve"])
        with self.assertRaises(PermissionError): acl.require_read("Bob", manifest)

    def test_current_acl_revocation_applies_to_prior_and_new_versions(self):
        manifest = self.manifest()
        shared = DatasetACL(IDENTITY, "alice", ["bob"])
        later = next_version(manifest, shared, "alice", [])
        revoked = shared.replace_readers("alice", [])
        for version in (manifest, later):
            shared.require_read("bob", version)
            with self.assertRaises(PermissionError): revoked.require_read("bob", version)
        self.assertEqual(manifest.document()["owner"], "alice")
        self.assertNotIn("readers", manifest.document())

    def test_acl_must_bind_both_dataset_id_and_immutable_owner(self):
        acl = DatasetACL(IDENTITY, "alice", ["bob"])
        for manifest in (self.manifest(owner="bob"), self.manifest(dataset_id="ds_" + "2" * 32)):
            with self.assertRaises(PermissionError): acl.require_read("bob", manifest)
            with self.assertRaises(PermissionError): acl.require_write("alice", manifest)

    def test_public_access_checks_require_a_manifest_not_just_an_acl(self):
        acl = DatasetACL(IDENTITY, "alice", ["bob"])
        for manifest in (None, {}, "known-digest", SHA):
            with self.subTest(manifest=manifest), self.assertRaises(PermissionError):
                acl.require_read("bob", manifest)
            with self.subTest(manifest=manifest), self.assertRaises(PermissionError):
                acl.require_write("alice", manifest)

    def test_changed_registered_bytes_produce_a_new_version_and_keep_the_old_identity(self):
        with tempfile.TemporaryDirectory(prefix="dataset-version-") as directory:
            root, scratch = Path(directory) / "objects", Path(directory) / "private"
            root.mkdir(mode=0o755)
            scratch.mkdir(mode=0o700)
            registrar = ArtifactRegistrar(root, staging_root=scratch)
            original = registrar.register(io.BytesIO(DATA), expected_size=len(DATA))
            first, acl = create_dataset("alice", [DatasetEntry("input.csv", "file", original.sha256, original.size_bytes)])
            changed = DATA + b"new row\n"
            artifact = registrar.register(io.BytesIO(changed), expected_size=len(changed))
            second = next_version(first, acl, "alice", [DatasetEntry("input.csv", "file", artifact.sha256, artifact.size_bytes)])
            self.assertEqual(second.version, 2)
            self.assertEqual(first.entries[0].sha256, original.sha256)
            self.assertEqual(second.entries[0].sha256, artifact.sha256)
            self.assertNotEqual(second.entries[0].sha256, first.entries[0].sha256)
            self.assertNotEqual(second.sha256, first.sha256)
            self.assertEqual((root / original.key).read_bytes(), DATA)
            self.assertEqual((root / artifact.key).read_bytes(), changed)

    def test_same_physical_object_does_not_share_authorization(self):
        with tempfile.TemporaryDirectory(prefix="dataset-manifest-dedup-") as directory:
            root = Path(directory) / "objects"
            scratch = Path(directory) / "private"
            root.mkdir(mode=0o755)
            scratch.mkdir(mode=0o700)
            registrar = ArtifactRegistrar(root, staging_root=scratch)
            first = registrar.register(io.BytesIO(DATA), expected_size=len(DATA))
            second = registrar.register(io.BytesIO(DATA), expected_size=len(DATA))
            self.assertTrue(first.created)
            self.assertFalse(second.created)
            alice, alice_acl = create_dataset("alice", [DatasetEntry("input", "file", first.sha256, first.size_bytes)])
            bob, bob_acl = create_dataset("bob", [DatasetEntry("input", "file", second.sha256, second.size_bytes)])
            alice_acl.require_read("alice", alice)
            bob_acl.require_read("bob", bob)
            with self.assertRaises(PermissionError): alice_acl.require_read("bob", alice)
            with self.assertRaises(PermissionError): bob_acl.require_read("alice", bob)
            self.assertEqual(len(list(root.glob("sha256/*/*"))), 1)

    def test_owner_and_reader_syntax_rejects_wildcards_duplicates_and_coercion(self):
        for owner in ("", "*", "../alice", "alice\n", "x" * 129, None):
            with self.subTest(owner=owner), self.assertRaises(ValueError): self.manifest(owner=owner)
        for readers in (["*"], ["bob", "bob"], ["alice"], ["bad/id"], "bob", b"bob", [True]):
            with self.subTest(readers=readers), self.assertRaises(ValueError): DatasetACL(IDENTITY, "alice", readers)

    def test_invalid_identity_version_or_predecessor_is_rejected(self):
        for options in ({"dataset_id": "../dataset"}, {"dataset_id": "ds_" + "A" * 32},
                        {"version": True}, {"version": 0}, {"version": 1.0}, {"version": 1 << 63},
                        {"previous_manifest_sha256": SHA}, {"version": 2},
                        {"version": 2, "previous_manifest_sha256": "A" * 64}):
            with self.subTest(options=options), self.assertRaises(ValueError): self.manifest(**options)

    def test_unknown_kind_schema_or_fields_fail_closed(self):
        for field, value in (("schema_version", True), ("schema_version", 2), ("kind", "source-tree"),
                             ("acl", ["eve"]), ("path", "/srv/objects")):
            document = self.manifest().document()
            document[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError): parse_manifest(self.encoded(document))
        document = self.manifest().document()
        del document["owner"]
        with self.assertRaises(ValueError): parse_manifest(self.encoded(document))

    def test_duplicate_json_keys_are_rejected_at_every_depth(self):
        raw = self.manifest().canonical_bytes()
        for before, after in ((b'"owner":"alice"', b'"owner":"eve","owner":"alice"'),
                              (b'"type":"file"', b'"type":"file","type":"file"')):
            with self.subTest(before=before), self.assertRaises(ValueError): parse_manifest(raw.replace(before, after))

    def test_noncanonical_whitespace_entry_order_and_json_spellings_rejected(self):
        manifest = self.manifest()
        document = manifest.document()
        document["entries"].reverse()
        for raw in (manifest.canonical_bytes().rstrip(), json.dumps(manifest.document()).encode(),
                    self.encoded(document), manifest.canonical_bytes().replace(b'"version":1', b'"version":1.0')):
            with self.subTest(raw=raw[:30]), self.assertRaises(ValueError): parse_manifest(raw)

    def test_bounded_decoder_rejects_malformed_utf8_deep_json_nonfinite(self):
        for raw in (b"\xff", b"not-json", b"[]", b"[" * 1200 + b"0" + b"]" * 1200,
                    self.manifest().canonical_bytes().replace(b'"version":1', b'"version":NaN')):
            with self.subTest(raw=raw[:15]), self.assertRaises(ValueError): parse_manifest(raw)
        with patch("hybrid_storage.dataset_manifests.MAX_MANIFEST_BYTES", 10):
            with self.assertRaises(ValueError): parse_manifest(b"x" * 11)

    def test_unsafe_paths_reject_traversal_aliases_and_controls(self):
        for path in ("", ".", "..", "../input", "/input", "a/../b", "./a", "a//b", "a/", "a\\b",
                     "C:input", "a\0b", "a\nb", "a\x7fb", "a\ud800", "a" * 1025, "/".join(["a"] * 65)):
            with self.subTest(path=repr(path)), self.assertRaises(ValueError): DatasetEntry(path, "file", SHA, len(DATA))

    def test_case_and_unicode_aliases_including_implicit_prefixes_are_rejected(self):
        for left, right in (("A", "a"), ("Data/x", "data/y"), ("é", "e\u0301"), ("ß/x", "ss/y")):
            entries = [DatasetEntry(left, "file", SHA, len(DATA)), DatasetEntry(right, "file", SHA, len(DATA))]
            with self.subTest(left=left), self.assertRaises(ValueError): checked_entries(entries, require_parents=False)

    def test_duplicate_paths_missing_parents_and_file_ancestors_rejected(self):
        file = DatasetEntry("data", "file", SHA, len(DATA))
        for entries in ([file, file], [file, DatasetEntry("data", "directory")],
                        [file, DatasetEntry("data/child", "file", SHA, len(DATA))], self.entries()[1:]):
            with self.subTest(entries=entries), self.assertRaises(ValueError): self.manifest(entries=entries)

    def test_regular_file_types_sizes_and_same_digest_consistency(self):
        for options in ({"type": "symlink"}, {"type": "fifo"}, {"size_bytes": True}, {"size_bytes": -1},
                        {"size_bytes": 1 << 63}, {"sha256": "A" * 64}, {"type": "directory"}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                DatasetEntry(**dict(dict(path="input", type="file", sha256=SHA, size_bytes=len(DATA)), **options))
        with self.assertRaises(ValueError): self.manifest(entries=[
            DatasetEntry("one", "file", SHA, 1), DatasetEntry("two", "file", SHA, 2)])

    def test_exact_entry_fields_and_array_type_are_required(self):
        for change in ({"type": "symlink"}, {"executable": False}, {"size_bytes": "14"}):
            document = self.manifest().document()
            document["entries"][1].update(change)
            with self.subTest(change=change), self.assertRaises(ValueError): parse_manifest(self.encoded(document))
        document = self.manifest().document()
        document["entries"] = {}
        with self.assertRaises(ValueError): parse_manifest(self.encoded(document))

    def test_entry_reader_and_serialized_manifest_limits_are_enforced(self):
        with patch("hybrid_storage.dataset_manifests.MAX_ENTRIES", 1):
            with self.assertRaises(ValueError): self.manifest()
        with patch("hybrid_storage.dataset_manifests.MAX_READERS", 1):
            with self.assertRaises(ValueError): DatasetACL(IDENTITY, "alice", ["bob", "eve"])
        with patch("hybrid_storage.dataset_manifests.MAX_MANIFEST_BYTES", 10):
            with self.assertRaises(ValueError): self.manifest()

    def test_empty_directories_and_literal_percent_names_are_retained(self):
        entries = [DatasetEntry("empty", "directory"), DatasetEntry("literal%2finput", "file", SHA, len(DATA))]
        self.assertEqual(parse_manifest(self.manifest(entries=entries).canonical_bytes()).entries, tuple(entries))

    def test_schema_fixture_is_canonical_and_runtime_valid(self):
        fixture = ROOT / "docs/storage/examples/dataset-manifest-v1.json"
        raw = fixture.read_bytes()
        manifest = parse_manifest(raw)
        self.assertEqual(manifest.owner, "example-student")
        self.assertEqual(manifest.entries[-1].sha256, hashlib.sha256(b"example dataset\n").hexdigest())
        schema = json.loads((ROOT / "docs/storage/schemas/dataset-manifest-v1.schema.json").read_text())
        self.assertFalse(schema["additionalProperties"])
        self.assertEqual(set(schema["required"]), set(manifest.document()))

    def test_import_is_stdlib_only_and_has_no_controller_config_db_side_effects(self):
        script = (f"import sys; sys.path.insert(0,{str(ROOT)!r}); "
                  "import hybrid_storage.dataset_manifests, hybrid_storage.dataset_validation; "
                  "assert 'sqlite3' not in sys.modules; assert 'job_store' not in sys.modules; "
                  "assert 'hybrid_storage.config' not in sys.modules")
        result = subprocess.run([sys.executable, "-B", "-c", script], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
