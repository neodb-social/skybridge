"""LD signature (``RsaSignature2017``) creation, interop-checked against takahe.

The verifier half of these tests is transplanted from NeoDB's takahe fork
(``core/signatures.py`` ``LDSignature.verify_signature`` / ``normalized_hash``
and ``core/ld.py`` ``builtin_document_loader``) so ``create_ld_signature`` is
exercised against the code path NeoDB receivers actually run on relayed
activities, not against our own implementation of the same scheme. The only
adaptation is passing the document loader per call instead of via pyld's
process-global default.
"""

# ruff: noqa: B006, B904 -- mutable default / raise-without-from preserved
# verbatim from the transplanted takahe code.

from __future__ import annotations

import base64
import binascii
import urllib.parse as urllib_parse
from typing import cast

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from pyld import jsonld
from skybridge.crypto import create_ld_signature, generate_keypair, verify_ld_signature
from skybridge.ld_contexts import SCHEMAS
from skybridge.translate.neodb import AP_CONTEXT, PUBLIC

ACTOR = "https://bridge.test/users/alice.example.com"
KEY_ID = f"{ACTOR}#main-key"


def _activity() -> dict:
    """A realistic bridged Create, including the inline NeoDB extension terms."""
    return {
        "@context": AP_CONTEXT,
        "id": f"{ACTOR}/activities/at%3A%2F%2Fdid%3Aplc%3Aabc%2Fapp.popfeed.feed.item%2F1",
        "type": "Create",
        "actor": ACTOR,
        "published": "2026-07-14T00:00:00Z",
        "to": [PUBLIC],
        "object": {
            "id": f"{ACTOR}/objects/1",
            "type": "Note",
            "attributedTo": ACTOR,
            "to": [PUBLIC],
            "content": '<p>rated 8/10 <a href="https://example.com/movie/1">Movie</a></p>',
            "relatedWith": [
                {
                    "type": "Rating",
                    "value": 8,
                    "best": 10,
                    "worst": 1,
                    "withRegardTo": "https://example.com/movie/1",
                }
            ],
        },
    }


# --- transplanted takahe verifier (core/signatures.py + core/ld.py) ---------


class VerificationError(BaseException):
    pass


class VerificationFormatError(VerificationError):
    pass


def takahe_document_loader(url: str, options={}):
    pieces = urllib_parse.urlparse(url)
    if pieces.hostname is None:
        return SCHEMAS["unknown"]
    key = pieces.hostname + pieces.path.rstrip("/")
    try:
        return SCHEMAS[key]
    except KeyError:
        try:
            key = "*" + pieces.path.rstrip("/")
            return SCHEMAS[key]
        except KeyError:
            return SCHEMAS["unknown"]


def takahe_normalized_hash(document) -> bytes:
    norm_form = jsonld.normalize(
        document,
        {
            "algorithm": "URDNA2015",
            "format": "application/n-quads",
            "documentLoader": takahe_document_loader,
        },
    )
    digest = hashes.Hash(hashes.SHA256())
    digest.update(norm_form.encode("utf8"))
    return digest.finalize().hex().encode("ascii")


def takahe_verify_signature(document: dict, public_key: str) -> None:
    try:
        document = document.copy()
        signature = document.pop("signature")
        options = {
            "@context": "https://w3id.org/identity/v1",
            "creator": signature["creator"],
            "created": signature["created"],
        }
    except KeyError:
        raise VerificationFormatError("Invalid signature section")
    if signature["type"].lower() != "rsasignature2017":
        raise VerificationFormatError("Unknown signature type")
    final_hash = takahe_normalized_hash(options) + takahe_normalized_hash(document)
    public_key_instance: rsa.RSAPublicKey = cast(
        rsa.RSAPublicKey,
        serialization.load_pem_public_key(public_key.encode("ascii")),
    )
    try:
        public_key_instance.verify(
            base64.b64decode(signature["signatureValue"]),
            final_hash,
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
    except InvalidSignature:
        raise VerificationError("LDSignature mismatch")
    except binascii.Error:
        raise VerificationFormatError("Invalid base64 in signatureValue")


# --- tests -------------------------------------------------------------------


def test_takahe_verifies_skybridge_signature():
    private_pem, public_pem = generate_keypair()
    activity = _activity()
    activity["signature"] = create_ld_signature(activity, private_pem=private_pem, key_id=KEY_ID)

    assert activity["signature"]["type"] == "RsaSignature2017"
    # NeoDB's inbox additionally requires creator (defragged) == actor.
    assert activity["signature"]["creator"] == KEY_ID
    takahe_verify_signature(activity, public_pem)  # raises on mismatch


def test_own_roundtrip():
    private_pem, public_pem = generate_keypair()
    activity = _activity()
    activity["signature"] = create_ld_signature(activity, private_pem=private_pem, key_id=KEY_ID)
    assert verify_ld_signature(activity, public_pem=public_pem)


def test_tampered_content_fails():
    private_pem, public_pem = generate_keypair()
    activity = _activity()
    activity["signature"] = create_ld_signature(activity, private_pem=private_pem, key_id=KEY_ID)
    activity["object"]["content"] = "<p>tampered</p>"
    assert not verify_ld_signature(activity, public_pem=public_pem)
    with pytest.raises(VerificationError):
        takahe_verify_signature(activity, public_pem)


def test_creator_must_match_actor():
    private_pem, public_pem = generate_keypair()
    activity = _activity()
    activity["signature"] = create_ld_signature(
        activity,
        private_pem=private_pem,
        key_id="https://bridge.test/users/mallory#main-key",
    )
    # The math checks out, but NeoDB rejects relayed documents whose LD
    # creator is not the document actor; our verifier mirrors that.
    assert not verify_ld_signature(activity, public_pem=public_pem)


def test_wrong_key_fails():
    private_pem, _ = generate_keypair()
    _, other_public = generate_keypair()
    activity = _activity()
    activity["signature"] = create_ld_signature(activity, private_pem=private_pem, key_id=KEY_ID)
    assert not verify_ld_signature(activity, public_pem=other_public)


def test_signing_never_touches_the_network(monkeypatch):
    """Context URLs resolve from the vendored cache; unknown ones fall back to
    the empty context instead of pyld's default (network) loader."""

    def explode(url, options={}):
        raise AssertionError(f"network document loader hit for {url}")

    # pyld falls back to the module-global _default_document_loader when a
    # normalize call passes no documentLoader; make that path fatal.
    monkeypatch.setattr(jsonld, "_default_document_loader", explode)
    private_pem, public_pem = generate_keypair()
    activity = _activity()
    activity["@context"] = [
        "https://www.w3.org/ns/activitystreams",
        "https://unknown.example/ns",  # not in the vendored cache
    ]
    activity["signature"] = create_ld_signature(activity, private_pem=private_pem, key_id=KEY_ID)
    assert verify_ld_signature(activity, public_pem=public_pem)


def test_resigning_excludes_prior_signature_block():
    private_pem, public_pem = generate_keypair()
    activity = _activity()
    activity["signature"] = create_ld_signature(activity, private_pem=private_pem, key_id=KEY_ID)
    resigned = create_ld_signature(activity, private_pem=private_pem, key_id=KEY_ID)
    verified = {**activity, "signature": resigned}
    assert verify_ld_signature(verified, public_pem=public_pem)


def test_verify_never_raises_on_malformed_signature_blocks():
    _, public_pem = generate_keypair()
    activity = _activity()
    for bad in (
        None,
        "not-a-dict",
        {},
        {"type": 5, "creator": KEY_ID, "created": "x", "signatureValue": ""},
        {"type": "RsaSignature2017", "creator": KEY_ID, "created": "x", "signatureValue": "!!"},
        {"type": "RsaSignature2017", "creator": None, "created": "x", "signatureValue": ""},
    ):
        assert not verify_ld_signature({**activity, "signature": bad}, public_pem=public_pem)
    assert not verify_ld_signature(activity, public_pem=public_pem)  # no signature at all


def test_fanout_signs_relay_copy_only(monkeypatch):
    """The relay Task body carries the LD signature; the direct-path body and
    the caller's dict stay byte-identical to the pre-signing form."""
    import asyncio

    from skybridge.activitypub import delivery

    private_pem, public_pem = generate_keypair()
    monkeypatch.setattr(delivery, "relay_inboxes", lambda: ["https://relay.test/inbox"])
    monkeypatch.setattr(delivery, "follower_targets", lambda did: ["https://peer.test/inbox"])
    monkeypatch.setattr(delivery, "_author_key", lambda did: (private_pem, KEY_ID))

    activity = _activity()
    original = {**activity}
    worker = delivery.DeliveryWorker()  # never started: tasks stay queued

    count = asyncio.run(
        delivery.fanout(worker, record_uri="at://x", did="did:plc:abc", activity=activity)
    )
    assert count == 2
    assert activity == original, "fanout must not mutate the caller's activity"

    tasks = {}
    while not worker.queue.empty():
        task = worker.queue.get_nowait()
        tasks[task.target_inbox] = task
    relayed = tasks["https://relay.test/inbox"].activity
    direct = tasks["https://peer.test/inbox"].activity
    assert verify_ld_signature(relayed, public_pem=public_pem)
    assert "signature" not in direct
    assert direct == original


def test_fanout_skips_relay_path_when_signing_fails(monkeypatch):
    """An unsigned relay delivery is a guaranteed 401 at NeoDB receivers, so a
    signing failure must drop the relay tasks (direct path still delivers)."""
    import asyncio

    from skybridge.activitypub import delivery

    private_pem, _ = generate_keypair()
    monkeypatch.setattr(delivery, "relay_inboxes", lambda: ["https://relay.test/inbox"])
    monkeypatch.setattr(delivery, "follower_targets", lambda did: ["https://peer.test/inbox"])
    monkeypatch.setattr(delivery, "_author_key", lambda did: (private_pem, KEY_ID))

    def boom(*args, **kwargs):
        raise RuntimeError("normalization exploded")

    monkeypatch.setattr(delivery, "create_ld_signature", boom)

    worker = delivery.DeliveryWorker()
    count = asyncio.run(
        delivery.fanout(worker, record_uri="at://x", did="did:plc:abc", activity=_activity())
    )
    assert count == 1
    task = worker.queue.get_nowait()
    assert task.target_inbox == "https://peer.test/inbox"
    assert "signature" not in task.activity
