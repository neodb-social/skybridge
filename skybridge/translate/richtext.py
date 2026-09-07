"""Render a foreign app's review body into safe HTML.

The review fields we bridge are declared as plain strings by their lexicons,
but in practice they carry HTML:

* BookHive's ``buzz.bookhive.book.review`` is written by a rich-text editor,
  so line breaks arrive as literal ``<br>``. Its own web UI turns them back
  into newlines at render time; nothing on the wire says the field is markup.
* popfeed's ``feed.review.text`` holds raw Letterboxd markup (``<i>``,
  ``<a href>``, ``<b>``) on records created by its Letterboxd importer, with
  ``facets`` left empty.

Escaping such a body wholesale (the obvious reading of "plain string") puts
``&lt;br&gt;`` on the wire and shows the tag names to every reader. Passing it
through unescaped would hand authors an XSS on our own post page, which embeds
the generated ``content`` with ``| safe``.

So every review body goes through the sanitizer below. Because the same field
holds prose *and* markup, each ``<`` is judged on its own: it opens markup
when it names a real HTML element, and is literal text otherwise. A bridged
review really does contain ``<insert witty joke ... here>``, which a blanket
"strip what isn't allowlisted" rule deletes outright, and another really does
open with ``<div>``, which no reader wants to see spelled out.

Note the ambiguity this leaves: an author who writes about the ``<b>`` tag in
a plain-text review has it read as bold, because ``b`` *is* an element. That
is inherent to a field which mixes plain text and editor HTML with nothing to
tell them apart, and erring toward markup is what the app that wrote the
record does too.

The scanner that splits markup from text is ours, not a parser, and that is
safe for one reason worth stating plainly: it only decides whether a run is
dropped by the sanitizer or shown as literal text. :data:`_CLEANER` runs over
the result either way, so a mistake in the scanner is cosmetic and never
exploitable.
"""

from __future__ import annotations

import re
from collections.abc import Iterator

import nh3

# Roughly Mastodon's inbound allowlist, minus what a review body has no use
# for: no headings (a review is not a document), no tables, no images or media
# (posters ride on the catalog-item tag, see neodb._work_tag), no ``span``
# (only ever carries the class-based decoration we strip anyway).
_TAGS = frozenset(
    {
        "p",
        "br",
        "a",
        "em",
        "strong",
        "i",
        "b",
        "u",
        "s",
        "del",
        "blockquote",
        "ul",
        "ol",
        "li",
        "code",
        "pre",
    }
)

# ``href`` on a link is the only attribute worth keeping; everything else
# (``style``, ``class``, ``id``, every ``on*`` handler) is dropped.
_ATTRIBUTES = {"a": {"href"}}

_CLEANER = nh3.Cleaner(
    tags=_TAGS,
    attributes=_ATTRIBUTES,
    # Same rule as render_facets: only http(s) may become a live link, or a
    # javascript: URI would execute wherever the content HTML is embedded.
    # ``url_relative="deny"`` covers the schemeless case (``/foo``), which a
    # scheme allowlist alone lets through.
    url_schemes=frozenset({"http", "https"}),
    url_relative="deny",
    link_rel="nofollow noopener",
    # Drop the *content* of these, not just the tags: stripping only the tags
    # would leave the script body behind as visible text.
    clean_content_tags=frozenset({"script", "style"}),
    strip_comments=True,
)

# Element names that make a ``<`` markup rather than text. Everything the
# sanitizer keeps, plus the elements it throws away — an editor emits ``div``
# and ``h2``, and an attacker tries ``script`` and ``iframe``; both are markup
# and belong to the sanitizer, not on the page as literal text. This set is
# deliberately not the whole HTML spec: a name missing from it renders as
# text, which is safe, only ugly.
_HTML_ELEMENTS = nh3.ALLOWED_TAGS | {
    "script",
    "style",
    "iframe",
    "object",
    "embed",
    "form",
    "input",
    "button",
    "select",
    "textarea",
    "video",
    "audio",
    "svg",
    "math",
    "noscript",
    "template",
    "marquee",
}

_TAG_NAME = re.compile(r"</?([a-zA-Z][a-zA-Z0-9]*)")

_PARAGRAPHS = re.compile(r"\n{2,}")

# The sanitizer re-parses the fragment as HTML5, so a block element nested in
# one of our paragraphs closes it and leaves a stray empty paragraph behind
# (``<p>a<blockquote>b</blockquote></p>`` -> ``...<blockquote>b</blockquote><p></p>``).
_EMPTY_PARAGRAPH = re.compile(r"<p>\s*</p>")


def _runs(text: str) -> Iterator[tuple[bool, str]]:
    """Split *text* into ``(is_markup, chunk)`` runs.

    Markup runs are handed to the sanitizer untouched — crucially including
    any newline *inside* a tag, which breaks the tag if a line break is
    substituted into it. Text runs are where line breaks belong.
    """
    i = 0
    while True:
        lt = text.find("<", i)
        if lt < 0:
            yield False, text[i:]
            return
        if text.startswith("<!--", lt):
            close = text.find("-->", lt)
            if close >= 0:
                yield False, text[i:lt]
                yield True, text[lt : close + 3]  # the sanitizer strips comments
                i = close + 3
                continue
        name = _TAG_NAME.match(text, lt)
        if name is None or name.group(1).lower() not in _HTML_ELEMENTS:
            # Prose, not a tag: ``<3``, ``<https://...>``, ``<insert joke>``.
            # Only the ``<`` needs escaping; the sanitizer escapes the rest.
            yield False, text[i:lt] + "&lt;"
            i = lt + 1
            continue
        end = text.find(">", lt)
        if end < 0:
            # Nothing closes this tag, and no later ``<`` can be one either,
            # so the rest is text: a review that trails off mid-tag keeps the
            # characters its author typed. Escaping the tail in one pass
            # rather than looping keeps this scan linear on input like ``<b``
            # repeated to the field's length.
            yield False, text[i:].replace("<", "&lt;")
            return
        yield False, text[i:lt]
        yield True, text[lt : end + 1]
        i = end + 1


def _breaks(text: str) -> str:
    """Blank lines become paragraph breaks, single newlines line breaks."""
    return "</p><p>".join("<br/>".join(para.split("\n")) for para in _PARAGRAPHS.split(text))


def review_html(text: str) -> str:
    """Render a review body into a safe HTML fragment."""
    body = "".join(
        chunk if is_markup else _breaks(chunk) for is_markup, chunk in _runs(text.strip())
    )
    if not body:
        return ""
    # Sanitizing last lets its HTML5 parse normalize the mix of our paragraphs
    # and the body's own tags into a well-formed fragment (unclosed tags
    # closed, block elements lifted out of paragraphs), which a string
    # substitution over already-sanitized HTML could not do without
    # re-implementing the parser.
    return _EMPTY_PARAGRAPH.sub("", _CLEANER.clean(f"<p>{body}</p>"))
