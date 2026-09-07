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

So every review body goes through one sanitizer: allowlisted tags survive as
markup, everything else is dropped or escaped as text. There is deliberately
no "does this look like HTML?" sniffing — a review that mentions ``<3`` would
misfire, and a field that is *sometimes* markup has to be treated as markup
always.

Note the ambiguity this leaves: an author who writes about the ``<b>`` tag in
a plain-text review has it read as bold. That is inherent to a field which
mixes plain text and editor HTML with nothing to tell them apart, and erring
toward markup is what the app that wrote the record does too.
"""

from __future__ import annotations

import re

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

_PARAGRAPHS = re.compile(r"\n{2,}")

# The sanitizer re-parses the fragment as HTML5, so a block element nested in
# one of our paragraphs closes it and leaves a stray empty paragraph behind
# (``<p>a<blockquote>b</blockquote></p>`` -> ``...<blockquote>b</blockquote><p></p>``).
_EMPTY_PARAGRAPH = re.compile(r"<p>\s*</p>")


def review_html(text: str) -> str:
    """Render a review body into a safe HTML fragment.

    Newlines become markup *before* sanitizing rather than after: the
    sanitizer's HTML5 parse then normalizes the mix of our paragraphs and the
    body's own tags into a well-formed fragment (unclosed tags closed, block
    elements lifted out of paragraphs), which a string substitution over
    already-sanitized HTML could not do without re-implementing the parser.
    """
    body = "".join(
        "<p>" + "<br/>".join(para.split("\n")) + "</p>"
        for para in _PARAGRAPHS.split(text.strip())
        if para.strip()
    )
    return _EMPTY_PARAGRAPH.sub("", _CLEANER.clean(body))
