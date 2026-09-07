"""Review bodies that arrive as the writing app's HTML (translate.richtext)."""

from __future__ import annotations

import pytest
from skybridge.translate import neodb
from skybridge.translate.richtext import review_html

# The real record that surfaced this: BookHive's editor writes line breaks as
# literal <br>, so escaping the body showed &lt;br&gt; to every reader.
# (at://did:plc:4r63iuy5cy4upfrdp3ww6gpd/buzz.bookhive.book/3msg6wfxovkkk)
BOOKHIVE_REVIEW = (
    "I missed out on the books when I was growing up.<br><br>"
    "This was unabashedly a children's book.<br><br>"
    "However, it's a great children's book!"
)

# popfeed's Letterboxd importer writes that site's markup into feed.review.text
# and leaves facets empty.
LETTERBOXD_TEXT = (
    'all this film did was remind me of what made <a href="http://letterboxd.com/film/moon/">'
    "<i>Moon</i></a> enjoyable."
)


def test_bookhive_line_breaks_survive_as_markup():
    assert review_html(BOOKHIVE_REVIEW) == (
        "<p>I missed out on the books when I was growing up.<br><br>"
        "This was unabashedly a children's book.<br><br>"
        "However, it's a great children's book!</p>"
    )


def test_letterboxd_markup_survives_with_a_safe_rel():
    assert review_html(LETTERBOXD_TEXT) == (
        "<p>all this film did was remind me of what made "
        '<a href="http://letterboxd.com/film/moon/" rel="nofollow noopener">'
        "<i>Moon</i></a> enjoyable.</p>"
    )


def test_plain_text_keeps_paragraph_and_line_breaks():
    assert review_html("one\ntwo\n\n\nthree") == "<p>one<br>two</p><p>three</p>"


def test_stray_angle_brackets_in_prose_are_escaped():
    assert review_html("I love <3 this, and 5 < 6 & 7 > 2") == (
        "<p>I love &lt;3 this, and 5 &lt; 6 &amp; 7 &gt; 2</p>"
    )


def test_a_bracketed_url_is_prose_not_a_tag():
    # `https` names no element, so this is text. Reading it as a tag deletes
    # the author's URL instead of showing it.
    assert review_html("More: <https://example.com/review>") == (
        "<p>More: &lt;https://example.com/review&gt;</p>"
    )


def test_a_bracketed_aside_is_prose_not_a_tag():
    # A real bridged review opens with this; `insert` names no element.
    assert review_html("<insert witty joke here>\n\nOn to the film.") == (
        "<p>&lt;insert witty joke here&gt;</p><p>On to the film.</p>"
    )


def test_a_real_element_name_is_markup_even_when_we_drop_it():
    # The flip side, also from a real bridged review: `div` is an element, so
    # it belongs to the sanitizer rather than on the page as literal text.
    assert review_html("<div>A great choice.</div>") == "A great choice."


@pytest.mark.parametrize(
    "href",
    [
        "javascript:alert(1)",
        " javascript:alert(1)",  # leading space: not a scheme of its own
        "data:text/html;base64,PHNjcmlwdD4=",
        "/relative",  # schemeless, so a scheme allowlist alone lets it pass
    ],
)
def test_unsafe_link_loses_its_href(href):
    out = review_html(f'<a href="{href}">click</a>')
    assert "href" not in out
    assert "click" in out


def test_script_and_style_lose_their_content_too():
    # Stripping only the tags would leave the script body as visible text.
    assert review_html("<script>alert(1)</script>ok") == "<p>ok</p>"
    assert review_html("<style>body{color:red}</style>ok") == "<p>ok</p>"


def test_event_handlers_and_non_allowlisted_elements_are_dropped():
    assert review_html("<img src=x onerror=alert(1)>text") == "<p>text</p>"
    assert review_html('<b class="x" onclick="y()">bold</b>') == "<p><b>bold</b></p>"
    assert review_html("<h1>shouting</h1>") == "shouting"


def test_a_newline_inside_a_tag_does_not_break_it():
    # Line breaks belong in text, never inside markup: substituting one into
    # the tag would leave the attributes behind as visible text.
    assert review_html('<a\nhref="https://example.com">link</a>') == (
        '<p><a href="https://example.com" rel="nofollow noopener">link</a></p>'
    )


def test_a_blank_line_inside_an_inline_element_splits_cleanly():
    assert review_html("<i>a\n\nb</i>") == "<p><i>a</i></p><p><i>b</i></p>"


def test_a_body_trailing_off_mid_tag_keeps_its_text():
    # Nothing closes this tag, so it was never markup; the characters the
    # author typed survive rather than being swallowed as a tag name.
    assert review_html("I rated it <b") == "<p>I rated it &lt;b</p>"


def test_output_is_well_formed_despite_unclosed_and_nested_blocks():
    assert review_html("<b>unclosed") == "<p><b>unclosed</b></p>"
    # A block element inside our paragraph closes it; the stray empty
    # paragraph that leaves behind is dropped rather than shipped.
    assert review_html("<blockquote>quoted</blockquote>") == "<blockquote>quoted</blockquote>"
    assert review_html("lead<blockquote>quoted</blockquote>") == (
        "<p>lead</p><blockquote>quoted</blockquote>"
    )


def test_empty_and_whitespace_only_bodies_render_nothing():
    assert review_html("") == ""
    assert review_html("  \n\n \n ") == ""


def test_facets_keep_the_escaped_richtext_path():
    # Facet indices are byte offsets into the raw text, so a faceted record
    # must not be sanitized: escaping everything keeps the spans aligned.
    text = "see <b>bold</b> here"
    facets = [
        {
            "index": {"byteStart": 0, "byteEnd": 3},
            "features": [{"$type": "app.bsky.richtext.facet#link", "uri": "https://example.com"}],
        }
    ]
    assert neodb._review_body(text, facets) == (
        '<p><a href="https://example.com" rel="nofollow noopener">see</a>'
        " &lt;b&gt;bold&lt;/b&gt; here</p>"
    )
    # No facets on the same text: the sanitizer path, markup preserved.
    assert neodb._review_body(text, []) == "<p>see <b>bold</b> here</p>"
    assert neodb._review_body(text, None) == "<p>see <b>bold</b> here</p>"
