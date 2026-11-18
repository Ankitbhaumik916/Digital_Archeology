import numpy as np

from da.entities import (acr_match, extract_mentions, find_definitions, norm_name, same_entity)
from da.embed_input import sample_segments
from da.textutil import (_params, band_hashes, clean_text, minhash, norm_for_hash, text_stats, word_shingles)


# ---------------------------------------------------------------- cleaning
def test_clean_text_dehyphenates_and_strips_page_markers():
    raw = "The govern-\nment argued.\n\n12\n\nPage 3 of 9\n\nIt &amp; the board.  *45 done"
    out = clean_text(raw)
    assert "government" in out and "Page 3" not in out and "&amp;" not in out and "*45" not in out


def test_noise_score_separates_garbage_from_prose():
    good = "The court considered whether the statute applied to the defendant in this matter. " * 20
    junk = "xj3kq9 bnmtrw 7f8g2h qqqqqq zzzxvv 9a8b7c8d " * 40
    assert text_stats(clean_text(good))[2] < 0.05
    assert text_stats(clean_text(junk))[2] > 0.5


# ---------------------------------------------------------------- minhash
def _jac(a, b, k=5):
    A, B = set(word_shingles(norm_for_hash(a), k)), set(word_shingles(norm_for_hash(b), k))
    return len(A & B) / len(A | B)


def test_minhash_estimates_jaccard_and_is_deterministic():
    rng = np.random.RandomState(0)
    vocab = [f"w{i}" for i in range(3000)]
    a = " ".join(rng.choice(vocab, 2500))
    words = a.split()
    b = " ".join(words[:2000] + list(rng.choice(vocab, 500)))
    x, y = _params(125)
    sa, sb = minhash(word_shingles(norm_for_hash(a), 5), x, y), minhash(word_shingles(norm_for_hash(b), 5), x, y)
    est, true = float((sa == sb).mean()), _jac(a, b)
    assert abs(est - true) < 0.12
    assert (sa == minhash(word_shingles(norm_for_hash(a), 5), x, y)).all()
    assert len(band_hashes(sa, 25)) == 25


# ---------------------------------------------------------------- entity resolution rules
def test_typo_and_abbreviation_merge():
    n = norm_name
    assert same_entity(n("Environmental Protection Agency"), n("Envir0nmental Protection Agency"))
    assert same_entity(n("Department of Labor"), n("Dept. of Labor"))
    assert same_entity(n("Federal Deposit Insurance Corporation"), n("Federal Deposit Insurance Corp."))
    assert same_entity(n("Patent and Trademark Office"), n("Patent & Trademark Office"))
    assert same_entity(n("Environmental Protection Agency"), n("United States Environmental Protection Agency"))


def test_similar_but_different_entities_stay_apart():
    n = norm_name
    for a, b in [("Department of Health", "Department of Housing"), ("Hartford Fire Insurance Company", "Hartford Life Insurance Company"),
                 ("Northern Pacific Railway", "Northern Pacific Railroad"), ("Clean Air Act", "Clean Water Act"),
                 ("Fourth Amendment", "Fifth Amendment"), ("Federal Trade Commission", "Federal Communications Commission")]:
        assert not same_entity(n(a), n(b)), (a, b)


def test_acronym_definitions():
    assert acr_match("FINRA", "Financial Industry Regulatory Authority".split())
    assert acr_match("DOJ", "Department of Justice".split())
    assert not acr_match("EPA", "Federal Trade Commission".split())
    d = dict(find_definitions('the Federal Deposit Insurance Corporation (FDIC) and the Department of Justice ("DOJ")'))
    assert d["fdic"].startswith("federal deposit insurance") and d["doj"] == "department of justice"


def test_extraction_splits_adjacent_entities_and_ignores_headings():
    got = [x[1] for x in extract_mentions(
        "OPINION The Department of Justice met. Under the Fourth Amendment and the NLRB, the Securities and Exchange "
        "Commission and Occupational Safety and Health Administration acted.", "", 50, 5)]
    assert {"department of justice", "fourth amendment", "nlrb", "securities and exchange commission",
            "occupational safety and health administration"} <= set(got)
    assert not any(g.startswith("opinion") for g in got)


def test_case_caption_parties():
    got = [x[1] for x in extract_mentions("Some text here.", "Acme Chemical Corporation v. Environmental Protection Agency", 50, 5)]
    assert "environmental protection agency" in got and "acme chemical" in got


# ---------------------------------------------------------------- embed input
def test_segments_span_the_document():
    text = " ".join(f"word{i}" for i in range(4000))
    segs = sample_segments(text, 4, 500)
    assert len(segs) == 4 and segs[0].startswith("word0") and "word39" in segs[-1]
    assert sample_segments("short text", 4, 500) == ["short text"]


# ---------------------------------------------------------------- regressions found on the 20k-document run
def test_corporate_suffix_typos_are_dropped():
    assert norm_name("Hartford Fire Insurance Compny") == norm_name("Hartford Fire Insurance Company")
    assert norm_name("Digital Compaq") == "digital compaq"       # a real name that merely looks like a suffix stays


def test_deletion_keys_give_exact_recall_for_single_edit_variants():
    from da.entities import deletion_keys
    a, b = norm_name("Copyright Office"), norm_name("Copyrlght Office")   # substitution
    c = norm_name("Copyriight Office")                                    # insertion
    d = norm_name("Copyrght Office")                                      # deletion
    for x in (b, c, d):
        assert set(deletion_keys(a)) & set(deletion_keys(x))
    assert not set(deletion_keys(norm_name("Department of Health"))) & set(deletion_keys(norm_name("Department of Housing")))
