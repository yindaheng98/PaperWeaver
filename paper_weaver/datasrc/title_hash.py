"""Title hash strategies for matching titles from different data sources."""

import re
import unicodedata
from typing import Dict


def to_lowercase(title: str) -> str:
    """Convert title to lowercase."""
    return title.lower()


def strip_whitespace(title: str) -> str:
    """Strip leading and trailing whitespace."""
    return title.strip()


def normalize_whitespace(title: str) -> str:
    """Collapse multiple consecutive whitespace characters to a single space."""
    return re.sub(r"\s+", " ", title)


def remove_all_whitespace(title: str) -> str:
    """Remove all whitespace characters."""
    return re.sub(r"\s+", "", title)


def remove_trailing_period(title: str) -> str:
    """Remove trailing period(s) from the title."""
    return title.rstrip(".")


def remove_trailing_punctuation(title: str) -> str:
    """Remove common trailing punctuation marks."""
    return title.rstrip(".,;:!?")


def remove_all_punctuation(title: str) -> str:
    """
    Remove all punctuation, keeping only alphanumeric characters,
    CJK characters, and Unicode ligatures.
    """
    return re.sub(r"[^0-9a-zA-Z\u4E00-\u9FFF\uFB00-\uFEFF\s]", "", title)


def remove_all_punctuation_and_space(title: str) -> str:
    """
    Remove all punctuation and whitespace, keeping only alphanumeric characters,
    CJK characters, and Unicode ligatures.
    """
    return re.sub(r"[^0-9a-zA-Z\u4E00-\u9FFF\uFB00-\uFEFF]", "", title)


def normalize_unicode(title: str) -> str:
    """
    Normalize Unicode characters using NFKC normalization.
    This handles ligatures, full-width characters, etc.
    """
    return unicodedata.normalize("NFKC", title)


def normalize_quotes(title: str) -> str:
    """Normalize various quote characters to standard ASCII quotes."""
    # Normalize single quotes
    title = re.sub(r"[''`‚‛]", "'", title)
    # Normalize double quotes
    title = re.sub(r"[""„‟«»]", '"', title)
    return title


def normalize_dashes(title: str) -> str:
    """Normalize various dash/hyphen characters to standard ASCII hyphen."""
    return re.sub(r"[‐‑‒–—―−]", "-", title)


def remove_articles(title: str) -> str:
    """Remove common leading articles (a, an, the) for sorting/matching."""
    return re.sub(r"^(a|an|the)\s+", "", title, flags=re.IGNORECASE)


def remove_html_tags(title: str) -> str:
    """Remove HTML/XML tags from title."""
    return re.sub(r"<[^>]+>", "", title)


def normalize_ampersand(title: str) -> str:
    """Normalize '&' and 'and' variations."""
    # Replace & with 'and'
    return re.sub(r"\s*&\s*", " and ", title)


def basic_normalize(title: str) -> str:
    """
    Apply basic normalization: lowercase, strip, normalize whitespace.
    This is a gentle normalization that preserves most characters.
    """
    result = title
    result = normalize_unicode(result)
    result = to_lowercase(result)
    result = strip_whitespace(result)
    result = normalize_whitespace(result)
    return result


def moderate_normalize(title: str) -> str:
    """
    Apply moderate normalization: basic + remove trailing punctuation,
    normalize quotes and dashes.
    """
    result = basic_normalize(title)
    result = normalize_quotes(result)
    result = normalize_dashes(result)
    result = remove_trailing_punctuation(result)
    return result


def aggressive_normalize(title: str) -> str:
    """
    Apply aggressive normalization: remove all punctuation and whitespace,
    lowercase. This creates a highly compressed hash for fuzzy matching.
    """
    result = title
    result = normalize_unicode(result)
    result = remove_html_tags(result)
    result = to_lowercase(result)
    result = remove_all_punctuation_and_space(result)
    return result


def title_hash(title: str) -> Dict[str, str]:
    """
    Generate multiple hash versions of a title for matching across different data sources.

    Returns a dict of normalized title versions with method names as keys:
    - basic: lowercase, trimmed, normalized whitespace
    - moderate: basic + normalized quotes/dashes, no trailing punctuation
    - aggressive: only alphanumeric and CJK characters, no spaces

    Args:
        title: The original title string

    Returns:
        A dict mapping hash method names to their hash values
    """
    if not title:
        return {}

    hashes = {}

    # Basic normalization
    hashes["basic"] = basic_normalize(title)

    # Moderate normalization
    hashes["moderate"] = moderate_normalize(title)

    # Aggressive normalization (best for cross-source matching)
    hashes["aggressive"] = aggressive_normalize(title)

    return hashes


def get_canonical_hash(title: str) -> str:
    """
    Get the most aggressive (canonical) hash for a title.
    This is the best hash for matching titles across different sources.

    Args:
        title: The original title string

    Returns:
        A canonical hash string
    """
    return aggressive_normalize(title)


if __name__ == "__main__":
    # Comprehensive test cases
    test_cases = [
        # Basic punctuation and whitespace
        "  An Example: Title with Punctuation!  ",
        # Multiple consecutive spaces
        "Deep   Learning    for   NLP",
        # Trailing punctuation variations
        "A Survey on Machine Learning.",
        "What is AI?",
        "Hello World!!!",
        # Special quotes (curly quotes)
        "\u201cSmart Quotes\u201d and \u2018Single Quotes\u2019",
        # Special dashes (em-dash, en-dash)
        "Neural Networks — A Comprehensive Review",
        "2020–2024: A Survey",
        # HTML tags
        "A <i>Survey</i> on <b>Deep Learning</b>",
        # Ampersand
        "Tom & Jerry: A Story",
        # CJK characters (Chinese)
        "深度学习在自然语言处理中的应用",
        # Mixed CJK and English
        "BERT: 预训练语言模型",
        # Unicode ligatures and special chars
        "The ﬁrst ﬂight of ﬀ ligatures",
        # Full-width characters (common in CJK text)
        "Ｆｕｌｌ－Ｗｉｄｔｈ　Ｔｅｘｔ",
        # Leading articles
        "The Quick Brown Fox",
        "A New Approach to AI",
        # Empty and edge cases
        "",
        "   ",
        # Only punctuation
        "...",
        # Numbers
        "GPT-4: A 2023 Model",
        # Colon variations
        "Title: Subtitle - Part 1",
        # Complex real-world example
        "  BERT: Pre-training of Deep Bidirectional Transformers for Language Understanding.  ",
    ]

    print("=" * 80)
    print("Title Hash Test Cases")
    print("=" * 80)

    for i, title in enumerate(test_cases, 1):
        print(f"\n[Test {i}] Original: {repr(title)}")
        hashes = title_hash(title)
        if hashes:
            for method, h in hashes.items():
                print(f"  {method:12s}: {repr(h)}")
        else:
            print("  (empty result)")
        print(f"  {'canonical':12s}: {repr(get_canonical_hash(title))}")
