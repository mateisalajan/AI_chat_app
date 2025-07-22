from collections import Counter
import re

def extract_keywords(text, top_n=5):
    words = re.findall(r'\w+', text.lower())
    common = Counter(words).most_common(top_n)
    return [word for word, count in common]
