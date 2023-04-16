import copy
import re
from dataclasses import dataclass
from typing import Iterable, Tuple

import requests
from bs4 import BeautifulSoup, Tag

# Index of Terms_by_species_or_taxon section.
# It's hardcoded because while Wikimedia's API does allow finding a section's
# ID by its title, it doesn't work in this particular page (I guess its TOC is
# irregular?). So this is the next best thing, and at least it won't break on
# title change ;)
SECTION_INDEX = "2"


@dataclass
class Ref:
    """
    Noticed that some rows have that little "See xxx" below the animal name?
    This class represents a reference to that xxx
    """

    referral: str


def main():
    section = BeautifulSoup(fetch_section_html())
    animal_to_ca_index = resolve_refs(dict(parse_species_table(section)))
    print(invert(animal_to_ca_index))


def invert(animal_to_ca: dict[str, list[str]]) -> dict[str, list[str]]:
    "Transform an index of animal->CAs to an index of CA->animals"

    result: dict[str, list[str]] = {}
    for animal_name, cas in animal_to_ca.items():
        for ca in cas:
            result.setdefault(ca, []).append(animal_name)
    return result


def resolve_refs(has_unresolved: dict[str, list[str] | Ref]) -> dict[str, list[str]]:
    """
    Returns a copy of has_unresolved where every Ref value is replaced with a
    pointer to the CA list in its referral
    """
    cas = copy.deepcopy(has_unresolved)
    only_refs = filter(lambda x: isinstance(x[1], Ref), dict(cas).items())
    for k, v in only_refs:
        if isinstance(v, Ref):
            try:
                cas[k] = cas[v.referral]
            # some rows have weird names, e.g. the donkey row is named
            # "Ass/Donkey" and there's also _another_ donkey row which refers
            # to "Ass". So let's just skip these for now
            except KeyError:
                del cas[k]
    # mypy is concerned that the values may be of type Ref, but the loop above
    # eliminated all Ref types so we're safe to ignore this error.
    return cas  # type: ignore


def parse_species_table(table: Tag) -> Iterable[Tuple[str, list[str] | Ref]]:
    """
    Given the table under the section "Terms by species or taxon", returns an
    iterable of tuples, where every tuple represents an animal name and its
    collateral adjectives (CAs).

    Assumes the first column is the animal's name, and that the fifth column is
    a list of CAs separated by <br> tags.

    Rows where the CA column is a question mark are discarded.

    Some rows are merely aliases for other rows; for example, the Bull row only
    holds the text "See Cattle". In this case, the special Ref class is
    returned, signaling that this field needs to be evaluated later.

    This iterator is intended to be converted to a dict!
    """
    for row in table.find_all("tr"):
        cols = row.find_all("td")

        # if no <td> tags are present, it's probably a header row or an anchor
        # row, so nothing interesting here
        if len(cols) == 0:
            continue

        animal_name_tag: Tag = cols[0]
        collat_adj_tag: Tag = cols[5]

        if collat_adj_tag.text == "?":
            continue

        # the <td> tag sometimes contains extra crufty text such as references
        # or links to related articles. the <a> tag contains only the name
        animal_name_link = animal_name_tag.find("a")
        if not animal_name_link:
            raise ValueError(f"no <a> tag in first child of: {str(row)}")
        animal_name = animal_name_link.text

        if collat_adj_tag.text == "":
            # the optional closing bracket is because some of the see xxx's are
            # surrounded by parentheses
            if match := re.search("See (.*?)\\)?$", animal_name_tag.text, re.I):
                yield animal_name, Ref(match.group(1))
                continue

        # I don't care about references!!!!!! I'm not interesting in footnotes!!!
        # idc if citation is needed!!!!
        for sup in collat_adj_tag.find_all("sup"):
            sup.decompose()

        # discard anything between parentheses. usually pointless but maybe I
        # just didn't understand the assignment
        adjs = [
            re.sub("\s+\\(.*\\)", "", adj) for adj in collat_adj_tag.stripped_strings
        ]

        yield animal_name, adjs


def fetch_section_html() -> str:
    resp = requests.get(
        "https://en.wikipedia.org/w/api.php",
        headers={"Accept-Encoding": "gzip"},
        params={
            "page": "List_of_animal_names",
            "action": "parse",
            "prop": "text",
            "format": "json",
            "formatversion": "2",
            "section": SECTION_INDEX,
        },
    )
    return resp.json()["parse"]["text"]


if __name__ == "__main__":
    main()
