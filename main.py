import asyncio
import copy
import itertools
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from pprint import pformat
from string import Template
from typing import Iterable, Tuple, TypeVar

from aiohttp import ClientSession
from bs4 import BeautifulSoup, Tag

WIKIMEDIA_BATCH_CAP = 49
TMP_ARTICLE_PIC_PATH = Path("shielded-cute-animal-pics")
ARTICLE_NAME = "List_of_animal_names"
# Index of Terms_by_species_or_taxon section.
# It's hardcoded because while Wikimedia's API does allow finding a section's
# ID by its title, it doesn't work in this particular page (I guess its TOC is
# irregular?). So this is the next best thing, and at least it won't break on
# title change ;)
SECTION_INDEX = 2

T = TypeVar("T")
U = TypeVar("U")


@dataclass
class Ref:
    """
    Noticed that some rows have that little "See xxx" below the animal name?
    This class represents a reference to that xxx
    """

    referral: str


async def main():
    async with ClientSession() as session:
        section_raw = await fetch_section_html(ARTICLE_NAME, SECTION_INDEX, session)
        section = BeautifulSoup(section_raw, "html.parser")
        animal_to_cas_index = resolve_refs(dict(parse_species_table(section)))

        ## thumbnail fetching
        animals = animal_to_cas_index.keys()
        links_futures = [
            fetch_thumbnails_links(batch, session)
            for batch in batched(animals, WIKIMEDIA_BATCH_CAP)
        ]
        links = join_dicts(await asyncio.gather(*links_futures))
        logging.info(pformat(links))
        pics_futures = [
            dl_file(url, animal_name, session) for (animal_name, url) in links.items() if url
        ]
        pic_index = dict(await asyncio.gather(*pics_futures))
        logging.info(pformat(pic_index))

        ## html output
        ca_to_animals_index = invert(animal_to_cas_index)
        logging.info(pformat(ca_to_animals_index))
        make_html_result(ca_to_animals_index, pic_index)


def invert(index: dict[T, list[U]]) -> dict[U, list[T]]:
    """
    Transform an index of x->ys to an index of y->xs.

    >>> d = {"a": [1, 2], "b": [1, 3, 4]}
    >>> invert(d)
    {1: ['a', 'b'], 2: ['a'], 3: ['b'], 4: ['b']}
    """

    result: dict[U, list[T]] = {}
    for t, us in index.items():
        for u in us:
            result.setdefault(u, []).append(t)
    return result


def resolve_refs(has_unresolved: dict[str, list[str] | Ref]) -> dict[str, list[str]]:
    """
    Returns a copy of has_unresolved where every Ref value is replaced with a
    pointer to the CA list in its referral.

    >>> d = {"a": ["hello"], "b": Ref("a")}
    >>> resolve_refs(d)
    {'a': ['hello'], 'b': ['hello']}
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
        adjs = [re.sub("\s+\\(.*\\)", "", adj) for adj in collat_adj_tag.stripped_strings]

        yield animal_name, adjs


async def fetch_section_html(article: str, section_index: int, sess: ClientSession) -> str:
    """
    Fetches only the specified section out of the article.
    Returns its content as HTML.
    """
    req = sess.get(
        "https://en.wikipedia.org/w/api.php",
        headers={"Accept-Encoding": "gzip"},
        params={
            "page": "List_of_animal_names",
            "action": "parse",
            "prop": "text",
            "format": "json",
            "formatversion": "2",
            "section": str(section_index),
        },
    )
    async with req as resp:
        return (await resp.json())["parse"]["text"]


async def fetch_thumbnails_links(titles: list[str], sess: ClientSession) -> dict[str, str]:
    """
    Bulk fetch thumbnails of all of articles corresponding to titles.

    Returns a dictionary mapping animal names to URL of their article's leading picture.
    """
    req = sess.get(
        "https://en.wikipedia.org/w/api.php",
        headers={"Accept-Encoding": "gzip"},
        params={
            "titles": "|".join(titles),
            "action": "query",
            "prop": "pageimages|pageterms",
            "piprop": "thumbnail",
            "pithumbsize": "200",
            "format": "json",
            "formatversion": "2",
        },
    )

    async with req as resp:
        resp_json = await resp.json()
        logging.info(pformat(resp_json))
        pages = resp_json["query"]["pages"]
        return {
            page["title"]: (page["thumbnail"]["source"] if "thumbnail" in page else None)
            for page in pages
        }


async def dl_file(link: str, animal_name: str, sess: ClientSession) -> tuple[str, Path]:
    """
    Asynchronously downloads content at link into a file named animal_name
    under the project's temporary dir.

    Returns a Path to the downloaded file, and the animal name so we won't get
    lost in the sauce.
    """
    # assuming ext is jpg because I'm tired
    target = TMP_ARTICLE_PIC_PATH / f"{animal_name}.jpg"
    async with sess.get(link) as resp:
        with target.open("wb") as f:
            f.write(await resp.read())
    return animal_name, target


def make_html_result(ca_animals_index: dict[str, list[str]], pic_index: dict[str, Path]) -> Path:
    with open("./template.html") as f:
        template = Template(f.read())

    rows = []
    for ca, animals in ca_animals_index.items():
        pic = pic_index.get(animals[0])
        row = f"""
        <tr>
          <td>{ca}</td>
          <td>{', '.join(animals)}</td>
          <td><img src="{pic.absolute() if pic else ''}"></img></td>
        </tr>
        """
        rows.append(row)

    out = Path("./out.html")
    with out.open("w") as f:
        f.write(template.substitute(tbody="".join(rows)))
    return out


def batched(iterable, n):
    """
    Batch data into tuples of length n. The last batch may be shorter.
    >>> list(batched('ABCDEFG', 3))
    [('A', 'B', 'C'), ('D', 'E', 'F'), ('G',)]
    """
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(itertools.islice(it, n)):
        yield batch


def join_dicts(dicts: list[dict]) -> dict:
    """
    >>> join_dicts([{"a": 1}, {"b": 2}])
    {'a': 1, 'b': 2}
    """
    result = {}
    for d in dicts:
        result.update(d)
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    TMP_ARTICLE_PIC_PATH.mkdir(exist_ok=True)
    asyncio.run(main())
