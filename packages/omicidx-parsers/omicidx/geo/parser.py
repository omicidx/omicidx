"""Usage

Common use will be to get a list of accessions using:



"""

from Bio import Entrez
import logging
import collections
import re
import datetime
import urllib
from xml import etree
import xml
from io import StringIO
import requests
import httpx
from tenacity import retry
from tenacity.wait import wait_exponential
from tenacity.stop import stop_after_attempt
from urllib.error import HTTPError  # for Python 3

import omicidx.geo.pydantic_models as pydantic_models

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("omicidx")

ETYP = ["GSE", "GSM", "GPL", "GDS"]


def get_entrez_instance(email="user@example.com"):
    """Return a Bio::Entrez object

    # Arguments
    email (str): the email to be used with the Entrez instance

    Returns
    =======
    A Bio::Entrez instance
    """
    ret = Entrez
    ret.email = email
    return ret


def get_geo_accessions(
    etyp=None, batch_size=1000, add_term=None, email="user@example.com"
):
    """Get GEO accessions by etyp

    Useful for getting all the ETYP accessions for
    later bulk processing

    Parameters
    ----------
    etyp: str
        One of GSE, GPL, GSM, GDS
    batch_size: int
        the number of accessions to return in one batch.
        Transparent to the user, as this returns an iterator.
    add_term: str
        Add a search term for the query. Useful to limit
        by date or search for specific text. For example,
        to limit by date: '2007/01/01:2007/03/01[PDAT]'
    email: str
        user email (not important)

    Return
    ------
    an iterator of accessions, each as a string
    """
    term = ""
    if add_term is not None:
        term = term + add_term
    entrez = get_entrez_instance(email)
    if etyp is not None:
        if etyp not in ETYP:
            raise Exception(
                "etype {} not one of the accepted etyps {}".format(etyp, str(ETYP))
            )
        term = term + " " + etyp + "[ETYP]"
    if term != "":

        handle = entrez.esearch(db="gds", term=term, usehistory="y")
    else:
        raise Exception("term is required for entrez search access")
    search_results = entrez.read(handle)
    webenv = search_results["WebEnv"]
    query_key = search_results["QueryKey"]
    count = int(search_results["Count"])
    data = []
    logging.info("found {} records for {} database".format(count, etyp))
    n = 0
    for start in range(0, count, batch_size):
        end = min(count, start + batch_size)
        attempt = 0
        fetch_handle = None
        while attempt < 10:
            attempt += 1
            try:
                fetch_handle = entrez.esummary(
                    db="gds",
                    rettype="acc",
                    retmode="xml",
                    retstart=start,
                    retmax=batch_size,
                    webenv=webenv,
                    query_key=query_key,
                )
                for g in entrez.read(fetch_handle):
                    n += 1
                    yield (g["Accession"])

                break
            except (HTTPError, RuntimeError) as err:
                import time

                if isinstance(err, HTTPError):
                    logging.error("Received error from server %s" % err)
                    logging.error("Attempt %i of 10" % attempt)
                    time.sleep(1 * attempt * attempt)
                else:
                    logging.error("RuntimeError received: {}".format(err))
                    logging.error("Attempt %i of 10" % attempt)
                    time.sleep(1)
            else:
                raise


def get_geo_accession_xml(accession, targ="all", view="brief"):
    """Open a connection to get the GEO XML for an accession

    Parameters
    ==========
    accession: str the GEO accesssion
    targ: str what to fetch. One of "all", "series", "platform",
        "samples", "self"
    view: str amount to return.

    Returns
    =======
    A file-like object for reading or readlines

    >>> handle = get_geo_accession_xml('GSE2553')
    >>> handle.readlines()
    """
    url = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?targ={}&acc={}&form=xml&view={}".format(
        targ, accession, view
    )
    attempt = 0
    while attempt < 10:
        attempt += 1
        try:
            return urllib.request.urlopen(url)
            break
        except Exception as err:
            print("Received error from server %s" % err)
            print("Attempt %i of 3" % attempt)
            import time

            time.sleep(1 * attempt)


def get_geo_accession_soft(
    accession: str, targ: str = "all", view: str = "brief"
) -> str:
    """Open a connection to get the GEO SOFT for an accession

    Parameters
    ==========
    accession: str the GEO accesssion
    targ: str what to fetch. One of "all", "series", "platform",
        "samples", "self"
    view: str amount to return.

    Returns
    =======
    A file-like object for reading or readlines

    >>> handle = get_geo_accession_soft('GSE2553')
    >>> handle.readlines()
    """
    logging.info(f"Accessing accession {accession} in SOFT format")
    url = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?targ={}&acc={}&form=text&view={}".format(
        targ, accession, view
    )

    # @retry(
    #     wait=wait_exponential(multiplier=1, min=10, max=60), stop=stop_after_attempt(30)
    # )
    def _perform_get(url: str) -> str:
        resp = httpx.get(url, timeout=120)
        resp.raise_for_status()
        return resp.text

    res = _perform_get(url)
    return res


def _split_on_first_equal(line, val="="):
    """Split a line based on first occurrence of " ="

    Parameters
    ----------
    line: str
        string to split
    split: str
        split based on this. Can be multiple characters.
    >>> _split_on_first_equal("this = abc = 1")
    ('this', 'abc = 1')
    """
    l = line.split(" {} ".format(val))
    if l[0].endswith(" {}".format(val)):
        l[0] = l[0].replace(" {}".format(val), "")
    return (l[0], " {} ".format(val).join(l[1:]))


def get_geo_entities(txt):
    """Get the text associated with each entity from a block of text

    Parameters
    ----------
    txt: list(str)

    Returns
    -------
    A dict of entities keyed by accession and values a list of txt lines
    """
    logging.info("Parsing out geo entities")
    entities = {}
    accession = None
    entity = []
    for line in txt:
        line = line.strip()
        if line.startswith("^"):
            accession = _split_on_first_equal(line)[1]
            entities[accession] = entity
            entity = []
        entity.append(line)
    entities[accession] = entity
    logging.info(f"found {len(entities.keys())}")
    return entities


#####################################
# Utility functions for GSE parsing #
#####################################


def get_subseries_from_relations(relation_list):
    ret = []
    for i in relation_list:
        if i.startswith("SuperSeries of:"):
            ret.append(i.replace("SuperSeries of: ", ""))
    return ret


def get_bioprojects_from_relations(relation_list):
    ret = []
    for i in relation_list:
        if i.startswith("BioProject: https://www.ncbi.nlm.nih.gov/bioproject/"):
            ret.append(
                i.replace("BioProject: https://www.ncbi.nlm.nih.gov/bioproject/", "")
            )
    return ret


def get_SRA_from_relations(relation_list):
    ret = []
    for i in relation_list:
        if i.startswith("SRA: https://www.ncbi.nlm.nih.gov/sra?term="):
            ret.append(i.replace("SRA: https://www.ncbi.nlm.nih.gov/sra?term=", ""))
    return ret


def get_biosample_from_relations(relation_list):
    ret = []
    for i in relation_list:
        if i.startswith("BioSample: https://www.ncbi.nlm.nih.gov/biosample/"):
            ret.append(
                i.replace("BioSample: https://www.ncbi.nlm.nih.gov/biosample/", "")
            )
    return ret


def get_channel_characteristics(d, ch):
    ch_items = list([k for k in d.keys() if k.endswith("ch{}".format(ch))])
    characteristics = []
    ret = {}
    for k in ch_items:
        if k.startswith("characteristics"):
            for characteristic in d[k]:
                characteristics.append(tuple(characteristic.split(": ")))
            continue
        newk = k.replace("_ch{}".format(ch), "")
        ret[k.replace("_ch{}".format(ch), "")] = "\n".join(d[k])
        if newk == "taxid":
            ret[newk] = list([int(x) for x in ret[newk].split("\n")])
    char = []
    for v in characteristics:
        if len(v) == 1:
            tag = v[0]
            val = None
        elif len(v) == 2:
            tag = v[0]
            val = v[1]
        elif len(v) > 2:
            tag = v[0]
            val = ":".join(v[1:])
        char.append({"tag": tag, "value": val})
    ret["characteristics"] = char
    return ret


def _split_geo_name(v):
    """split name of form first,middle,last into dict"""
    return dict(zip("first middle last".split(), v.split(",")))


def _create_contact_from_parsed(d):
    contact_dict = {}
    for k, v in d.items():
        if k.startswith("contact"):
            v = v[0]
            if v == "":
                v = None
            if k == "contact_name":
                v = _split_geo_name(v)
            if k == "contact_web_link":
                if v is not None and type(v) == str and len(v):
                    if not str(v).startswith("http"):
                        v = "http://" + str(v)
            contact_dict[k.replace("contact_", "")] = v
    return contact_dict


def _split_contributor_names(contribs):
    if len(contribs) == 0:
        return []
    return list([_split_geo_name(v) for v in contribs])


###############################################
# Parse a single entity of SOFT format,       #
# generically. Then pass along to             #
# GSE, GSM, or GPL specific parser and return #
# appropriate class.                          #
###############################################
def _parse_single_entity_soft(entity_txt):
    # Deal with common stuff first:
    tups = [_split_on_first_equal(line) for line in entity_txt]
    accession = tups[0][1]

    entity_type = tups[0][0].replace("^", "")
    tups = [(re.sub("![^_]+_", "", tup[0]), tup[1]) for tup in tups]
    d = collections.defaultdict(list)
    for tup in tups[1:]:
        d[tup[0]].append(tup[1])
    d2 = dict((k, v) for k, v in d.items())
    # int fields
    INT_FIELDS = ["pubmed_id", "sample_taxid", "platform_taxid"]
    for i in INT_FIELDS:
        if i in d2:
            try:
                d2[i] = list(map(lambda x: int(x), d2[i]))
            except:
                d2[i] = []
    # change "geo_accesion = [...]" to "accession = ..."
    if "geo_accession" in d2:
        d2["accession"] = d2["geo_accession"][0]
    elif "accession" in d2:
        d2["accession"] = d2["accession"][0]
    else:
        d2["accession"] = "UNKNOWN"  # type: ignore
    if "description =" in d2:
        del d2["description ="]
    for k in [
        "status",
        "description",
        "data_processing",
        "title",
        "summary",  # GSE only?
        "submission_date",  # to fix later
        "last_update_date",  # to fix later
        "overall_design",
    ]:  # GSE only ]:
        try:
            d2[k] = "\n".join(d2[k])
        except KeyError:
            d2[k] = None
    # contact details as object
    contact = _create_contact_from_parsed(d2)
    dkeys = d2.keys()
    for k in list(dkeys):
        if k.startswith("contact"):
            del d2[k]
    d2["contact"] = contact
    if entity_type == "SERIES":
        return _parse_single_gse_soft(d2)
    if entity_type == "SAMPLE":
        return _parse_single_gsm_soft(d2)
    if entity_type == "PLATFORM":
        return _parse_single_gpl_soft(d2)


###############################################################################
#        Update date fields of format 'month day year' to date type.          #
###############################################################################
def _fix_date_fields(d):
    for datefield in ["submission_date", "last_update_date"]:
        if datefield in d:
            d[datefield] = datetime.datetime.strptime(d[datefield], "%b %d %Y")
    return d


#######################################
# Parse the GSE entity in SOFT format #
#######################################
def _parse_single_gse_soft(d2):
    try:
        d2["subseries"] = get_subseries_from_relations(d2["relation"])
        d2["bioprojects"] = get_bioprojects_from_relations(d2["relation"])
        d2["sra_studies"] = get_SRA_from_relations(d2["relation"])
        # GEO sometimes references SRS and SRR from series--filter those out
        d2["sra_studies"] = list(filter(lambda a: a.find("P") > 0, d2["sra_studies"]))
    except KeyError:
        d2["subseries"] = []
        d2["bioprojects"] = []
        d2["sra_studies"] = []
    supp_files = []
    for k in list(d2.keys()):
        if k.startswith("supplementa"):
            supp_files += d2[k]
            del d2[k]
    d2["supplemental_files"] = supp_files
    if len(supp_files) > 0:
        if supp_files[0] == "NONE":
            supp_files = []
    d2["_entity"] = "GSE"
    if "contributor" in d2:
        d2["contributor"] = _split_contributor_names(d2["contributor"])
    d2 = _fix_date_fields(d2)
    return pydantic_models.GEOSeries(**d2)


def _create_gsm_channel_data(d):
    ch_count = int(d["channel_count"])
    ch_recs = []
    for i in range(0, ch_count):
        # this GEOChannel just encapsulates the parsing
        # of channel records.
        # TODO: refactor to parse directly to dict
        ch_recs.append(get_channel_characteristics(d, i + 1))
    return ch_recs


# Search for fields like _ch1 and _ch2 ....
import re

r1 = re.compile(r"_ch\d+$")


#######################################
# Parse the GSM entity in SOFT format #
#######################################
def _parse_single_gsm_soft(d2):
    # to singular:
    for k in [
        "platform_id",
        "channel_count",
        "library_strategy",
        "library_source",
        "library_selection",
        "instrument_model",
        "data_row_count",
        "anchor",
        "tag_length",
        "type",
        "tag_count",
    ]:
        try:
            d2[k] = "\n".join(d2[k])
        except KeyError:
            d2[k] = None
    try:
        d2["biosample"] = get_biosample_from_relations(d2["relation"])[0]
        d2["sra_experiment"] = get_SRA_from_relations(d2["relation"])[0]
    except:
        d2["biosample"] = None
        d2["sra_experiment"] = None
    if "contributor" in d2:
        d2["contributor"] = _split_contributor_names(d2["contributor"])
    d2["channels"] = _create_gsm_channel_data(d2)
    supp_files = []
    for k in list(d2.keys()):
        if r1.search(k):  # have to use search here because endswith does not take regex
            del d2[k]
        if k.startswith("supplementa"):
            supp_files += d2[k]
            del d2[k]
    d2["supplemental_files"] = supp_files
    if supp_files[0] == "NONE":
        supp_files = []
    d2["_entity"] = "GSM"
    d2 = _fix_date_fields(d2)
    # Collapse protocol fields into single string
    for k in filter(lambda k: k.endswith("protocol"), d2.keys()):
        if isinstance(d2[k], list):
            d2[k] = " ".join(d2[k])
    if "contributor" in d2:
        d2["contributor"] = _split_contributor_names(d2["contributor"])
    return pydantic_models.GEOSample(**d2)


#######################################
# Parse the GPL entity in SOFT format #
#######################################
def _parse_single_gpl_soft(d2):
    for k in ["technology", "distribution", "organism", "taxid", "data_row_count"]:
        try:
            d2[k] = "\n".join(d2[k])
        except KeyError:
            d2[k] = None
    d2["_entity"] = "GPL"
    d2 = _fix_date_fields(d2)
    if "manufacture_protocol" in d2:
        d2["manufacture_protocol"] = "\n".join(d2["manufacture_protocol"])
    if "contributor" in d2:
        d2["contributor"] = _split_contributor_names(d2["contributor"])
    return pydantic_models.GEOPlatform(**d2)


###############################################################################
#                   This is the main entrypoint for parsing.                  #
###############################################################################
def geo_entity_iterator(geo: str, targ: str = "self", view: str = "brief"):
    """Returns an iterator of GEO entities

    Given a GEO accession (typically a GSE,
    will return an iterator of the GEO entities
    associated with the record, including all
    GSMs, GPLs, and the GSE record itself

    Parameters
    ----------
    fh: Iterable[str]
       Anything that can iterate over lines of text
       Could be a list of text lines, a file handle, or
       an open url.

    Yields
    ------
    Iterator of GEO entities as pydantic models


    >>> for i in geo_soft_entity_iterator('GSE2553'):
    ...     print(i)
    """
    entity = []
    accession = None
    in_table = False
    logger.info(f"Getting GEO entities for {geo}")
    text = get_geo_accession_soft(geo, targ=targ, view=view)
    logger.info(f"Got GEO entities for {geo}")
    for line in StringIO(text):
        try:
            if isinstance(line, bytes):
                line = line.decode()
        except:
            pass
        line = line.strip()

        # ignore table details for now

        if line.endswith("table_begin"):
            in_table = True
        if line.endswith("table_end"):
            in_table = False
            continue
        if in_table:
            continue

        # ignore comments

        if line.startswith("#"):
            continue

        if line.startswith("^"):
            if accession is not None:
                logger.info(f".. parsing found {accession}")
                yield (_parse_single_entity_soft(entity))
            accession = _split_on_first_equal(line)[1]
            entity = []

        entity.append(line)
    yield (_parse_single_entity_soft(entity))


def gse_to_json(gse):
    """Return json version of GSE record as text

    Parameters
    ----------
    gse : str
        The GSE accession to convert
    """
    res = []
    for z in geo_entity_iterator(gse):
        res.append(z.json())
    return "\n".join(res)


def bulk_gse_to_json(term, outfile="/dev/stdout"):
    """Bulk process GSEs to json

    This function takes a term (see below) and does a search. Results
    from the search of GSE records are returned as json lines to the
    output file in `outfile`.

    Parameters
    ----------
    term : str
        A search string like "2019/01/28:2019/02/28[PDAT]" as an example.
        Passed to entrez search.
    outfile: str
        The output file to which to write
    """
    for gse in get_geo_accessions(add_term=term):
        if outfile is not None:
            logging.info(f"writing accession {gse}")
            res = gse_to_json(gse)
            if res is not None:
                outfile.write(res + "\n")
            else:
                logging.warning(f"Accession {gse} not found, it appears.")


def gse_accessions(term, outfile="/dev/stdout"):
    """Output list of gse accessions based on term search

    Writes to a file.

    Parameters
    ----------
    term : str
        A search string like "2019/01/28:2019/02/28[PDAT]" as an example.
        Passed to entrez search.
    outfile: str
        The output file to which to write

    """

    with open(outfile, "w") as ofile:
        n = 0
        for gse in get_geo_accessions(add_term=term, batch_size=1000):
            n += 1
            if n % 5000 == 0:
                logging.info(f"Processed {n} records")
            ofile.write(gse + "\n")
        logging.info(f"Processed {n} records")
