"""Simple utility functions for EBI ENA"""
import urllib.request
import xml.etree.ElementTree as etree
import csv
import io
import omicidx.sra.parser


def get_xml_for_accession(accession: str):
    """Get an XML record for a single accession as an ElementTree Node from EBI

    Parameters
    ----------
    accession: str
        Any SRA/ENA accession

    Returns
    -------
    an etree ElementNode parsed from the retrieved XML."""
    url = "https://www.ebi.ac.uk/ena/data/view/{accession}&display=xml&download=xml&filename={accession}.xml"
    url = url.format(accession=accession)
    with urllib.request.urlopen(url) as response:
        return (etree.parse(response).getroot()[0])

def get_file_report_for_accession(accession: str):
    """Get a file report from EBI

    Parameters
    ----------
    accession: str
        Any SRA/ENA accession

    Returns
    -------
    a list of rows representing available files for the accession
    """
    url = "https://www.ebi.ac.uk/ena/data/warehouse/filereport?accession={accession}&result=read_run"
    url = url.format(accession=accession)
    print(url)
    response = urllib.request.urlopen(url)
    reader = csv.DictReader(io.StringIO(response.read().decode()),
                            delimiter="\t")
    return (list(reader))

def get_parsed_xml_for_accession(accession: str):
    """A parsed SRA record from EBI for a given accession

    Args:
        accession (str): An SRA accession (SRR, SRP, SRS, SRX)

    Returns:
        SRARecord: A parsed SRA accession
    """    
    xml = get_xml_for_accession(accession)
    xmlfunc = getattr(omicidx.sra.parser,
                        '{}_xml_iter_parser'.format(xml.tag.lower()))
    return (xmlfunc(xml))
