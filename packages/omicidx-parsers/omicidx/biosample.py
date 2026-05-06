"""Biosample parser

Implemented as an iterator


>>> import omicidx.biosample as b
>>> for bios in b.BioSampleParser(gzip.open('biosample_set.xml.gz', 'rb')):
>>>     print(bios.json())
>>>     print(bios.dict())
"""

import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element
import typing
import re
import pydantic
import datetime


class IdRecs(pydantic.BaseModel):
    db: typing.Optional[str]
    label: typing.Optional[str]
    id: typing.Optional[str]


class AttrRecs(pydantic.BaseModel):
    attribute_name: typing.Optional[str]
    display_name: typing.Optional[str]
    harmonized_name: typing.Optional[str]
    value: typing.Optional[str]
    unit: typing.Optional[str]


class BioSample(pydantic.BaseModel):
    accession: str
    id: str
    title: typing.Optional[str]
    description: typing.Optional[str]
    taxonomy_name: str
    taxon_id: int
    attribute_recs: list[AttrRecs]
    attributes: list[str]
    model: typing.Optional[str]
    id_recs: list[IdRecs]
    ids: list[str]
    sra_sample: typing.Optional[str]
    dbgap: typing.Optional[str]
    gsm: typing.Optional[str]
    publication_date: typing.Optional[datetime.datetime]
    last_update: typing.Optional[datetime.datetime]
    submission_date: typing.Optional[datetime.datetime]
    access: typing.Optional[str]


class SimplePublication(pydantic.BaseModel):
    db: typing.Optional[str]
    id: typing.Optional[str]
    pubdate: typing.Optional[datetime.datetime]


class ExternalLink(pydantic.BaseModel):
    url: typing.Optional[str]
    label: typing.Optional[str]
    category: typing.Optional[str]


class LocusTag(pydantic.BaseModel):
    biosample_id: typing.Optional[str]
    assembly_id: typing.Optional[str]
    value: str


class BioProject(pydantic.BaseModel):
    data_types: list[str] = []
    description: typing.Optional[str]
    accession: str
    name: typing.Optional[str]
    publications: list[SimplePublication] = []
    title: typing.Optional[str]
    external_links: list[ExternalLink] = []
    release_date: typing.Optional[str]
    locus_tags: list[LocusTag] = []


class BioSampleParser(object):
    """Parse a BioSample xml file.

    This is a generator that yields dict records.
    If you want to validate the records, set validate_with_schema to True
    and the generator will pass the records through pydantic models.
    """

    def __init__(self, fh: typing.IO, validate_with_schema: bool = False):
        """Initialize a new parser

        Args:
            fh (typing.IO): An open file-like object containing Biosample XML records.
        """
        self.fhandle = fh
        self.context = ET.iterparse(self.fhandle, events=("end",))
        self.validate_with_schema = validate_with_schema
        event, self.root = next(self.context)

    def __iter__(self):
        return self

    def __next__(self):
        for event, elem in self.context:
            if elem.tag == "BioSample":
                bios = dict()
                bios["is_reference"] = None
                for k, v in elem.items():
                    bios[k] = v
                bios["id_recs"] = []
                bios["ids"] = []
                bios["sra_sample"] = None
                bios["dbgap"] = None
                bios["gsm"] = None
                for id in elem.iterfind(".//Id"):
                    idrec = {
                        "db": id.get("db"),
                        "label": id.get("db_label"),
                        "id": id.text,
                    }
                    bios["ids"].append(idrec["id"])
                    bios["id_recs"].append(idrec)
                    # add xref fields for SRA, dbGaP, and GEO

                    if id.get("db") == "SRA":
                        bios["sra_sample"] = id.text
                    if id.get("db") == "dbGaP":
                        bios["dbgap"] = id.text
                    if id.get("db") == "GEO":
                        bios["gsm"] = id.text
                bios["title"] = elem.findtext(".//Description/Title")
                bios["description"] = elem.findtext(".//Description/Comment/Paragraph")
                organism = elem.find(".//Organism")
                bios["taxonomy_name"] = organism.get("taxonomy_name")
                bios["taxon_id"] = int(organism.get("taxonomy_id"))
                bios["attribute_recs"] = []
                bios["attributes"] = []
                for attribute in elem.findall(".//Attribute"):
                    attrec = attribute.attrib
                    attrec["value"] = attribute.text
                    bios["attribute_recs"].append(attrec)
                    try:
                        bios["attributes"].append(attrec["harmonized_name"])
                    except Exception:
                        bios["attributes"].append(attrec["attribute_name"])

                bios["model"] = elem.findtext(".//Model")
                # print(json.dumps(bios))
                # res = es.index(index="bioes", doc_type='biosample', id=bios['id'], body=bios)
                elem.clear()

                # allow this behavior to be turned off
                # as performance is better without validation
                if self.validate_with_schema:
                    return BioSample(**bios)
                else:
                    return bios
        raise StopIteration


def parse_bioproject_xml_element(element: Element) -> dict:
    """Parse a BioProject xml element

    Args:
        element (Element): An lxml.etree.Element

    Returns:
        dict: A BioProject dict.
    """
    projtop: Element = element.find("./Project")  # type: ignore
    d2 = {}
    d2["title"] = projtop.findtext("./Project/ProjectDescr/Title")
    d2["description"] = projtop.findtext("./Project/ProjectDescr/Description")
    d2["name"] = projtop.findtext("./Project/ProjectDescr/Name")
    archive_id = projtop.find("./Project/ProjectID/ArchiveID")
    d2["accession"] = archive_id.attrib["accession"]  # type: ignore
    pubs = []
    for pub in projtop.findall(".//Publication"):
        pubs.append(
            {
                "pubdate": pub.get("date", None),
                "id": pub.get("id", None),
                "db": re.sub("^e", "", pub.findtext("./DbType").replace("^e", "", 1)),  # type: ignore
            }
        )
    d2["publications"] = pubs
    ext_links = []
    for link in projtop.findall(".//ExternalLink"):
        ext_links.append(
            {
                "category": link.get("category", None),
                "label": link.get("label", None),
                "url": link.findtext("./URL"),
            }
        )
    data_types = []
    for datatype in projtop.findall(".//ProjectDataTypeSet"):
        data_types.append(datatype.findtext("./DataType"))
    d2["locus_tags"] = []
    for locustag in projtop.findall(".//LocusTagPrefix"):
        d2["locus_tags"].append(
            {
                "biosample_id": locustag.get("biosample_id", None),
                "assembly_id": locustag.get("assembly_id", None),
                "value": locustag.text,
            }
        )
    d2["release_date"] = projtop.findtext("./Project/ProjectDescr/ProjectReleaseDate")
    d2["data_types"] = data_types
    d2["external_links"] = ext_links
    return d2


class BioProjectParser(typing.Iterable):
    """Parse a BioProject xml file.

    If you want to validate the records, set validate_with_schema to True
    and the generator will pass the records through pydantic models, although
    this will be slower (by about 5x).

    The return values are simple dicts.
    """

    def __init__(self, fh: typing.IO, validate_with_schema: bool = False):
        """Initialize a BioProjectParser

        Args:
            fh (typing.IO): An open file-like object containing BioProject records.
        """
        self.fhandle = fh
        self.context = ET.iterparse(self.fhandle, events=("end",))
        self.validate_with_schema = validate_with_schema
        event, self.root = next(self.context)

    def __iter__(self):
        return self

    def __next__(self):
        for event, elem in self.context:
            if elem.tag == "Package":
                results = parse_bioproject_xml_element(elem)
                elem.clear()
                if self.validate_with_schema:
                    return BioProject(**results).dict()
                else:
                    return results

        raise StopIteration
