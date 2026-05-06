from datetime import date
from typing import Annotated

from pydantic import BaseModel, Field


class GEOBase(BaseModel):
    title: str
    status: str
    submission_date: date | None = None
    last_update_date: date | None = None


class GEOName(BaseModel):
    first: str | None = None
    middle: str | None = None
    last: str | None = None


class GEOContact(BaseModel):
    city: str | None = None
    name: GEOName | None = None
    email: str | None = None
    state: str | None = None
    address: str | None = None
    department: str | None = None
    country: str | None = None
    web_link: str | None = None
    institute: str | None = None
    zip_postal_code: str | None = None
    phone: str | None = None


class GEOPlatform(GEOBase):
    accession: str  # constr(regex="GPL[0-9]+")
    status: str
    _entity: str = "GPL"
    contact: GEOContact | None = None
    summary: str | None = None
    organism: str | None = None
    sample_id: list[str] | None = []  # List[constr(regex="GSM[0-9]+")] = []
    series_id: list[str] | None = []  # List[constr(regex="GSE[0-9]+")] = []
    technology: str | None = None
    description: str | None = None
    distribution: str | None = None
    manufacturer: list[str] = []
    data_row_count: int | None = None
    contributor: list[GEOName] = []
    relation: list[str] = []
    manufacture_protocol: str | None = None


class GEOCharacteristic(BaseModel):
    tag: str
    value: str | None = None  # there are apparently some of these


class GEOChannel(BaseModel):
    label: str | None = None
    taxid: list[int] = []
    molecule: str | None = None
    organism: str | None = None
    source_name: str | None = None
    label_protocol: str | None = None
    growth_protocol: str | None = None
    extract_protocol: str | None = None
    treatment_protocol: str | None = None
    characteristics: list[GEOCharacteristic] = []


class GEOSample(GEOBase):
    type: str
    anchor: str | None = None
    _entity: None
    contact: GEOContact | None = None
    description: str | None = None
    accession: Annotated[str, Field(pattern=r"GSM[0-9]+")]
    biosample: Annotated[str, Field(pattern=r"SAM[A-Z]+[0-9]+")] | None = None
    tag_count: int | None = None
    tag_length: float | None = None
    platform_id: Annotated[str, Field(pattern=r"GPL[0-9]+")]
    hyb_protocol: str | None = None
    channel_count: int = 0
    scan_protocol: str | None = None
    data_row_count: int = 0
    library_source: str | None = None
    overall_design: str | None = None
    sra_experiment: Annotated[str, Field(pattern=r"[DES]RX[0-9]+")] | None = None
    data_processing: str | None = None
    supplemental_files: list[str] = []
    channels: list[GEOChannel] = []
    contributor: list[GEOName] = []


class GEOSeries(GEOBase):
    accession: Annotated[str, Field(pattern=r"GSE[0-9]+")]
    subseries: list[Annotated[str, Field(pattern=r"GSE[0-9]+")]] = []
    bioprojects: list[Annotated[str, Field(pattern=r"PRJ[A-Z]+[0-9]+")]] = []
    sra_studies: list[Annotated[str, Field(pattern=r"[ESD]RP[0-9]+")]] = []
    _entity: str = "GSE"
    contact: GEOContact | None = None
    type: list[str] = []
    summary: str | None = None
    relation: list[str] = []
    pubmed_id: list[int] = []
    sample_id: list[Annotated[str, Field(pattern=r"GSM[0-9]+")]] = []
    sample_taxid: list[int] = []
    sample_organism: list[str] = []
    platform_id: list[Annotated[str, Field(pattern=r"GPL[0-9]+")]] = []
    platform_taxid: list[int] = []
    platform_organism: list[str] = []
    data_processing: str | None = None
    description: str | None = None
    supplemental_files: list[str] = []
    overall_design: str | None = None
    contributor: list[GEOName] = []
