from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import List, Dict, Optional


class Attribute(BaseModel):
    tag: Optional[str] = None
    value: Optional[str] = None


class Xref(BaseModel):
    db: Optional[str] = None
    id: Optional[str] = None


class Identifier(BaseModel):
    namespace: Optional[str] = None
    id: Optional[str] = None


class FileAlternative(BaseModel):
    url: Optional[str] = None
    free_egress: Optional[str] = None
    access_type: Optional[str] = None
    org: Optional[str] = None


class FileSet(BaseModel):
    cluster: str = "public"
    filename: Optional[str] = None
    url: Optional[str] = None
    size: int = 0
    date: datetime = None
    md5: Optional[str] = None
    sratoolkit: str = "1"
    alternatives: List[FileAlternative]


class BaseQualityCount(BaseModel):
    quality: int = 0
    count: int = 0


class BaseQualities(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    pass


class TaxCountEntry(BaseModel):
    rank: Optional[str] = None
    name: Optional[str] = None
    parent: Optional[int] = None
    total_count: int = 0
    self_count: int = 0
    tax_id: int


class TaxCountAnalysis(BaseModel):
    nspot_analyze: Optional[int] = None
    total_spots: Optional[int] = None
    mapped_spots: Optional[int] = None
    tax_counts: List[TaxCountEntry] = None


class RunRead(BaseModel):
    index: int
    count: int
    mean_length: float = 0.0
    sd_length: float = 0.0


class BaseCounts(BaseModel):  # (List[Dict[str, int]]):
    pass


class LiveList(BaseModel):
    lastupdate: datetime = None
    published: datetime = None
    received: datetime = None
    status: str = "live"
    insdc: bool = True


class SraRun(LiveList, BaseModel):
    alias: Optional[str] = None
    run_date: datetime = None
    run_center: Optional[str] = None
    center_name: Optional[str] = None
    accession: str
    total_spots: int = 0
    total_bases: int = 0
    size: int = 0
    load_done: bool = True
    published: datetime = None
    is_public: bool = True
    cluster_name: str = "public"
    static_data_available: str = "1"
    avg_length: float = 0.0
    experiment_accession: str
    attributes: List[Attribute] = None
    files: List[FileSet] = None
    qualities: BaseQualities = None
    base_counts: BaseCounts = None
    reads: List[RunRead] = None
    tax_analysis: TaxCountAnalysis = None


class SraStudy(LiveList, BaseModel):
    abstract: Optional[str] = None
    BioProject: Optional[str] = None
    Geo: Optional[str] = None
    accession: str
    alias: Optional[str] = None
    center_name: Optional[str] = None
    broker_name: Optional[str] = None
    description: Optional[str] = None
    study_type: Optional[str] = None
    title: Optional[str] = None
    identifiers: List[Identifier] = None
    attributes: List[Attribute] = None
    pubmed_ids: List[int] = None


class SraExperiment(LiveList, BaseModel):
    accession: str
    attributes: List[Attribute] = None
    alias: Optional[str] = None
    center_name: Optional[str] = None
    design: Optional[str] = None
    description: Optional[str] = None
    identifiers: List[Identifier] = None
    instrument_model: Optional[str] = None
    library_name: Optional[str] = None
    library_construction_protocol: Optional[str] = None
    library_layout_orientation: Optional[str] = None
    library_layout_length: float = None
    library_layout_sdev: float = None
    library_strategy: Optional[str] = None
    library_source: Optional[str] = None
    library_selection: Optional[str] = None
    library_layout: Optional[str] = None
    xrefs: List[Xref] = None
    platform: Optional[str] = None
    sample_accession: Optional[str] = None
    study_accession: Optional[str] = None
    title: Optional[str] = None


class SraSample(LiveList, BaseModel):
    accession: str
    geo: Optional[str] = None
    BioSample: Optional[str] = None
    title: Optional[str] = None
    alias: Optional[str] = None
    organism: Optional[str] = None
    taxon_id: Optional[int] = None
    description: Optional[str] = None
    identifiers: List[Identifier] = None
    attributes: List[Attribute] = None
    xrefs: List[Xref] = None


class FullSraRun(SraRun):
    experiment: Optional[SraExperiment] = None
    sample: Optional[SraSample] = None
    study: Optional[SraStudy] = None
