from datetime import datetime

from pydantic import BaseModel


class Attribute(BaseModel):
    tag: str | None = None
    value: str | None = None


class Xref(BaseModel):
    db: str | None = None
    id: str | None = None


class Identifier(BaseModel):
    namespace: str | None = None
    id: str | None = None


class FileAlternative(BaseModel):
    url: str | None = None
    free_egress: str | None = None
    access_type: str | None = None
    org: str | None = None


class FileSet(BaseModel):
    cluster: str = "public"
    filename: str | None = None
    url: str | None = None
    size: int = 0
    date: datetime | None = None
    md5: str | None = None
    sratoolkit: str = "1"
    alternatives: list[FileAlternative] = []


class BaseQualityCount(BaseModel):
    quality: int = 0
    count: int = 0


class BaseQualities(BaseModel):
    pass


class TaxCountEntry(BaseModel):
    rank: str | None = None
    name: str | None = None
    parent: int | None = None
    total_count: int = 0
    self_count: int = 0
    tax_id: int


class TaxCountAnalysis(BaseModel):
    nspot_analyze: int | None = None
    total_spots: int | None = None
    mapped_spots: int | None = None
    tax_counts: list[TaxCountEntry] = []


class RunRead(BaseModel):
    index: int
    count: int
    mean_length: float = 0.0
    sd_length: float = 0.0


class BaseCounts(BaseModel):  # (List[Dict[str, int]]):
    pass


class LiveList(BaseModel):
    lastupdate: datetime | None = None
    published: datetime | None = None
    received: datetime | None = None
    status: str = "live"
    insdc: bool = True


class SraRun(LiveList, BaseModel):
    alias: str | None = None
    run_date: datetime | None = None
    run_center: str | None = None
    center_name: str | None = None
    accession: str
    total_spots: int = 0
    total_bases: int = 0
    size: int = 0
    load_done: bool = True
    published: datetime | None = None
    is_public: bool = True
    cluster_name: str = "public"
    static_data_available: str = "1"
    avg_length: float = 0.0
    experiment_accession: str
    attributes: list[Attribute] = []
    files: list[FileSet] = []
    qualities: BaseQualities | None = None
    base_counts: BaseCounts | None = None
    reads: list[RunRead] = []
    tax_analysis: TaxCountAnalysis | None = None


class SraStudy(LiveList, BaseModel):
    abstract: str | None = None
    BioProject: str | None = None
    Geo: str | None = None
    accession: str
    alias: str | None = None
    center_name: str | None = None
    broker_name: str | None = None
    description: str | None = None
    study_type: str | None = None
    title: str | None = None
    identifiers: list[Identifier] = []
    attributes: list[Attribute] = []
    pubmed_ids: list[int] = []


class SraExperiment(LiveList, BaseModel):
    accession: str
    attributes: list[Attribute] = None
    alias: str | None = None
    center_name: str | None = None
    design: str | None = None
    description: str | None = None
    identifiers: list[Identifier] = []
    instrument_model: str | None = None
    library_name: str | None = None
    library_construction_protocol: str | None = None
    library_layout_orientation: str | None = None
    library_layout_length: float | None = None
    library_layout_sdev: float | None = None
    library_strategy: str | None = None
    library_source: str | None = None
    library_selection: str | None = None
    library_layout: str | None = None
    xrefs: list[Xref] = []
    platform: str | None = None
    sample_accession: str | None = None
    study_accession: str | None = None
    title: str | None = None


class SraSample(LiveList, BaseModel):
    accession: str
    geo: str | None = None
    BioSample: str | None = None
    title: str | None = None
    alias: str | None = None
    organism: str | None = None
    taxon_id: int | None = None
    description: str | None = None
    identifiers: list[Identifier] = []
    attributes: list[Attribute] = []
    xrefs: list[Xref] = []


class FullSraRun(SraRun):
    experiment: SraExperiment | None = None
    sample: SraSample | None = None
    study: SraStudy | None = None
