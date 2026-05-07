from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, Index, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


# -- BioSample ----------------------------------------------------------------


class BioSample(Base):
    __tablename__ = "biosample"

    accession: Mapped[str] = mapped_column(Text, primary_key=True)
    sra_sample_id: Mapped[str | None] = mapped_column(Text)
    organism: Mapped[str | None] = mapped_column(Text)
    tax_id: Mapped[int | None] = mapped_column(Integer)
    submission_date: Mapped[date | None] = mapped_column(Date)
    last_update: Mapped[date | None] = mapped_column(Date)
    is_reference: Mapped[bool | None] = mapped_column(Boolean)
    data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    _loaded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), server_default="now()"
    )

    __table_args__ = (
        Index("ix_biosample_organism", "organism"),
        Index("ix_biosample_tax_id", "tax_id"),
        Index("ix_biosample_submission_date", "submission_date"),
        Index("ix_biosample_sra_sample_id", "sra_sample_id"),
    )


class BioProject(Base):
    __tablename__ = "bioproject"

    accession: Mapped[str] = mapped_column(Text, primary_key=True)
    name: Mapped[str | None] = mapped_column(Text)
    title: Mapped[str | None] = mapped_column(Text)
    release_date: Mapped[str | None] = mapped_column(Text)
    data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    _loaded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), server_default="now()"
    )


# -- SRA -----------------------------------------------------------------------


class SraStudy(Base):
    __tablename__ = "sra_study"

    accession: Mapped[str] = mapped_column(Text, primary_key=True)
    organism: Mapped[str | None] = mapped_column(Text)
    study_type: Mapped[str | None] = mapped_column(Text)
    title: Mapped[str | None] = mapped_column(Text)
    bioproject: Mapped[str | None] = mapped_column(Text)
    submission_date: Mapped[date | None] = mapped_column(Date)
    last_update: Mapped[date | None] = mapped_column(Date)
    data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    _loaded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), server_default="now()"
    )

    __table_args__ = (
        Index("ix_sra_study_organism", "organism"),
        Index("ix_sra_study_study_type", "study_type"),
        Index("ix_sra_study_submission_date", "submission_date"),
        Index("ix_sra_study_bioproject", "bioproject"),
    )


class SraSample(Base):
    __tablename__ = "sra_sample"

    accession: Mapped[str] = mapped_column(Text, primary_key=True)
    organism: Mapped[str | None] = mapped_column(Text)
    tax_id: Mapped[int | None] = mapped_column(Integer)
    biosample: Mapped[str | None] = mapped_column(Text)
    title: Mapped[str | None] = mapped_column(Text)
    submission_date: Mapped[date | None] = mapped_column(Date)
    last_update: Mapped[date | None] = mapped_column(Date)
    data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    _loaded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), server_default="now()"
    )

    __table_args__ = (
        Index("ix_sra_sample_organism", "organism"),
        Index("ix_sra_sample_tax_id", "tax_id"),
        Index("ix_sra_sample_biosample", "biosample"),
    )


class SraExperiment(Base):
    __tablename__ = "sra_experiment"

    accession: Mapped[str] = mapped_column(Text, primary_key=True)
    library_strategy: Mapped[str | None] = mapped_column(Text)
    library_source: Mapped[str | None] = mapped_column(Text)
    platform: Mapped[str | None] = mapped_column(Text)
    instrument_model: Mapped[str | None] = mapped_column(Text)
    sample_accession: Mapped[str | None] = mapped_column(Text)
    study_accession: Mapped[str | None] = mapped_column(Text)
    submission_date: Mapped[date | None] = mapped_column(Date)
    last_update: Mapped[date | None] = mapped_column(Date)
    data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    _loaded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), server_default="now()"
    )

    __table_args__ = (
        Index("ix_sra_experiment_library_strategy", "library_strategy"),
        Index("ix_sra_experiment_library_source", "library_source"),
        Index("ix_sra_experiment_platform", "platform"),
        Index("ix_sra_experiment_sample_accession", "sample_accession"),
        Index("ix_sra_experiment_study_accession", "study_accession"),
    )


class SraRun(Base):
    __tablename__ = "sra_run"

    accession: Mapped[str] = mapped_column(Text, primary_key=True)
    experiment_accession: Mapped[str | None] = mapped_column(Text)
    total_spots: Mapped[int | None] = mapped_column(Integer)
    total_bases: Mapped[int | None] = mapped_column(Integer)
    published: Mapped[date | None] = mapped_column(Date)
    last_update: Mapped[date | None] = mapped_column(Date)
    data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    _loaded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), server_default="now()"
    )

    __table_args__ = (
        Index("ix_sra_run_experiment_accession", "experiment_accession"),
        Index("ix_sra_run_published", "published"),
    )


# -- GEO -----------------------------------------------------------------------


class GEOSeries(Base):
    __tablename__ = "geo_series"

    accession: Mapped[str] = mapped_column(Text, primary_key=True)
    title: Mapped[str | None] = mapped_column(Text)
    organism: Mapped[str | None] = mapped_column(Text)
    series_type: Mapped[str | None] = mapped_column(Text)
    submission_date: Mapped[date | None] = mapped_column(Date)
    last_update: Mapped[date | None] = mapped_column(Date)
    data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    _loaded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), server_default="now()"
    )

    __table_args__ = (
        Index("ix_geo_series_organism", "organism"),
        Index("ix_geo_series_series_type", "series_type"),
        Index("ix_geo_series_submission_date", "submission_date"),
    )


class GEOSample(Base):
    __tablename__ = "geo_sample"

    accession: Mapped[str] = mapped_column(Text, primary_key=True)
    organism: Mapped[str | None] = mapped_column(Text)
    platform_id: Mapped[str | None] = mapped_column(Text)
    series_id: Mapped[str | None] = mapped_column(Text)
    title: Mapped[str | None] = mapped_column(Text)
    submission_date: Mapped[date | None] = mapped_column(Date)
    last_update: Mapped[date | None] = mapped_column(Date)
    data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    _loaded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), server_default="now()"
    )

    __table_args__ = (
        Index("ix_geo_sample_organism", "organism"),
        Index("ix_geo_sample_platform_id", "platform_id"),
        Index("ix_geo_sample_series_id", "series_id"),
    )


class GEOPlatform(Base):
    __tablename__ = "geo_platform"

    accession: Mapped[str] = mapped_column(Text, primary_key=True)
    title: Mapped[str | None] = mapped_column(Text)
    organism: Mapped[str | None] = mapped_column(Text)
    technology: Mapped[str | None] = mapped_column(Text)
    data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    _loaded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), server_default="now()"
    )


# -- PubMed --------------------------------------------------------------------


class PubMedArticle(Base):
    __tablename__ = "pubmed_article"

    pmid: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str | None] = mapped_column(Text)
    journal: Mapped[str | None] = mapped_column(Text)
    pub_date: Mapped[date | None] = mapped_column(Date)
    data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    _loaded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), server_default="now()"
    )

    __table_args__ = (
        Index("ix_pubmed_article_journal", "journal"),
        Index("ix_pubmed_article_pub_date", "pub_date"),
    )
