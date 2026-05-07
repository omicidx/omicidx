from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from omicidx.api.config import settings
from omicidx.api.db import get_session
from omicidx.api.models import SraExperiment, SraRun, SraSample, SraStudy
from omicidx.api.pagination import decode_cursor, encode_cursor
from omicidx.api.schemas.envelope import (
    Relationship,
    build_item_response,
    build_list_response,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()

Session = Annotated[AsyncSession, Depends(get_session)]


# -- Studies -------------------------------------------------------------------


@router.get("/studies/{accession}")
async def get_study(accession: str, session: Session):
    row = await session.get(SraStudy, accession)
    if not row:
        raise HTTPException(404, detail=f"SRA study {accession} not found")

    relationships = {}
    if row.bioproject:
        relationships["bioproject"] = Relationship(
            accession=row.bioproject, href=f"/v1/biosample/{row.bioproject}"
        )

    return build_item_response(item=row.data, relationships=relationships or None)


@router.get("/studies")
async def list_studies(
    session: Session,
    organism: str | None = None,
    study_type: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=settings.max_page_size)] = settings.default_page_size,
):
    stmt = select(SraStudy).order_by(SraStudy.accession).limit(limit + 1)

    if cursor:
        stmt = stmt.where(SraStudy.accession > decode_cursor(cursor).after)
    if organism:
        stmt = stmt.where(SraStudy.organism == organism)
    if study_type:
        stmt = stmt.where(SraStudy.study_type == study_type)
    if date_from:
        stmt = stmt.where(SraStudy.submission_date >= date_from)
    if date_to:
        stmt = stmt.where(SraStudy.submission_date <= date_to)

    result = await session.execute(stmt)
    rows = result.scalars().all()
    has_next = len(rows) > limit
    items = [r.data for r in rows[:limit]]
    next_cursor = encode_cursor(rows[limit - 1].accession) if has_next else None

    return build_list_response(
        items=items, path="/v1/sra/studies", limit=limit,
        next_cursor=next_cursor, cursor_param=cursor,
    )


# -- Samples -------------------------------------------------------------------


@router.get("/samples/{accession}")
async def get_sample(accession: str, session: Session):
    row = await session.get(SraSample, accession)
    if not row:
        raise HTTPException(404, detail=f"SRA sample {accession} not found")

    relationships = {}
    if row.biosample:
        relationships["biosample"] = Relationship(
            accession=row.biosample, href=f"/v1/biosample/{row.biosample}"
        )

    return build_item_response(item=row.data, relationships=relationships or None)


@router.get("/samples")
async def list_samples(
    session: Session,
    organism: str | None = None,
    tax_id: int | None = None,
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=settings.max_page_size)] = settings.default_page_size,
):
    stmt = select(SraSample).order_by(SraSample.accession).limit(limit + 1)

    if cursor:
        stmt = stmt.where(SraSample.accession > decode_cursor(cursor).after)
    if organism:
        stmt = stmt.where(SraSample.organism == organism)
    if tax_id is not None:
        stmt = stmt.where(SraSample.tax_id == tax_id)

    result = await session.execute(stmt)
    rows = result.scalars().all()
    has_next = len(rows) > limit
    items = [r.data for r in rows[:limit]]
    next_cursor = encode_cursor(rows[limit - 1].accession) if has_next else None

    return build_list_response(
        items=items, path="/v1/sra/samples", limit=limit,
        next_cursor=next_cursor, cursor_param=cursor,
    )


# -- Experiments ---------------------------------------------------------------


@router.get("/experiments/{accession}")
async def get_experiment(accession: str, session: Session):
    row = await session.get(SraExperiment, accession)
    if not row:
        raise HTTPException(404, detail=f"SRA experiment {accession} not found")

    relationships = {}
    if row.sample_accession:
        relationships["sample"] = Relationship(
            accession=row.sample_accession,
            href=f"/v1/sra/samples/{row.sample_accession}",
        )
    if row.study_accession:
        relationships["study"] = Relationship(
            accession=row.study_accession,
            href=f"/v1/sra/studies/{row.study_accession}",
        )

    return build_item_response(item=row.data, relationships=relationships or None)


@router.get("/experiments")
async def list_experiments(
    session: Session,
    library_strategy: str | None = None,
    library_source: str | None = None,
    platform: str | None = None,
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=settings.max_page_size)] = settings.default_page_size,
):
    stmt = select(SraExperiment).order_by(SraExperiment.accession).limit(limit + 1)

    if cursor:
        stmt = stmt.where(SraExperiment.accession > decode_cursor(cursor).after)
    if library_strategy:
        stmt = stmt.where(SraExperiment.library_strategy == library_strategy)
    if library_source:
        stmt = stmt.where(SraExperiment.library_source == library_source)
    if platform:
        stmt = stmt.where(SraExperiment.platform == platform)

    result = await session.execute(stmt)
    rows = result.scalars().all()
    has_next = len(rows) > limit
    items = [r.data for r in rows[:limit]]
    next_cursor = encode_cursor(rows[limit - 1].accession) if has_next else None

    return build_list_response(
        items=items, path="/v1/sra/experiments", limit=limit,
        next_cursor=next_cursor, cursor_param=cursor,
    )


# -- Runs ----------------------------------------------------------------------


@router.get("/runs/{accession}")
async def get_run(accession: str, session: Session):
    row = await session.get(SraRun, accession)
    if not row:
        raise HTTPException(404, detail=f"SRA run {accession} not found")

    relationships = {}
    if row.experiment_accession:
        relationships["experiment"] = Relationship(
            accession=row.experiment_accession,
            href=f"/v1/sra/experiments/{row.experiment_accession}",
        )

    return build_item_response(item=row.data, relationships=relationships or None)


@router.get("/runs")
async def list_runs(
    session: Session,
    experiment_accession: str | None = None,
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=settings.max_page_size)] = settings.default_page_size,
):
    stmt = select(SraRun).order_by(SraRun.accession).limit(limit + 1)

    if cursor:
        stmt = stmt.where(SraRun.accession > decode_cursor(cursor).after)
    if experiment_accession:
        stmt = stmt.where(SraRun.experiment_accession == experiment_accession)

    result = await session.execute(stmt)
    rows = result.scalars().all()
    has_next = len(rows) > limit
    items = [r.data for r in rows[:limit]]
    next_cursor = encode_cursor(rows[limit - 1].accession) if has_next else None

    return build_list_response(
        items=items, path="/v1/sra/runs", limit=limit,
        next_cursor=next_cursor, cursor_param=cursor,
    )
