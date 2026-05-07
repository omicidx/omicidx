from omicidx.api.schemas.envelope import (
    Relationship,
    build_item_response,
    build_list_response,
)


def test_build_list_response_with_next():
    items = [{"accession": f"SAMN{i}"} for i in range(3)]
    result = build_list_response(
        items=items,
        path="/v1/biosample",
        limit=25,
        next_cursor="abc123",
        cursor_param=None,
    )
    assert result["meta"]["count"] == 3
    assert result["meta"]["cursor"]["next"] == "abc123"
    assert "cursor=abc123" in result["links"]["next"]
    assert result["links"]["self"] == "/v1/biosample?limit=25"


def test_build_list_response_no_next():
    items = [{"accession": "SAMN1"}]
    result = build_list_response(
        items=items,
        path="/v1/biosample",
        limit=25,
        next_cursor=None,
        cursor_param=None,
    )
    assert result["meta"]["cursor"]["next"] is None
    assert result["links"]["next"] is None


def test_build_list_response_with_cursor_param():
    result = build_list_response(
        items=[],
        path="/v1/biosample",
        limit=10,
        next_cursor=None,
        cursor_param="prev_token",
    )
    assert "cursor=prev_token" in result["links"]["self"]


def test_build_item_response_without_relationships():
    result = build_item_response(item={"accession": "SAMN1", "organism": "Homo sapiens"})
    assert result["data"]["accession"] == "SAMN1"
    assert "relationships" not in result


def test_build_item_response_with_relationships():
    rels = {
        "study": Relationship(accession="SRP000001", href="/v1/sra/studies/SRP000001"),
    }
    result = build_item_response(
        item={"accession": "SRR000001"},
        relationships=rels,
    )
    assert result["relationships"]["study"]["accession"] == "SRP000001"
    assert result["relationships"]["study"]["href"] == "/v1/sra/studies/SRP000001"
