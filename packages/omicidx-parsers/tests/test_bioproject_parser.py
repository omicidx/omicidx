# Successfully parse a valid XML element with tag "Package"
def test_parse_valid_package_element():
    from omicidx.biosample import (
        BioProjectParser,
    )
    from io import StringIO

    # Mock XML content
    xml_content = """<root>
                        <Package>
                            <Project>
                            <Project>
                                <ProjectDescr>
                                    <Title>Test Title</Title>
                                    <Description>Test Description</Description>
                                    <Name>Test Name</Name>
                                    <ProjectReleaseDate>2023-01-01</ProjectReleaseDate>
                                </ProjectDescr>
                                <ProjectID>
                                    <ArchiveID accession="PRJNA12345"/>
                                </ProjectID>
                                <LocusTagPrefix biosample_id="SAMN12345" assembly_id="GCA_12345">Test Locus Tag</LocusTagPrefix>
                            </Project>
                            </Project>
                        </Package>
                     </root>"""
    mock_file = StringIO(xml_content)

    parser = BioProjectParser(mock_file, validate_with_schema=False)

    result = next(parser)

    assert result["title"] == "Test Title"
    assert result["description"] == "Test Description"
    assert result["name"] == "Test Name"
    assert result["accession"] == "PRJNA12345"
    assert result["release_date"] == "2023-01-01"
    assert result["locus_tags"][0]["biosample_id"] == "SAMN12345"
