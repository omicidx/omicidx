#!/usr/bin/env python
import argparse
import omicidx.sra_parsers
import json
import logging
import collections
from xml.etree import ElementTree as et



def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="""
    Transforms an SRA XML mirroring metadata file into
    corresponding JSON format files. JSON is line-delimited
    JSON (not an array).""")
    
    parser.add_argument('fname',
                        help = "The SRA xml filename")
    parser.add_argument('outfile',
                        help="the output filename where to write json records")

    opts = parser.parse_args()

    entity=""

    if('study' in opts.fname):
        entity = "STUDY"
        sra_parser = omicidx.sra_parsers.SRAStudyRecord

    if('run' in opts.fname):
        entity = "RUN"
        sra_parser = omicidx.sra_parsers.SRARunRecord

    if('sample' in opts.fname):
        entity = "SAMPLE"
        sra_parser = omicidx.sra_parsers.SRASampleRecord

    if('experiment' in opts.fname):
        entity = "EXPERIMENT"
        sra_parser = omicidx.sra_parsers.SRAExperimentRecord

    logger.info('configuration: {}'.format(str(opts)))
    logger.info('using {} entity type'.format(entity))
    logger.info('parsing {} records'.format(entity))
    n = 0
    with open(opts.outfile, 'w') as outfile:
        with omicidx.sra_parsers.open_file(opts.fname) as f:
            for event, element in et.iterparse(f):
                if(event == 'end' and element.tag == entity):
                    rec = sra_parser(element).data
                    n+=1
                    if((n % 100000)==0):
                        logger.info('parsed {} {} entries'.format(entity, n))
                    outfile.write(json.dumps(rec) + "\n")
                    element.clear()
            logger.info('parsed {} entity entries'.format(n))

if __name__ == '__main__':
    main()
