import click
import omicidx.biosample
from sd_cloud_utils.aws.sqs import SQS
import omicidx.sra_parsers as sp
import logging
import json
import datetime
import asyncio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('omicidx-cli')

@click.group('biosample')
def biosample():
    pass

@biosample.command('downloadxml')
def biosample_download():
    """Download the biosample_set.xml.gz file"""
    omicidx.biosample.download_biosample()

@biosample.command('xml2json')
@click.argument('xmlfile')
def biosample_to_json(xmlfile):
    """Convert the biosample xml file to json"""
    omicidx.biosample.biosample_to_json(xmlfile)

@click.group('sra')
#@click.pass_context
def sra():
    pass

@sra.command('download')
def sra_download():
    print('Downloading files')

@sra.command('entity2json')
@click.argument("entity")
def sra_entity_to_json(entity):
    print('convert one entity')

@sra.command('entities2json')
def sra_entities2json():
    print('convert everything at once')

@sra.command("load2postgres")
def sra_load2postgres():
    print('load and transform in postgresql')

@sra.command("load2bigquery")
def sra_load2bigquery():
    print("Load to bigquery")

@click.group(help='get SRA metadata as json')
@click.option('--config',
              help="config file")
def json_utils(config=None):
    pass


def dateconverter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()

def _srr_to_json(accession):
    models = sp.models_from_runbrowser(accession)
    if(not isinstance(models, dict)):
        return None
    res = {}
    for k in models.keys():
        res[k] = models[k].dict()
    print(json.dumps(res, default = dateconverter))

@json_utils.command(help='Convert SRR to json')
@click.option('--accession',
              help = "An SRA Run accession")
def srr_to_json(accession):
    _srr_to_json(accession)
    
@click.group()
def omicidx_cli():
    pass

@click.group()
def admin():
    pass

@admin.command()
@click.option('--queue')
def create_queue(queue):
    sqs = SQS(queuename=queue)
    sqs.get_queue()
    return True

@admin.command()
@click.option('--queue')
@click.option('--from_date')
@click.option('--to_date')
def send_message(queue, from_date, to_date):
    n = 0
    q = SQS(queue)
    msg = []
    for rec in sp.get_accession_list(
            from_date = from_date,
            to_date = to_date,
            type = 'RUN'
    ):
        msg.append(q.make_message(body=rec['Accession'], id=str(n)))
        n+=1
        if(n % 10 == 0):
            q.send_messages(msg)
            msg = []
            logger.info('sent {} messages'.format(n))
    q.send_messages(msg)
    logger.info('sent {} messages'.format(n))
    


async def produce(queue, queuename):
    while True:
        vals = SQS(queuename=queuename).receive_message()
        if(len(vals)==0):
            logger.info('nothing to retrieve')
            queue.close()
        for val in vals:
            logger.info('Message:' + val.body)
            await queue.put(val)


async def consume(queue):
    while True:
        # wait for an item from the producer
        val = await queue.get()
        
        # process the item
        models = sp.models_from_runbrowser(val.body)
        if(isinstance(models, dict)):
            res = {}
            for k in models.keys():
                res[k] = models[k].dict()
            val.delete()
            queue.task_done()
            print(json.dumps(res, default = dateconverter))
        else:
            # Notify the queue that the item has been processed
            val.delete()
            queue.task_done()


async def run(queuename):
    queue = asyncio.Queue(maxsize=10)
    # schedule the consumer
    consumer = asyncio.ensure_future(consume(queue))
    # run the producer and wait for completion
    await produce(queue, queuename)
    # wait until the consumer has processed all items
    await queue.join()
    # the consumer is still awaiting for an item, cancel it
    consumer.cancel()



@admin.command(help='write out livelist results to stdout')
@click.option('--from_date', default = "2001-01-01",
              help = "records from date (default '2001-01-01')")
@click.option('--to_date',default=None, type=str)
@click.option('--entity',
              help = 'type of record to return',
              type = click.Choice(['RUN', 'EXPERIMENT', 'SAMPLE', 'STUDY']),
              default="RUN")
@click.option('--count',
              help="records to retrieve in each call (default 1000)",
              default = 1000,
              type=int)
@click.option('--offset',
              help="start at offset (default 0)",
              default = 0,
              type=int)
@click.option('--header', flag_value='header',
              help="include header (default False)",
              default = False)
def livelist(from_date, to_date, entity, count, offset, header):
    ll = sp.LiveList(from_date = from_date,
                     to_date = to_date, offset = offset,
                     count = count, entity = entity)
    if(header):
        print("\t".join(row))
    for row in ll:
        print('\t'.join(row.values()))
    
                     

            
@admin.command()
@click.option('--queue')
def process_accessions(queue):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(queue))
    loop.close()



omicidx_cli.add_command(biosample)
omicidx_cli.add_command(sra)
omicidx_cli.add_command(json_utils)
omicidx_cli.add_command(admin)



if __name__ == '__main__':
    omicidx_cli()
