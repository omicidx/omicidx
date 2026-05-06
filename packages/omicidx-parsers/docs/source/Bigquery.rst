OmicIDX dataset on Bigquery
===========================

Bigquery_ is a cloud-based, fully-managed data warehouse and
relational database and analytical engine available from
Google. Bigquery is capable of storing and querying very large
datasets (think billions or even more rows, TB of data).

.. _Bigquery: https://bigquery.cloud.google.com/


The OmicIDX data are accessible as a *public* dataset on
Bigquery. For users who wish to use SQL for search, analytics, or
other analytical workflows, Bigquery provides a ready-made solution
that includes an online web-based query tool, a command-line tool, and
clients in many languages including R, Python, Go, and Java.

For OmicIDX, though, Bigquery offers additional capabilities
including:

* Export of large query results to Google cloud storage
* Joining OmicIDX with any other database tables available to the
  user, including private ones.
* Public data warehouse for publicly available genomics datasets
* Integration with other Google Cloud Platform services like machine
  learning, Dataflow, or natural language processing

.. note::
	   
   While OmicIDX data on Bigquery is public, accessing Bigquery
   requires a Google Cloud Platform account and an active `billing
   project`_. A `new account`_ comes with free credits.


.. _`billing project`: https://abc.com
.. _`new account`: https://abc.com




Bigquery OmicIDX tables
-----------------------

The OmicIDX data in Bigquery comprise a set of tables that mirror the
data model available from NCBI. 

========== ============== ================
Repository Table Name     Accessions like:
---------- -------------- ----------------
 sra       sra_study      SRP...
 sra       sra_sample     SRS...
 sra       sra_experiment SRX...  
 sra       sra_run        SRR...
 biosample biosample      SAMN...
========== ============== ================

Unlike in a traditional relational database, these tables include
"nested" columns.

