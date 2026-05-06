# Enable dotenv loading to manage environment variables 
set dotenv-load := true

# Run `oidx sra extract`
[group('oidx')]
sra-extract:
	uv run oidx sra extract --dest s3://${OMICIDX_DATA_ROOT}/sra/raw

# Run `oidx geo extract`
[group('oidx')]
geo-extract:
	uv run oidx geo extract s3://${OMICIDX_DATA_ROOT}

# Run `oidx biosample extract`
[group('oidx')]
biosample-extract:
	uv run oidx biosample extract s3://${OMICIDX_DATA_ROOT}

# Run `oidx ebi_biosample extract`
[group('oidx')]
ebi-biosample-extract:
	uv run oidx ebi-biosample extract ${OMICIDX_DATA_ROOT}

# Run `oidx europepmc extract`
[group('oidx')]
europepmc-extract:
	uv run oidx europepmc extract ${OMICIDX_DATA_ROOT}

# Run `oidx icite extract`
[group('oidx')]
icite-extract:
    uv run oidx icite extract ${OMICIDX_DATA_ROOT}

# Run `oidx pubmed extract`
[group('oidx')]
pubmed-extract:
    uv run oidx pubmed extract s3://${OMICIDX_DATA_ROOT}



# oidx commands
[group('oidx')]
extract-all: geo-extract biosample-extract europepmc-extract pubmed-extract

# Run the data worker locally (requires wrangler login)
[group('worker')]
worker-dev:
    cd worker && npx wrangler dev --remote

# Deploy the data worker to Cloudflare
[group('worker')]
worker-deploy:
    cd worker && npx wrangler deploy
