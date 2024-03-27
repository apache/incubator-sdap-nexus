# SDAP Docker image build helper

This script will help ease the process of building a full set of SDAP Docker images, whether building from an official ASF release, a release candidate, GitHub (any branch, official repo or forks), or 
the local filesystem (not yet implemented). Builds from ASF will also have their checksums and signatures checked, making this useful in the release process. You can also choose to not use build cache 
and can also push the images once they've been built.

## Images built

- `sdap-nexus-webapp`
- `sdap-collection-manager`
- `sdap-granule-ingester`
- `sdap-solr-cloud`
- `sdap-solr-cloud-init`

## Requirements

- Docker must be installed and running
- gpg
- git
- tar
- Python with the dependencies in `requirements.txt` installed

## Usage

Basic usage is simple:

```shell
python build.py
```

You will be prompted for all the information needed. There are, however, a couple extra options. Run `python build.py -h` to learn more on them.