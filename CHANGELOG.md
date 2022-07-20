# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- SDAP-388: Enable SDAP to proxy/redirect to alternate SDAP
- SDAP-372: Updated matchup algorithm to point to AWS insitu API endpoint
- SDAP-372: Added new matchup endpoint `match_spark_doms` that points to DOMS insitu endpoint
- SDAP-372: Updated `match_spark_doms` to interface with samos_cdms endpoint 
- SDAP-393: Included `insitu` in ingress based on the value of `insituAPI.enabled` in `values.yaml`
- SDAP-371: Renamed `/domssubset` endpoint to `/cdmssubset`
- CDMS-122: Added new `ZarrProxy` to access Zarr-formatted data stored in AWS S3
  - Currently only supports single variable & gridded data and the `/matchup_spark` endpoint
  - Included unit tests
### Changed
### Deprecated
### Removed
### Fixed
- Fix failing test_matchup unit test
- Fixed null value for count in matchup response
- SDAP-371: Fixed DOMS subset endpoint
- SDAP-397: Added depth to matchup insitu output
- SDAP-397: Pull ID from insitu api response for matchup
- SDAP-397: Added null unit field for later use in matchup
- Fixed issue where satellite satellite matchups failed
- Fixed issue where null results were returned if more than "resultSizeLimit" matches are found
- Fixed issue where satellite to satellite matchups with the same dataset don't return the expected result
### Security