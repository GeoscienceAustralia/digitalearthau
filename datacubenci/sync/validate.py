import logging
from pathlib import Path

import gdal
from compliance_checker.runner import ComplianceChecker, CheckSuite

from datacubenci import paths

# Is this necessary? Copied from Simon's checker

CHECK_SUITE = CheckSuite()
CHECK_SUITE.load_all_available_checkers()


def validate_dataset(md_path: Path, log: logging.Logger):
    base_path, all_files = paths.get_dataset_paths(md_path)

    for file in all_files:
        if file.suffix.lower() in ('.nc', '.tif'):
            try:
                storage_unit = gdal.Open(str(file), gdal.gdalconst.GA_ReadOnly)

                if storage_unit.GetDriver().ShortName == 'netCDF':
                    is_compliant, errors_occurred = _compliance_check(file)

                    if (not is_compliant) or errors_occurred:
                        log.info("validate.compliance.fail", path=file)
                        return False

                for subdataset in storage_unit.GetSubDatasets():
                    if 'dataset' not in subdataset[0]:
                        band = gdal.Open(subdataset[0], gdal.gdalconst.GA_ReadOnly)
                        try:
                            band.GetRasterBand(1).GetStatistics(0, 1)
                            logging.info("Band is OK %s", subdataset[0])
                            log.info("validate.band.pass", path=file)
                        except ValueError as v:
                            # Only show stack trace at debug-level logging. We get the message at info.
                            log.debug("validate.band.exception", exc_info=True)
                            log.info("validate.band.fail", path=file, error_args=v.args)
                            return False
            except ValueError as v:
                # Only show stack trace at debug-level logging. We get the message at info.
                log.debug("validate.band.exception", exc_info=True)
                log.info("validate.open.fail", path=file, error_args=v.args)
                return False
    return True


def _compliance_check(nc_path: Path):
    """
    Run cf and adcc checks with normal strictness, verbose text format to stdout
    """
    was_success, errors_occurred = ComplianceChecker.run_checker(
        str(nc_path),
        ['cf'],
        0,
        'lenient',
        '-',
        'text'
    )
    return was_success, errors_occurred
