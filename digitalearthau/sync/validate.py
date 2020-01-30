import logging
import os
import tempfile
from pathlib import Path

from compliance_checker.runner import ComplianceChecker, CheckSuite
from osgeo import gdal

from digitalearthau import paths

# prevent aux.xml write
os.environ["GDAL_PAM_ENABLED"] = "NO"

# The 'load' method actually loads it globally, not on the specific instance.
CHECK_SUITE = CheckSuite()
CHECK_SUITE.load_all_available_checkers()


def validate_dataset(md_path: Path, log: logging.Logger):
    base_path, all_files = paths.get_dataset_paths(md_path)

    for file in all_files:
        if file.suffix.lower() in ('.nc', '.tif'):
            if not validate_image(file, log):
                return False
    return True


def validate_image(file: Path, log: logging.Logger, compliance_check=False):
    try:
        storage_unit = gdal.Open(str(file), gdal.gdalconst.GA_ReadOnly)

        if compliance_check and storage_unit.GetDriver().ShortName == 'netCDF':
            is_compliant, errors_occurred = _compliance_check(file)

            if (not is_compliant) or errors_occurred:
                log.info("validate.compliance.fail", path=file)
                return False

        for subdataset in storage_unit.GetSubDatasets():
            if 'dataset' not in subdataset[0]:
                band = gdal.Open(subdataset[0], gdal.gdalconst.GA_ReadOnly)
                try:
                    band.GetRasterBand(1).GetStatistics(0, 1)
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


def _compliance_check(nc_path: Path, results_path: Path = None):
    """
    Run cf and adcc checks with normal strictness, verbose text format to stdout
    """
    # Specify a tempfile as a sink, as otherwise it will spew results into stdout.
    out_file = str(results_path) if results_path else tempfile.mktemp(prefix='compliance-log-')

    try:
        was_success, errors_occurred = ComplianceChecker.run_checker(
            ds_loc=str(nc_path),
            checker_names=['cf'],
            verbose=0,
            criteria='lenient',
            skip_checks=['check_dimension_order'],
            output_filename=out_file,
            output_format='text'
        )
    finally:
        if not results_path and os.path.exists(out_file):
            os.remove(out_file)

    return was_success, errors_occurred
