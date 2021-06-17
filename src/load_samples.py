
from parquetranger import TableRepo
from .create_samples import sample_dir, COV_DIRNAME, NON_COV_DIRNAME, sample_recsize

def _get_cov_day(is_covid=False, day=0):
    subdir = COV_DIRNAME if is_covid else NON_COV_DIRNAME
    return TableRepo(sample_dir / subdir / str(day), max_records=sample_recsize, mkdirs=False)


covid_monday = _get_cov_day(is_covid=True, day=0)
non_covid_monday = _get_cov_day(is_covid=False, day=0)
covid_tuesday = _get_cov_day(is_covid=True, day=1)
non_covid_tuesday = _get_cov_day(is_covid=False, day=1)
covid_wednesday = _get_cov_day(is_covid=True, day=2)
non_covid_wednesday = _get_cov_day(is_covid=False, day=2)
covid_thursday = _get_cov_day(is_covid=True, day=3)
non_covid_thursday = _get_cov_day(is_covid=False, day=3)
covid_friday = _get_cov_day(is_covid=True, day=4)
non_covid_friday = _get_cov_day(is_covid=False, day=4)
covid_saturday = _get_cov_day(is_covid=True, day=5)
non_covid_saturday = _get_cov_day(is_covid=False, day=5)
covid_sunday = _get_cov_day(is_covid=True, day=6)
non_covid_sunday = _get_cov_day(is_covid=False, day=6)
