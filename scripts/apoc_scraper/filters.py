from enum import Enum
from typing import NamedTuple


class YearEnum(str, Enum):
    """Valid Year filter values for APOC forms."""

    any = "Any"
    _2008 = "2008"
    _2009 = "2009"
    _2010 = "2010"
    _2011 = "2011"
    _2012 = "2012"
    _2013 = "2013"
    _2014 = "2014"
    _2015 = "2015"
    _2016 = "2016"
    _2017 = "2017"
    _2018 = "2018"
    _2019 = "2019"
    _2020 = "2020"
    _2021 = "2021"
    _2022 = "2022"
    _2023 = "2023"
    _2024 = "2024"
    _2025 = "2025"
    _2026 = "2026"
    _2027 = "2027"
    _2028 = "2028"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, int):
            return cls(str(value))
        return super()._missing_(value)

    def __repr__(self) -> str:
        return f"'{self.value}'"


class StatusEnum(str, Enum):
    """Valid Status filter values for APOC forms."""

    complete_not_amended = "Complete, Not Amended"
    all_complete_forms = "All Complete Forms"
    non_amended_only = "Non-Amended Only"
    amended_only = "Amended Only"
    non_amendments_only = "Non-Amendments Only"
    amendments_only = "Amendments Only"

    def __repr__(self) -> str:
        return f"'{self.value}'"


class ScrapeFilters(
    NamedTuple("ScrapeFilters", [("report_year", YearEnum), ("status", StatusEnum)])
):
    """Which filters to apply in the APOC search UI."""

    def __new__(
        cls,
        report_year: YearEnum | int | str = YearEnum.any,
        status: StatusEnum | str = StatusEnum.complete_not_amended,
    ):
        return super().__new__(cls, YearEnum(report_year), StatusEnum(status))
