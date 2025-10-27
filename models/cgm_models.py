"""
CGM (Continuous Glucose Monitoring) API models.
"""
from enum import Enum
from typing import Optional, List
from pydantic import BaseModel, Field, ConfigDict

class CgmRollingWindowTypeEnum(str, Enum):
    ROLLING_14_DAYS = "ROLLING_14_DAYS"

class CgmTimeInRangeEnum(str, Enum):
    TIR = "TIR"
    TAR_LEVEL_1 = "TAR_LEVEL_1"
    TAR_LEVEL_2 = "TAR_LEVEL_2"
    TBR_LEVEL_1 = "TBR_LEVEL_1"
    TBR_LEVEL_2 = "TBR_LEVEL_2"

class CgmExcursionBlock(BaseModel):
    """
    Model for CGM excursion block.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    startLocal: Optional[str] = Field(default=None, description="Start local")
    endLocal: Optional[str] = Field(default=None, description="End local")
    durationMin: Optional[int] = Field(default=None, description="Duration in minutes")
    minMgDl: Optional[float] = Field(default=None, description="Minimum glucose in mg/dL")
    maxMgDl: Optional[float] = Field(default=None, description="Maximum glucose in mg/dL")
    meanMgDl: Optional[float] = Field(default=None, description="Mean glucose in mg/dL")
    direction: Optional[str] = Field(default=None, description="Direction of the excursion")


class CgmExcursionTrendResult(BaseModel):
    """
    Model for CGM excursion trend result.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    patientId: Optional[str] = Field(default=None, description="Patient ID")
    startDate: Optional[str] = Field(default=None, description="Start date")
    endDate: Optional[str] = Field(default=None, description="End date")
    templateCoverageDays: int = Field(default=0, description="Template coverage days")
    lookBackDays: int = Field(default=0, description="Look back days")
    excursions: Optional[List[CgmExcursionBlock]] = Field(default=None, description="Trend data from CGM excursion API")


class CgmRollingWindowTimeRangePercentage(BaseModel):
    """
    Model for CGM rolling window time range percentage.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    metric: Optional[CgmTimeInRangeEnum] = Field(default=None, description="Metric")
    upperLimit: Optional[float] = Field(default=None, description="Upper limit")
    lowerLimit: Optional[float] = Field(default=None, description="Lower limit")
    percentage: Optional[float] = Field(default=None, description="Percentage")


class CgmRollingWindow(BaseModel):
    """
    Model for CGM rolling window.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    startDate: Optional[str] = Field(default=None, description="Start date")
    endDate: Optional[str] = Field(default=None, description="End date")
    daysWorn: Optional[int] = Field(default=None, description="Days worn")
    percentTimeActive: Optional[float] = Field(default=None, description="Percent time active")
    averageGlucose: Optional[float] = Field(default=None, description="Average glucose")
    gmi: Optional[float] = Field(default=None, description="Gmi")
    gv: Optional[float] = Field(default=None, description="Gv")
    windowValid: Optional[bool] = Field(default=None, description="Window valid")
    timeRangePercentage: Optional[List[CgmRollingWindowTimeRangePercentage]] = Field(default=None, description="Time range percentage")


class CgmRollingStatsResponse(BaseModel):
    """
    Model for CGM rolling stats response.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    patientId: Optional[str] = Field(default=None, description="Patient ID")
    windowType: Optional[CgmRollingWindowTypeEnum] = Field(default=None, description="Window type")
    wearThresholdPercent: Optional[float] = Field(default=None, description="Wear threshold percent")
    windows: Optional[List[CgmRollingWindow]] = Field(default=None, description="Windows")


# Request models
class CgmExcursionTrendRequest(BaseModel):
    """
    Request model for CGM excursion trend API.
    """
    patientId: str = Field(description="Patient ID")


class CgmRollingStatsRequest(BaseModel):
    """
    Request model for CGM rolling stats API.
    """
    patientId: str = Field(description="Patient ID")
