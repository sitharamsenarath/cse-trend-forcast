from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class CSEBaseModel(BaseModel):
    model_config = ConfigDict(
        str_strip_whitespace=True,
        coerse_numbers_to_strings=False
    )


class StockDimension(CSEBaseModel):
    symbol: str = Field(..., min_length=3, max_length=20, description="The ticker symbol")
    name: str = Field(..., min_length=1)
    sector: Optional[str] = "Unknown"
    isin: Optional[str] = None
    beta_value: Optional[float] = None
    market_cap_total: Optional[float] = Field(None, alias="marketCap")
    issued_quantity: Optional[int] = Field(None, alias="quantityIssued")
    last_updated: datetime = Field(default_factory=datetime.now)

    model_config = ConfigDict(populate_by_name=True)

    @field_validator('symbol')
    def validate_symbol(cls, v: str) -> str:
        if not v or len(v.strip()) == 0:
            raise ValueError("Symbol cannot be empty")
        return v.upper().strip()
    
    
class StockPriceFact(CSEBaseModel):
    symbol: str
    name: Optional[str] = None
    price: float = Field(..., gt=0) # Must be positive
    open_price: Optional[float] = Field(None, alias="open")
    high: float = Field(..., ge=0)
    low: float = Field(..., ge=0)
    prev_close: Optional[float] = Field(None, alias="previousClose")
    volume: int = Field(..., alias="sharevolume", ge=0)
    turnover: float = Field(..., alias="turnover", ge=0) 
    trade_count: int = Field(..., alias="tradevolume", ge=0)
    change_percentage: float = Field(..., alias="percentageChange")
    extracted_at: datetime = Field(default_factory=datetime.now)

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode='after')
    def check_high_low(self) -> 'StockPriceFact':
        if self.low > self.high:
            self.low = self.high
        return self
    

class SectorFact(CSEBaseModel):
    index_name: str = Field(..., alias="name")
    index_code: Optional[str] = Field(None, alias="indexCode")
    index_value: float = Field(..., alias="indexValue")
    sector_turnover: float = Field(0.0, alias="sectorTurnoverToday")
    sector_volume: Optional[int] = Field(0, alias="sectorVolumeToday")
    change_percentage: float = Field(0.0, alias="percentage")
    extracted_at: datetime = Field(default_factory=datetime.now)

    model_config = ConfigDict(populate_by_name=True)

    @field_validator('index_value', 'sector_turnover', mode='before')
    def handle_nulls(cls, v):
        # API sometimes returns null for turnover during holidays/weekends
        return v if v is not None else 0.0