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

    @field_validator('symbol')
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        if not v.replace:
            raise ValueError("Symbol must be alphanumeric (allowing dots)")
        return v.upper()
    
    
class StockPriceFact(CSEBaseModel):
    symbol: str
    price: float = Field(..., gt=0) # Must be positive
    high: float = Field(..., ge=0)
    low: float = Field(..., ge=0)
    volume: int = Field(..., alias="sharevolume", ge=0)
    change_percentage: float = Field(..., alias="percentageChange")
    extracted_at: datetime = Field(default_factory=datetime.now)

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode='after')
    def check_high_low(self) -> 'StockPriceFact':
        if self.low > self.high:
            raise ValueError(f"Low price {self.low} cannot be higher than High price {self.high}")
        return self