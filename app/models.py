from pydantic import BaseModel, ConfigDict, Field, model_validator


class Product(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: int
    title: str
    price_kopecks: int = Field(ge=0)
    stock: int = Field(ge=0)
    revision: int = Field(ge=1)


class ProductPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    price_kopecks: int | None = Field(default=None, ge=0, strict=True)
    stock: int | None = Field(default=None, ge=0, strict=True)

    @model_validator(mode="after")
    def require_change(self):
        if not self.model_dump(exclude_none=True):
            raise ValueError("Provide price_kopecks or stock")
        return self
