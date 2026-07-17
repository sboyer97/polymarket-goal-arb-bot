from pydantic_settings import BaseSettings
from pydantic import Field, field_validator
from typing import Optional


class PolymarketSettings(BaseSettings):
    api_key: str = Field(default="", env="POLYMARKET_API_KEY")
    api_secret: str = Field(default="", env="POLYMARKET_API_SECRET")
    api_passphrase: str = Field(default="", env="POLYMARKET_API_PASSPHRASE")
    private_key: str = Field(default="", env="POLYMARKET_PRIVATE_KEY")
    smart_wallet: str = Field(default="", env="POLYMARKET_SMART_WALLET")  # Magic: profile address (funder)
    chain_id: int = Field(default=137, env="POLYMARKET_CHAIN_ID")  # Polygon
    
    clob_api_url: str = "https://clob.polymarket.com"
    gamma_api_url: str = "https://gamma-api.polymarket.com"
    
    class Config:
        env_prefix = "POLYMARKET_"


class DataProviderSettings(BaseSettings):
    provider_type: str = Field(default="allsportsapi", env="DATA_PROVIDER_TYPE")
    api_key: str = Field(default="", env="DATA_PROVIDER_API_KEY")
    websocket_url: Optional[str] = Field(default=None, env="DATA_PROVIDER_WS_URL")
    
    class Config:
        env_prefix = "DATA_PROVIDER_"


class TradingSettings(BaseSettings):
    bet_amount_usdc: float = Field(default=5.0)  # env: TRADING_BET_AMOUNT_USDC
    max_position_size_usdc: float = Field(default=100.0, env="MAX_POSITION_SIZE")
    min_edge_percent: float = Field(default=2.0, env="MIN_EDGE_PERCENT")
    exit_delay_seconds: float = Field(default=5.0, env="EXIT_DELAY_SECONDS")
    exit_after_seconds: float = Field(default=60.0, env="EXIT_AFTER_SECONDS")  # live time-exit (s) - 60s captures the fast repricing
    max_slippage_percent: float = Field(default=1.0, env="MAX_SLIPPAGE_PERCENT")
    take_profit_pct: float = Field(default=5.0, env="TAKE_PROFIT_PCT")   # +5% → exit (ratio 1:1)
    stop_loss_pct: float = Field(default=-5.0, env="STOP_LOSS_PCT")      # -5% → exit (ratio 1:1)
    min_minute_to_bet: float = Field(default=0, env="MIN_MINUTE_TO_BET")  # 0 = all goals, 75 = late goals only
    min_liquidity_usd: float = Field(default=50.0, env="MIN_LIQUIDITY_USD")  # min liquidity to enter
    
    dry_run: bool = Field(default=True, env="DRY_RUN")

    @field_validator("dry_run", mode="before")
    @classmethod
    def parse_dry_run(cls, v):
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            return v.strip().lower() not in ("false", "0", "no", "off", "")
        return bool(v)

    class Config:
        env_prefix = "TRADING_"


class Settings(BaseSettings):
    polymarket: PolymarketSettings = PolymarketSettings()
    data_provider: DataProviderSettings = DataProviderSettings()
    trading: TradingSettings = TradingSettings()
    
    log_level: str = Field(default="INFO", env="LOG_LEVEL")


settings = Settings()
