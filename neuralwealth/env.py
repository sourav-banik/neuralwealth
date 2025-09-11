data_pipeline_env = dict(
    env='development',
    fred_api_key = "7a43e94049639e99eab6a3f5c3c5b128",
    twitter_bearer_token = "AAAAAAAAAAAAAAAAAAAAAHAV1AEAAAAAwpezFIdHnGhQpkE96fLvzDyEcoY%3Dq7KDog2MFiSqkzOu1JpQmT3e1fjqZ0cF01CilLpZFrRIoX3vCx",
    influxdb_url = "http://localhost:8086",
    influxdb_token = "FMfR3almj9B_9tlubDIaJbGrx3mR91McfNRKnd4Cg8R_8CIDSgYYrgFEIjRWzii-ZLLYEITdgchboPpIPH4uCQ==",
    influxdb_org =  "neuralwealth",
    influxdb_bucket = "neuralwealth"
)

ai_lab_env = dict(
    openai_sdk_base_url = "https://openrouter.ai/api/v1",
    open_ai_sdk_api_key = "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",
    llm_model = "deepseek/deepseek-chat-v3-0324",
    influxdb_url = "http://localhost:8086",
    influxdb_token = "FMfR3almj9B_9tlubDIaJbGrx3mR91McfNRKnd4Cg8R_8CIDSgYYrgFEIjRWzii-ZLLYEITdgchboPpIPH4uCQ==",
    influxdb_org =  "neuralwealth",
    influxdb_bucket = "neuralwealth",
    neo4j_uri = "neo4j://127.0.0.1:7687",
    neo4j_user = "neo4j",
    neo4j_password = "default_password"
)

portfolio_env = dict(
    constraints = {
        'max_position_size': 0.2,
        'sector_limits': {'technology': 0.3, 'healthcare': 0.25},
        'max_leverage': 1.0
    },
    broker_type = "paper",
    user_id = "123"
)