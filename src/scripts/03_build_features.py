import pandas as pd
from src.features.feature_pipeline import build_features

IN_FILE = "data/BTCUSDT_1m.csv"
OUT_FILE = "data/BTCUSDT_1m_features.csv"

if __name__ == "__main__":
    df = pd.read_csv(IN_FILE, parse_dates=["open_time","close_time"])
    df_feat = build_features(df)
    df_feat.to_csv(OUT_FILE, index=False)
    print(f"OK: features salvas em {OUT_FILE} (linhas: {len(df_feat)})")
