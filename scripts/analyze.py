import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


PATH = "data/raw/fred_monthly_master.parquet"


def main():
    print("\n[LOAD]")
    df = pd.read_parquet(PATH)

    # --------------------------------------------------
    # BASIC INFO
    # --------------------------------------------------
    print("\n[SHAPE]")
    n_rows, n_cols = df.shape
    print(f"Rows: {n_rows}, Columns: {n_cols}")

    print("\n[DATE RANGE]")
    print(df.index.min(), "→", df.index.max())

    print("\n[DTYPES]")
    print(df.dtypes.value_counts())

    # --------------------------------------------------
    # MISSINGNESS
    # --------------------------------------------------
    print("\n[MISSINGNESS SUMMARY]")

    missing_ratio = df.isna().mean()

    print(f"Avg missing: {missing_ratio.mean():.4f}")
    print(f"Median missing: {missing_ratio.median():.4f}")

    print("% columns >50% missing:", (missing_ratio > 0.5).mean())
    print("% columns >80% missing:", (missing_ratio > 0.8).mean())

    print("\nTop 10 worst:")
    print(missing_ratio.sort_values(ascending=False).head(10))

    print("\nTop 10 best:")
    print(missing_ratio.sort_values().head(10))

    # --------------------------------------------------
    # OBSERVATION COUNTS
    # --------------------------------------------------
    counts = df.notna().sum()

    print("\n[OBS COUNTS]")
    print(f"Avg obs: {counts.mean():.1f}")
    print(f"Median obs: {counts.median():.1f}")
    print(f"Min obs: {counts.min()}")
    print(f"Max obs: {counts.max()}")

    # --------------------------------------------------
    # VARIANCE
    # --------------------------------------------------
    variances = df.var(numeric_only=True)

    print("\n[VARIANCE]")
    print(f"Avg variance: {variances.mean():.4f}")
    print(f"Median variance: {variances.median():.4f}")

    zero_var = (variances == 0).sum()
    print(f"Constant series: {zero_var}")

    # --------------------------------------------------
    # CORRELATION (subset)
    # --------------------------------------------------
    print("\n[CORRELATION STATS]")

    subset = df.iloc[:, :100].fillna(0)
    corr = subset.corr().values

    upper = corr[np.triu_indices_from(corr, k=1)]

    print(f"Mean corr: {np.mean(upper):.4f}")
    print(f"Median corr: {np.median(upper):.4f}")
    print(f"% >0.9: {(np.abs(upper) > 0.9).mean():.4f}")

    # --------------------------------------------------
    # DENSITY OVER TIME
    # --------------------------------------------------
    print("\n[DATA DENSITY OVER TIME]")

    density = df.notna().mean(axis=1)

    print(f"Avg density: {density.mean():.4f}")
    print(f"Min density: {density.min():.4f}")
    print(f"Max density: {density.max():.4f}")

    # --------------------------------------------------
    # RECOMMENDATIONS
    # --------------------------------------------------
    print("\n[RECOMMENDATIONS]")

    keep_cols = counts >= 200
    print(f"Cols >=200 obs: {keep_cols.sum()} / {n_cols}")

    low_missing = missing_ratio < 0.5
    print(f"Cols <50% missing: {low_missing.sum()} / {n_cols}")

    print("\nSuggested cleaning:")
    print("df = df.loc[:, df.notna().sum() >= 200]")

    # --------------------------------------------------
    # PLOTS
    # --------------------------------------------------

    # Missingness heatmap
    step = max(1, df.shape[1] // 300)
    sampled = df.iloc[:, ::step]

    plt.figure()
    plt.imshow(sampled.isna(), aspect="auto")
    plt.title("Missingness structure (FRED)")
    plt.tight_layout()
    plt.show()

    # Variance histogram
    plt.figure()
    plt.hist(variances.dropna(), bins=100)
    plt.title("Variance distribution (FRED)")
    plt.tight_layout()
    plt.show()

    # Obs count histogram
    plt.figure()
    plt.hist(counts, bins=100)
    plt.title("Observations per series (FRED)")
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()