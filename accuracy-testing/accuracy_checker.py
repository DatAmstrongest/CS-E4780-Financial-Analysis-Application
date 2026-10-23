import argparse

import numpy as np
import pandas as pd

SYSTEM_EMA_PATH = "./system/ema.csv"
ACTUAL_EMA_PATH = "./actual/ema.csv"

SYSTEM_BREAKOUT_PATH = "./system/breakout.csv"
ACTUAL_BREAKOUT_PATH = "./actual/breakout.csv"

KEY = ["stock_id", "dt"]


def load(path):
    df = pd.read_csv(path)
    df["dt"] = pd.to_datetime(df["dt"], format="ISO8601")
    return df


def flag_initial(breakouts, emas):
    """A stock's first window always looks like a bull crossover because both EMAs start at 0."""
    df = breakouts.merge(emas[KEY + ["prev_ema38", "prev_ema100"]], on=KEY, how="left")
    df["initial"] = (df["prev_ema38"] == 0) & (df["prev_ema100"] == 0)
    return df.drop(columns=["prev_ema38", "prev_ema100"])


def show(title, df, columns):
    if len(df):
        print(f"\n{title} (first 10 of {len(df)}):")
        print(df[columns].head(10).to_string(index=False))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--include-initial", action="store_true",
                        help="also score each stock's first-window bull breakout")
    args = parser.parse_args()

    actual_ema, system_ema = load(ACTUAL_EMA_PATH), load(SYSTEM_EMA_PATH)
    actual_breakouts, system_breakouts = load(ACTUAL_BREAKOUT_PATH), load(SYSTEM_BREAKOUT_PATH)

    # The replay may still be running, so only compare windows both sides have fully processed.
    cutoff = min(actual_ema["dt"].max(), system_ema["dt"].max()).floor("5min") - pd.Timedelta(minutes=5)
    actual_ema, system_ema = actual_ema[actual_ema["dt"] < cutoff], system_ema[system_ema["dt"] < cutoff]
    actual_breakouts = flag_initial(actual_breakouts[actual_breakouts["dt"] < cutoff], actual_ema)
    system_breakouts = flag_initial(system_breakouts[system_breakouts["dt"] < cutoff], system_ema)
    print(f"Comparing windows before {cutoff}")

    # EMA values
    ema = actual_ema.merge(system_ema, on=KEY, how="outer", suffixes=("_actual", "_system"), indicator=True)
    both = ema[ema["_merge"] == "both"]
    values_match = (np.isclose(both["ema38_actual"], both["ema38_system"], rtol=1e-9, atol=1e-9)
                    & np.isclose(both["ema100_actual"], both["ema100_system"], rtol=1e-9, atol=1e-9))
    print(f"\nEMA rows: reference={len(actual_ema)} system={len(system_ema)} "
          f"in both={len(both)} values equal={int(values_match.sum())}")
    print(f"  only in reference (missed by system): {int((ema['_merge'] == 'left_only').sum())}")
    print(f"  only in system (not in reference):    {int((ema['_merge'] == 'right_only').sum())}")
    show("EMA value mismatches", both[~values_match], KEY + ["ema38_actual", "ema38_system", "ema100_actual", "ema100_system"])

    # Breakouts
    initial = (int(actual_breakouts["initial"].sum()), int(system_breakouts["initial"].sum()))
    if not args.include_initial:
        actual_breakouts = actual_breakouts[~actual_breakouts["initial"]]
        system_breakouts = system_breakouts[~system_breakouts["initial"]]
    merged = actual_breakouts.merge(system_breakouts, on=KEY, how="outer", suffixes=("_actual", "_system"), indicator=True)
    same_type = merged["breakout_type_actual"] == merged["breakout_type_system"]
    tp = int(((merged["_merge"] == "both") & same_type).sum())
    wrong_type = int(((merged["_merge"] == "both") & ~same_type).sum())
    fp = int((merged["_merge"] == "right_only").sum()) + wrong_type
    fn = int((merged["_merge"] == "left_only").sum()) + wrong_type

    precision = tp / (tp + fp) if tp + fp else None
    recall = tp / (tp + fn) if tp + fn else None
    if precision is None or recall is None:
        f1 = None
    else:
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    fmt = lambda value: "n/a (no breakouts to score)" if value is None else f"{value:.4f}"
    excluded = "included" if args.include_initial else "excluded"
    print(f"\nBreakouts ({excluded} first-window breakouts: reference={initial[0]} system={initial[1]})")
    print(f"True Positives: {tp}\nFalse Positives: {fp}\nFalse Negatives: {fn}\nWrong type: {wrong_type}")
    print(f"Precision: {fmt(precision)}\nRecall: {fmt(recall)}\nF1 Score: {fmt(f1)}")
    show("False positives (system only)", merged[merged["_merge"] == "right_only"], KEY + ["breakout_type_system"])
    show("False negatives (reference only)", merged[merged["_merge"] == "left_only"], KEY + ["breakout_type_actual"])


if __name__ == "__main__":
    main()
