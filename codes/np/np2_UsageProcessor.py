import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class UsageProcessor:

    def __init__(self, file_path=None, df=None):
        self.file_path = file_path
        self.df = df
        self.clean_df = None
        self.grid_hour_df = None
        self.daily_summary = None
        self.grid_summary = None
        self.rejected_rows = 0
        self.nulls_handled = 0

    def load_data(self):
        if self.df is not None:
            logger.info("Using DataFrame provided by user.")
            return self.df
        if self.file_path is None:
            raise ValueError("Either file_path or df must be provided.")
        logger.info("Loading file: %s", self.file_path)
        self.df = pd.read_csv(self.file_path)
        logger.info("Loaded %d rows", len(self.df))
        return self.df
    
    # Clean data
    def clean_data(self):
        if self.df is None:
            raise ValueError("Call load_data() first.")
        df = self.df.copy()
        logger.info("Starting cleaning...")

        # Raw → canonical column names
        column_mapping = {"datetime": "timestamp", "CellID": "grid_id", "countrycode": "country_code", "smsin": "sms_in", "smsout": "sms_out", "callin": "call_in", "callout": "call_out", "internet": "internet_activity"}
        df = df.rename(columns=column_mapping)

        # Required columns
        required_columns = ["timestamp", "grid_id", "country_code", "sms_in", "sms_out", "call_in", "call_out", "internet_activity"]

        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Convert timestamp
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        # Convert activity columns
        activity_cols = ["sms_in", "sms_out", "call_in", "call_out", "internet_activity"]

        for col in activity_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        original_rows = len(df)

        # Reject missing grid_id / timestamp
        invalid_key_rows = df["grid_id"].isna() | df["timestamp"].isna()
        rejected_key_count = invalid_key_rows.sum()
        df = df[~invalid_key_rows].copy()

        # Reject negative activity values
        negative_rows = (df[activity_cols] < 0).any(axis=1)
        rejected_negative_count = negative_rows.sum()
        df = df[~negative_rows].copy()

        self.rejected_rows = rejected_key_count + rejected_negative_count

        # ----------------------------------------------------
        # Null activity handling
        # ----------------------------------------------------
        #
        # Activity blanks are converted to zero in the
        # curated/clean layer.
        #
        # We count how many null values were handled.
        # ----------------------------------------------------

        null_count = df[activity_cols].isna().sum().sum()
        self.nulls_handled = int(null_count)
        df[activity_cols] = df[activity_cols].fillna(0)

        self.clean_df = df

        logger.info("Original rows: %d", original_rows)
        logger.info("Rejected rows: %d", self.rejected_rows)
        logger.info("Activity nulls handled: %d", self.nulls_handled)
        logger.info("Clean rows: %d", len(df))

        return self.clean_df

    # --------------------------------------------------------
    # 3. Derive time features
    # --------------------------------------------------------

    def derive_time_features(self):
        if self.clean_df is None:
            raise ValueError("Call clean_data() first.")
        df = self.clean_df.copy()
        df["date"] = df["timestamp"].dt.date
        df["hour"] = df["timestamp"].dt.hour
        df["day_of_week"] = df["timestamp"].dt.day_name()
        self.clean_df = df
        logger.info("Time features created.")
        return self.clean_df

    # --------------------------------------------------------
    # 4. Aggregate to grid + hour
    # --------------------------------------------------------

    def aggregate_to_grid_time(self):
        if self.clean_df is None:
            raise ValueError("Run clean_data() and derive_time_features() first.")
        df = self.clean_df

        activity_cols = ["sms_in", "sms_out", "call_in", "call_out", "internet_activity"]

        # IMPORTANT:
        # country_code is intentionally NOT included.
        # This changes the grain from:
        # timestamp + grid_id + country_code
        # timestamp + grid_id

        grid_hour = (df.groupby(["timestamp", "grid_id"], as_index=False)[activity_cols].sum())
        self.grid_hour_df = grid_hour
        #to avoid datetime issue
        self.grid_hour_df["timestamp"] = pd.to_datetime(
                self.grid_hour_df["timestamp"]
                )
        
        logger.info("Grid/hour rows created: %d", len(grid_hour))
        return self.grid_hour_df

    # --------------------------------------------------------
    # Derive activity features
    # --------------------------------------------------------

    def derive_activity_features(self):
        if self.grid_hour_df is None:
            raise ValueError("Call aggregate_to_grid_time() first.")
        df = self.grid_hour_df.copy()

        df["total_sms"] = df["sms_in"] + df["sms_out"]
        df["total_calls"] = df["call_in"] + df["call_out"]
        df["total_activity"] = df["total_sms"] + df["total_calls"] + df["internet_activity"]

        # Time features for the aggregated data
        df["date"] = df["timestamp"].dt.date
        df["hour"] = df["timestamp"].dt.hour
        df["day_of_week"] = df["timestamp"].dt.day_name()

        self.grid_hour_df = df
        logger.info("Activity features created.")
        return self.grid_hour_df

    # --------------------------------------------------------
    # Compute KPIs
    # --------------------------------------------------------

    def compute_kpis(self):
        if self.grid_hour_df is None:
            raise ValueError("Call derive_activity_features() first.")

        df = self.grid_hour_df

        # ----------------------------------------------------
        # Daily summary
        # ----------------------------------------------------

        self.daily_summary = (
            df.groupby("date")
            .agg(
                total_sms=("total_sms", "sum"),
                total_calls=("total_calls", "sum"),
                internet_activity=("internet_activity", "sum"),
                total_activity=("total_activity", "sum"),
                active_grids=("grid_id", "nunique")
            )
            .reset_index()
        )

        # ----------------------------------------------------
        # Grid summary
        # ----------------------------------------------------

        self.grid_summary = (
            df.groupby("grid_id")
            .agg(
                total_sms=("total_sms", "sum"),
                total_calls=("total_calls", "sum"),
                internet_activity=("internet_activity", "sum"),
                total_activity=("total_activity", "sum"),
                active_hours=("timestamp", "nunique")
            )
            .reset_index()
            .sort_values(
                "total_activity",
                ascending=False
            )
        )

        # ----------------------------------------------------
        # Top 10 grids
        # ----------------------------------------------------

        top_10_grids = self.grid_summary.head(10)
        print("\n========== TOP 10 GRIDS ==========")
        print(top_10_grids[["grid_id", "total_activity", "active_hours"]].to_string(index=False))

        # ----------------------------------------------------
        # Peak activity hour
        # ----------------------------------------------------

        hourly_summary = df.groupby("timestamp")["total_activity"].sum().sort_values(ascending=False)
        peak_timestamp = hourly_summary.index[0]
        peak_activity = hourly_summary.iloc[0]

        print("\n========== PEAK ACTIVITY ==========")
        print("Peak timestamp:", peak_timestamp)
        print("Peak activity:", peak_activity)

        # ----------------------------------------------------
        # Internet share
        # ----------------------------------------------------

        total_activity = df["total_activity"].sum()
        total_internet = df["internet_activity"].sum()

        if total_activity > 0:
            internet_share = total_internet / total_activity
        else:
            internet_share = 0

        print("\n========== KPI SUMMARY ==========")
        print("Total SMS activity:", df["total_sms"].sum())
        print("Total call activity:", df["total_calls"].sum())
        print("Total internet activity:", df["internet_activity"].sum())
        print("Total activity:", total_activity)
        print("Active grids:", df["grid_id"].nunique())
        print("Internet share:", round(internet_share, 4))

        return {"daily_summary": self.daily_summary, "grid_summary": self.grid_summary, "peak_timestamp": peak_timestamp, "peak_activity": peak_activity, "internet_share": internet_share}

    # --------------------------------------------------------
    # 7. Export summaries
    # --------------------------------------------------------

    def export_summary(self):
        if self.daily_summary is None:
            raise ValueError("Call compute_kpis() first.")

        self.daily_summary.to_csv("output/daily_summary.csv", index=False)
        self.grid_summary.to_csv("output/grid_summary.csv", index=False)
        self.grid_hour_df["timestamp"] = pd.to_datetime(
            self.grid_hour_df["timestamp"]
        ).dt.strftime("%Y-%m-%d %H:%M:%S")
        self.grid_hour_df.to_csv("output/grid_hour_summary.csv", index=False)

        logger.info("daily_summary.csv created.")
        logger.info("grid_summary.csv created.")
        logger.info("grid_hour_summary.csv created.")

# MAIN
if __name__ == "__main__":
    processor = UsageProcessor(file_path=r"data\landing\sms-call-internet-mi-2013-11-01.csv")
    processor.load_data()
    processor.clean_data()
    processor.derive_time_features()
    processor.aggregate_to_grid_time()
    processor.derive_activity_features()
    processor.compute_kpis()
    processor.export_summary()
    print("\nNP2 COMPLETE")