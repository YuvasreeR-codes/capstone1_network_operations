import pandas as pd
from np.np2_UsageProcessor import UsageProcessor


# ============================================================
# Test data
# ============================================================

def create_test_data():

    data = {
        "datetime": [
            "2026-08-01 00:00:00",
            "2026-08-01 00:00:00",
            "2026-08-01 01:00:00",
            "2026-08-01 01:00:00"
        ],

        "CellID": [
            100,
            100,
            101,
            101
        ],

        "countrycode": [
            39,
            49,
            39,
            49
        ],

        "smsin": [
            10,
            20,
            15,
            25
        ],

        "smsout": [
            5,
            10,
            5,
            10
        ],

        "callin": [
            3,
            4,
            2,
            3
        ],

        "callout": [
            2,
            3,
            1,
            2
        ],

        "internet": [
            100,
            200,
            150,
            250
        ]
    }

    return pd.DataFrame(data)


# ============================================================
# 1. Test load_data()
# ============================================================

def test_load_data():

    test_df = create_test_data()

    processor = UsageProcessor(df=test_df)

    result = processor.load_data()

    assert result is not None
    assert len(result) == 4

    print("PASS: load_data()")


# ============================================================
# 2. Test clean_data()
# ============================================================

def test_clean_data():

    test_df = create_test_data()

    processor = UsageProcessor(df=test_df)

    processor.load_data()

    result = processor.clean_data()

    # Canonical columns should exist
    assert "timestamp" in result.columns
    assert "grid_id" in result.columns
    assert "country_code" in result.columns

    # Timestamp should be datetime
    assert pd.api.types.is_datetime64_any_dtype(
        result["timestamp"]
    )

    # No negative activity
    activity_cols = [
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity"
    ]

    assert (result[activity_cols] >= 0).all().all()

    print("PASS: clean_data()")


# ============================================================
# 3. Test derive_time_features()
# ============================================================

def test_derive_time_features():

    test_df = create_test_data()

    processor = UsageProcessor(df=test_df)

    processor.load_data()
    processor.clean_data()

    result = processor.derive_time_features()

    assert "date" in result.columns
    assert "hour" in result.columns
    assert "day_of_week" in result.columns

    assert result["hour"].tolist() == [
        0, 0, 1, 1
    ]

    print("PASS: derive_time_features()")


# ============================================================
# 4. Test aggregate_to_grid_time()
# ============================================================

def test_aggregate_to_grid_time():

    test_df = create_test_data()

    processor = UsageProcessor(df=test_df)

    processor.load_data()
    processor.clean_data()
    processor.derive_time_features()

    result = processor.aggregate_to_grid_time()

    # 4 raw rows should become 2 grid/hour rows
    assert len(result) == 2

    # Country code should no longer be part of the result
    assert "country_code" not in result.columns

    # Grid 100 at 00:00:
    # sms_in = 10 + 20 = 30
    row = result[
        (result["grid_id"] == 100) &
        (result["timestamp"] == "2026-08-01 00:00:00")
    ]

    assert row["sms_in"].iloc[0] == 30

    print("PASS: aggregate_to_grid_time()")


# ============================================================
# 5. Test derive_activity_features()
# ============================================================

def test_derive_activity_features():

    test_df = create_test_data()

    processor = UsageProcessor(df=test_df)

    processor.load_data()
    processor.clean_data()
    processor.derive_time_features()
    processor.aggregate_to_grid_time()

    result = processor.derive_activity_features()

    assert "total_sms" in result.columns
    assert "total_calls" in result.columns
    assert "total_activity" in result.columns

    # Grid 100 at 00:00:
    #
    # SMS = (10+20) + (5+10) = 45
    # Calls = (3+4) + (2+3) = 12
    # Internet = 100+200 = 300
    # Total = 45 + 12 + 300 = 357

    row = result[
        (result["grid_id"] == 100) &
        (result["timestamp"] == "2026-08-01 00:00:00")
    ]

    assert row["total_sms"].iloc[0] == 45
    assert row["total_calls"].iloc[0] == 12
    assert row["total_activity"].iloc[0] == 357

    print("PASS: derive_activity_features()")


# ============================================================
# 6. Test compute_kpis()
# ============================================================

def test_compute_kpis():

    test_df = create_test_data()

    processor = UsageProcessor(df=test_df)

    processor.load_data()
    processor.clean_data()
    processor.derive_time_features()
    processor.aggregate_to_grid_time()
    processor.derive_activity_features()

    result = processor.compute_kpis()

    assert "daily_summary" in result
    assert "grid_summary" in result
    assert "peak_timestamp" in result
    assert "peak_activity" in result
    assert "internet_share" in result

    assert len(result["daily_summary"]) == 1

    assert len(result["grid_summary"]) == 2

    assert result["peak_activity"] > 0

    print("PASS: compute_kpis()")


# ============================================================
# 7. Test export_summary()
# ============================================================

# def test_export_summary():

#     test_df = create_test_data()

#     processor = UsageProcessor(df=test_df)

#     processor.load_data()
#     processor.clean_data()
#     processor.derive_time_features()
#     processor.aggregate_to_grid_time()
#     processor.derive_activity_features()
#     processor.compute_kpis()

#     processor.export_summary()

#     # Verify exported files can be read
#     daily = pd.read_csv("daily_summary.csv")
#     grid = pd.read_csv("grid_summary.csv")
#     hourly = pd.read_csv("grid_hour_summary.csv")

#     assert len(daily) == 1
#     assert len(grid) == 2
#     assert len(hourly) == 2

#     print("PASS: export_summary()")


# ============================================================
# Run all tests
# ============================================================

if __name__ == "__main__":

    print("\n========== NP2 UNIT VALIDATION ==========\n")

    test_load_data()
    test_clean_data()
    test_derive_time_features()
    test_aggregate_to_grid_time()
    test_derive_activity_features()
    test_compute_kpis()
    # test_export_summary()

    print("\n==========================================")
    print("ALL NP2 TESTS PASSED")
    print("==========================================")