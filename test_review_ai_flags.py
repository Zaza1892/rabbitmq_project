from unittest.mock import MagicMock,patch
import review_ai_flags

def test_only_flagged_rows_area_counted():

    fake_row=[
        ("msg-1","device-1","Everything looks normal.","2026-01-01"),
        ("msg-2","device-2","Looks broken and data missing.","2026-01-02"),
    ]

    with patch("review_ai_flags.psycopg2.connect") as test_connect:
        test_cursor=MagicMock()
        test_cursor.fetchall.return_value=fake_row
        test_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = test_cursor
review_ai_flags.main()