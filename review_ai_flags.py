import psycopg2
import os

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "device_lookups")
DB_USER = os.getenv("DB_USER", "appuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "apppassword")

flag_key_words = ["broken", "red flag", "issue", "missing", "suspicious"]


def main():
    """
    Connects to Postgres, reads every row with an AI analysis, and
    prints only the ones where the AI's response contains language
    suggesting something looked wrong, based on a simple keyword match.
    """

    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT message_id , device_id , ai_analysis , created_at FROM device_lookups WHERE ai_analysis IS NOT NULL ORDER BY created_at DESC"
            )
            rows = cur.fetchall()

        flagged_count = 0
        for message_id, device_id, ai_analysis, created_at in rows:
            analysis_lower = ai_analysis.lower()
            if any(keyword in analysis_lower for keyword in flag_key_words):
                flagged_count += 1
                print(f"\n[{created_at}] message_id={message_id} device_id={device_id}")
                print(ai_analysis)
                print("-" * 60)

        print(f"\n{flagged_count}flagged event(s) out of {len(rows)} total analyzed.")


if __name__ == "__main__":
    main()
