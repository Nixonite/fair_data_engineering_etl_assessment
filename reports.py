from sqlalchemy import create_engine
import pandas as pd

def fetch_top_20_cand_by_amount_by_cycle(dbconn):
    query = """
    SELECT * FROM (
        SELECT
            grp.recipient_id,
            cand.first_last_party,
            ROW_NUMBER() OVER (PARTITION BY grp.cycle ORDER BY grp.total_donated DESC) AS rank,
            grp.cycle,
            grp.num_contributors,
            grp.total_donated,
            grp.avg_donation,
            grp.first_time_num_contrib,
            grp.first_time_avg_contrib,
            grp.first_time_total_contrib,
            grp.male_num_contrib,
            grp.male_total_contrib,
            grp.male_avg_contrib,
            grp.female_num_contrib,
            grp.female_total_contrib,
            grp.female_avg_contrib,
            grp.num_small_contrib,
            grp.small_contrib_total,
            grp.avg_small_contrib,
            grp.num_large_contrib,
            grp.large_contrib_total,
            grp.avg_large_contrib
            FROM general_reporting grp
            JOIN candidates cand
                ON grp.recipient_id = cand.cid
                AND grp.cycle = cand.cycle
            ORDER BY grp.cycle DESC
    ) t WHERE t.rank <= 20 ORDER BY t.cycle DESC, t.rank ASC;
    """
    return pd.read_sql(query, dbconn)

def fetch_top_20_committees_by_amount_by_cycle(dbconn):
    query = """
    SELECT * FROM (
        SELECT
            grp.recipient_id,
            ROW_NUMBER() OVER (PARTITION BY grp.cycle ORDER BY grp.total_donated DESC) AS rank,
            comm.pac_short,
            grp.cycle,
            grp.num_contributors,
            grp.total_donated,
            grp.avg_donation,
            grp.first_time_num_contrib,
            grp.first_time_avg_contrib,
            grp.first_time_total_contrib,
            grp.male_num_contrib,
            grp.male_total_contrib,
            grp.male_avg_contrib,
            grp.female_num_contrib,
            grp.female_total_contrib,
            grp.female_avg_contrib,
            grp.num_small_contrib,
            grp.small_contrib_total,
            grp.avg_small_contrib,
            grp.num_large_contrib,
            grp.large_contrib_total,
            grp.avg_large_contrib
            FROM general_reporting grp
            JOIN committees comm
                ON grp.recipient_id = comm.committee_id
                AND grp.cycle = comm.cycle
            ORDER BY grp.cycle DESC
    ) t WHERE t.rank <= 20 ORDER BY t.cycle DESC, t.rank ASC;

    """
    return pd.read_sql(query, dbconn)


if __name__=="__main__":
    dbconn = create_engine("postgresql:///campaign_finance")
    fetch_top_20_cand_by_amount_by_cycle(dbconn).to_csv("top_20_cand_by_amt.csv", index=False)
    fetch_top_20_committees_by_amount_by_cycle(dbconn).to_csv("top_20_committees_by_amt.csv", index=False)
