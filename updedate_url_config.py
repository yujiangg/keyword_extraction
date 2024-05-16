from db import DBhelper
import pandas as pd

if __name__ == '__main__':
    q = f"""SELECT * FROM web_push.web_id_url_encoder_rule"""
    df = pd.DataFrame(DBhelper('jupiter_new').ExecuteSelect(q))
    DBhelper('dione_2').ExecuteSelect("TRUNCATE TABLE web_id_url_encoder_rule")
    DBhelper.ExecuteUpdatebyChunk(df, db='dione_2', table='web_id_url_encoder_rule', chunk_size=100000, is_ssh=False)
