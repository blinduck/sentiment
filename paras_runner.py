from tasks import process_para

from bs4 import BeautifulSoup
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types
from google.protobuf.json_format import MessageToJson
from sqlalchemy import Table, Column, Integer, String, MetaData, \
    ForeignKey, select, Float, Date, Time, join, exists, create_engine
from sqlalchemy.dialects.postgresql import JSON, JSONB

meta = MetaData()
engine = create_engine('postgresql+psycopg2://scrappyuser:password@localhost:5433/scrappy')
connection = engine.connect()
paras_table = Table('paras', meta, autoload=True, autoload_with=engine)
news = Table('news', meta, autoload=True, autoload_with=engine)
gs_client = language.LanguageServiceClient()


def extract_paragraphs_text(html):
    soup = BeautifulSoup(html, 'html.parser')
    paras = soup.find_all('p')
    paras = [p.text for p in paras if len(p.text) > 30]
    return paras


def process_news(news, para_tables, gs_client, pg_con):
    paras = extract_paragraphs_text(news.full_text)
    for i, f in enumerate(paras):
        process_para.delay(f, news.id)


def check_news_processed(connection, news_id, p_table):
    return connection.execute(select([p_table]).where(p_table.c.news_id == news_id)).scalar()


def process_news_articles(stmt, connection, p_table, n_table, gs_client):
    rq = connection.execute(stmt)
    for news in rq:
        already_processed = check_news_processed(connection, news.id, p_table)
        if already_processed:
            continue
        print('processing', news.id)
        try:
            process_news(news, p_table, gs_client, connection)
        except Exception as e:
            print(e)
            continue


# check_news_processed(connection, 1)
# stmt = select([news]).where(news.columns.source == 'mining-copper').order_by(news.c.id)



if __name__ == '__main__':
    print('running main')
    # stmt = select([paras_table.c.text]).where(paras_table.c.news_id == 1244)
    stmt = select([news]).where(news.c.id == 1244)
    process_news_articles(stmt, connection, paras_table, news, gs_client)
