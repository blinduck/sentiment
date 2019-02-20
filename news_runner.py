from tasks import process_para, process_news


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


def get_sentiment_for_news(stmt):
    news = connection.execute(stmt)
    for a in news:
        process_news.delay(a.id)



if __name__ == '__main__':
    print('running main')
    # stmt = select([paras_table.c.text]).where(paras_table.c.news_id == 1244)
    # stmt = select([news]).where(news.c.id == 1244)
    stmt = select([news]).where(
        news.c.source=='mining-copper'
    ).order_by(
        news.c.id
    )
    get_sentiment_for_news(stmt)

    # process_news_articles(stmt, connection, paras_table, news, gs_client)
