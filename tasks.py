from celery import Celery
from bs4 import BeautifulSoup
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types
from google.protobuf.json_format import MessageToJson
from sqlalchemy import Table, Column, Integer, String, MetaData, \
    ForeignKey, select, Float, Date, Time, join, exists, create_engine
from sqlalchemy.dialects.postgresql import JSON, JSONB
from celery.signals import worker_process_init, worker_process_shutdown


broker_url = 'redis://localhost:6379/0'
app = Celery('tasks', broker=broker_url)

engine = create_engine('postgresql+psycopg2://scrappyuser:password@localhost:5433/scrappy')
# connection = engine.connect()
# connection = engine.connect()
gs_client = language.LanguageServiceClient()

connection = None
news = None


@worker_process_init.connect
def init_worker(**kwargs):
    global connection
    global news
    print('Initializing database connection for worker.')
    connection = engine.connect()
    meta = MetaData()
    news = Table('news', meta, autoload=True, autoload_with=engine)
    paras_table = Table('paras', meta, autoload=True, autoload_with=engine)


@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    global connection
    if connection:
        print('Closing database connectionn for worker.')
        connection.close()



def get_sentiment(para, gs_client):
    document = types.Document(content=para, type=enums.Document.Type.PLAIN_TEXT)
    resp = gs_client.annotate_text(document, features = {
        'extract_entity_sentiment': True,
        'extract_document_sentiment': True,
        }, encoding_type='UTF32')
    return resp

@app.task
def process_news(news_id):
    stmt = select([news]).where(news.c.id == int(news_id))
    article = connection.execute(stmt).fetchone()
    raw_resp  = get_sentiment(article.full_text, gs_client)
    doc_sent = raw_resp.document_sentiment
    update_stmt = news.update().where(
        news.c.id == int(news_id)
    ).values(gs_score = doc_sent.score, gs_magnitude = doc_sent.magnitude)
    connection.execute(update_stmt)
    return (article.id, doc_sent.score)


@app.task
def process_para(para_text, news_id):
    raw_resp  = get_sentiment(para_text, gs_client)
    doc_sent = raw_resp.document_sentiment
    stmt = paras_table.insert().values(
        news_id=news_id,
        text=para_text,
        gs_raw_resp=MessageToJson(raw_resp),
        gs_magnitude=doc_sent.magnitude,
        gs_score=doc_sent.score
    )
    print(news_id, doc_sent)
    connection.execute(stmt)




