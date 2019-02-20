
# Imports the Google Cloud client library
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types

# Instantiates a client
client = language.LanguageServiceClient()

# The text to analyze
text = u'The document prepared by the Vancouver-based miner shows that Inferred Mineral Resources grew to 17 million tonnes at a copper grade of ' \
       u'100%; including 9.5 million tonnes at a copper grade of 100% in the Mala Noche Footwall Zone.'


text = """Capstone Mining (TSX: CS) released today positive results of an updated technical report for its Cozamin mine, located north-northwest of Zacatecas City in the Mexican north-central state of the same name.

Cozamin is a polymetallic underground mine with a surface milling facility.

According to the updated report on the operation, Proven and Probable Mineral Reserves increased significantly compared to what was reported by the end of 2017. The reported growth is of 89%, to 6.2 million tonnes grading 1.60% copper.

On the other hand, results of a Materials Handing Study show a 30% increase in expected throughput to 3,780 tonnes per day by the end of 2020, with an estimated investment of less than $5 million. The anticipated result is an increase in annual production to between 40 to 45 million pounds of copper.

The document prepared by the Vancouver-based miner shows that Inferred Mineral Resources grew to 17 million tonnes at a copper grade of 1.11%; including 9.5 million tonnes at a copper grade of 1.61% in the Mala Noche Footwall Zone.

“The Cozamin mine continues to be a valuable asset for Capstone and formed the foundation upon which we built our company,” said Darren Pylot, President and CEO of Capstone, in a press brief. “Since commencing production at 1,000 tpd in 2006, Cozamin has demonstrated a remarkable ability to continue to discover and define new reserves. Today, more than 12 years and approximately 12 million milled tonnes later, we announce the next step in Cozamin’s evolution. The combination of the new reserves, as well as the significantly increased Inferred Resources, give us the confidence to make additional investment to increase Cozamin’s production profile,” he added.
"""

document = types.Document(
    content=text,
    type=enums.Document.Type.PLAIN_TEXT)

# Detects the sentiment of the text
sentiment = client.analyze_sentiment(document=document).document_sentiment

print('Text: {}'.format(text))
print('Sentiment: {}, {}'.format(sentiment.score, sentiment.magnitude))
